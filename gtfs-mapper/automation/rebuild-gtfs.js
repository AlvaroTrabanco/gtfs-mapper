// rebuild-gtfs.js
/* eslint-disable no-console */
import fs from "node:fs/promises";
import path from "node:path";
import fetch from "node-fetch";
import JSZip from "jszip";
import Papa from "papaparse";

/**
 * Environment
 * - FEED_URL:         GTFS source zip (remote URL, optional)
 * - FEED_LOCAL_PATH:  GTFS source zip (local path under automation/, e.g. "feeds/alsa.zip")
 * - FEED_SLUG:        short id used for logs (optional)
 * - OUT_DIR:          output directory (default: site)
 * - OUT_ZIP:          compiled zip filename (default: <slug>_compiled.zip)
 * - OUT_REPORT:       report filename (default: report.json)
 * - OVERRIDES:        local overrides path (default: automation/overrides.json)
 * - OVERRIDES_URL:    remote overrides JSON URL (takes precedence)
 */
const SLUG           = process.env.FEED_SLUG        || "feed";
const SRC_URL_ENV    = process.env.FEED_URL         || "";         // remote URL (may be empty)
const LOCAL_PATH_ENV = process.env.FEED_LOCAL_PATH  || "";         // like "feeds/alsa.zip"
const OUT_DIR        = process.env.OUT_DIR          || "site";
const OUT_ZIP        = process.env.OUT_ZIP          || `${SLUG}_compiled.zip`;
const OUT_REPORT     = process.env.OUT_REPORT       || "report.json";
const OVERRIDES_PATH = process.env.OVERRIDES        || "automation/overrides.json";
const OVERRIDES_URL  = process.env.OVERRIDES_URL    || "";
const isHttpUrl = (u) => /^https?:\/\//i.test(u || "");

/* -------------------------- overrides auto-discovery ---------------------- */

// Auto-discovery candidates when OVERRIDES_URL is empty and explicit path is missing
const OVERRIDE_CANDIDATES = (slug) => [
  // slug-specific first
  `automation/overrides-${slug}.json`,
  `overrides-${slug}.json`,
  // generic fallbacks
  `automation/overrides.json`,
  `overrides.json`,
];

async function fileExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

/* ------------------------------ metrics ---------------------------------- */
const METRICS = {
  overrides: { total: 0, byMode: { normal: 0, pickup: 0, dropoff: 0, custom: 0 } },
  trips: { touched: new Set(), createdSegments: 0 },
  stops: { touched: new Set() },
  stopTimes: { modified: 0, added: 0, deleted: 0 },
  missing: { tripStopPairs: 0 },
  warnings: [],
};
const touchTrip = (id) => METRICS.trips.touched.add(id);
const touchStop = (id) => METRICS.stops.touched.add(id);

/* ------------------------------ csv utils -------------------------------- */
const parseCsv = (text) =>
  (Papa.parse(text, { header: true, dynamicTyping: true, skipEmptyLines: true }).data || []);

const csvify = (rows, headerOrder) => {
  const headers = (headerOrder?.length ? headerOrder : (rows?.[0] ? Object.keys(rows[0]) : []));
  const out = [];
  if (headers.length) out.push(headers.join(","));
  const esc = (v) => {
    const s = String(v ?? "");
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
  };
  for (const r of (rows || [])) out.push(headers.map(h => esc(r[h])).join(","));
  return out.join("\n");
};
const toHHMMSS = (s) => {
  if (!s) return "";
  const m = String(s).match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return s;
  return `${m[1].padStart(2,"0")}:${m[2]}:${(m[3] ?? "00").padStart(2,"0")}`;
};

/* ------------------------------ overrides -------------------------------- */
/**
 * Loads overrides as text and returns { text, source }.
 * Resolution order:
 *  1) OVERRIDES_URL (remote)
 *  2) OVERRIDES_PATH (local, if exists)
 *  3) Auto-discovery by FEED_SLUG:
 *       - automation/overrides-<slug>.json
 *       - overrides-<slug>.json
 *       - automation/overrides.json
 *       - overrides.json
 *  4) none => "{}"
 */
async function loadOverridesText() {
  if (OVERRIDES_URL) {
    console.log(`Overrides: fetching from ${OVERRIDES_URL}`);
    const r = await fetch(OVERRIDES_URL);
    if (!r.ok) throw new Error(`Failed to fetch overrides: HTTP ${r.status}`);
    const text = await r.text();
    return { text, source: OVERRIDES_URL };
  }

  const explicitPath = process.env.OVERRIDES || OVERRIDES_PATH;
  if (await fileExists(explicitPath)) {
    console.log(`Overrides: reading ${explicitPath}`);
    const text = await fs.readFile(explicitPath, "utf8");
    return { text, source: explicitPath };
  }

  const candidates = OVERRIDE_CANDIDATES(SLUG);
  for (const p of candidates) {
    if (await fileExists(p)) {
      console.log(`Overrides: auto-selected ${p}`);
      const text = await fs.readFile(p, "utf8");
      return { text, source: p };
    }
  }

  console.log("Overrides: none found (continuing with no rules)");
  return { text: "{}", source: "" };
}

const KEY_DELIMS = ["::","|","/","—","–","-"];
function splitKey(k) {
  for (const d of KEY_DELIMS) {
    if (k.includes(d)) {
      const [a,b] = k.split(d);
      return [String(a ?? "").trim(), String(b ?? "").trim()];
    }
  }
  const m = String(k).match(/^(.+?)\s+([A-Za-z0-9._:-]{3,})$/);
  return m ? [m[1].trim(), m[2].trim()] : ["",""];
}

function indexStopTimes(stop_times) {
  const byTrip = new Map();
  for (const st of stop_times) {
    if (!byTrip.has(st.trip_id)) byTrip.set(st.trip_id, []);
    byTrip.get(st.trip_id).push(st.stop_id);
  }
  return byTrip;
}
function clampToTrip(seq, sid, drop, pick) {
  if (!seq?.length) return { drop, pick };
  const idx = seq.indexOf(sid);
  if (idx === -1) return { drop, pick };
  const up = new Set(seq.slice(0, idx));
  const down = new Set(seq.slice(idx+1));
  const d = drop?.filter(x => up.has(x));
  const p = pick?.filter(x => down.has(x));
  return { drop: d?.length ? d : undefined, pick: p?.length ? p : undefined };
}

function importOverridesTolerant(raw, stop_times) {
  const byTripSeq = indexStopTimes(stop_times);
  const out = {};
  const src = raw?.rules ?? raw?.restrictions ?? raw ?? {};

  if (Array.isArray(src)) {
    for (const row of src) {
      const tid = String(row.trip_id ?? "").trim();
      const sid = String(row.stop_id ?? "").trim();
      const mode = String(row.mode ?? "normal");
      if (!tid || !sid || !mode) continue;
      let drop = Array.isArray(row.dropoffOnlyFrom) ? row.dropoffOnlyFrom.map(String) : undefined;
      let pick = Array.isArray(row.pickupOnlyTo)   ? row.pickupOnlyTo.map(String)   : undefined;
      if (mode === "custom") {
        const seq = byTripSeq.get(tid) ?? [];
        ({ drop, pick } = clampToTrip(seq, sid, drop, pick));
      }
      out[`${tid}::${sid}`] = { mode, dropoffOnlyFrom: drop, pickupOnlyTo: pick };
    }
    return out;
  }

  if (src && typeof src === "object") {
    for (const k of Object.keys(src)) {
      const { mode } = src[k] || {};
      const [tid, sid] = splitKey(k);
      if (!tid || !sid || !mode) continue;
      let drop = Array.isArray(src[k]?.dropoffOnlyFrom) ? src[k].dropoffOnlyFrom.map(String) : undefined;
      let pick = Array.isArray(src[k]?.pickupOnlyTo)   ? src[k].pickupOnlyTo.map(String)   : undefined;
      if (mode === "custom") {
        const seq = byTripSeq.get(tid) ?? [];
        ({ drop, pick } = clampToTrip(seq, sid, drop, pick));
      }
      out[`${tid}::${sid}`] = { mode, dropoffOnlyFrom: drop, pickupOnlyTo: pick };
    }
  }
  return out;
}

/* ------------------------- OD compiler (unchanged logic) ------------------ */
function compileTripsWithOD({ trips, stop_times }, restrictions) {
  const stopTimesByTrip = new Map();
  for (const st of stop_times) {
    if (!stopTimesByTrip.has(st.trip_id)) stopTimesByTrip.set(st.trip_id, []);
    stopTimesByTrip.get(st.trip_id).push(st);
  }
  for (const [, arr] of stopTimesByTrip) arr.sort((a,b)=>Number(a.stop_sequence)-Number(b.stop_sequence));

  const outTrips = [];
  const outStopTimes = [];

  for (const t of trips) {
    const rows = (stopTimesByTrip.get(t.trip_id) || []).slice();
    if (!rows.length) continue;

    const rulesByIdx = new Map();
    rows.forEach((st, i) => {
      const r = restrictions[`${t.trip_id}::${st.stop_id}`];
      if (r && r.mode && r.mode !== "normal") rulesByIdx.set(i, r);
    });

    const hasCustom = Array.from(rulesByIdx.values()).some(r => r.mode === "custom");

    if (!hasCustom) {
      outTrips.push({ ...t });
      for (let i = 0; i < rows.length; i++) {
        const st = rows[i];
        const r = rulesByIdx.get(i);
        let pickup_type = 0, drop_off_type = 0;
        if (r?.mode === "pickup")  { drop_off_type = 1; METRICS.stopTimes.modified++; touchTrip(t.trip_id); touchStop(st.stop_id); }
        if (r?.mode === "dropoff") { pickup_type  = 1; METRICS.stopTimes.modified++; touchTrip(t.trip_id); touchStop(st.stop_id); }

        const arr = toHHMMSS(st.arrival_time);
        const dep = toHHMMSS(st.departure_time);
        outStopTimes.push({
          trip_id: t.trip_id,
          stop_id: st.stop_id,
          stop_sequence: 0,
          arrival_time: arr,
          departure_time: dep,
          pickup_type,
          drop_off_type,
        });
      }
      continue;
    }

    const customIdxs = rows.map((_, i) => i).filter(i => rulesByIdx.get(i)?.mode === "custom");
    const firstC = Math.min(...customIdxs);
    const lastC  = Math.max(...customIdxs);

    const upId = `${t.trip_id}__segA`;
    METRICS.trips.createdSegments++;
    outTrips.push({ ...t, trip_id: upId });
    let addedUp = 0;
    for (let i = 0; i <= lastC; i++) {
      const st = rows[i];
      const r = rulesByIdx.get(i);
      let pickup_type = 0, drop_off_type = 0;
      if (r?.mode === "pickup")       { drop_off_type = 1; METRICS.stopTimes.modified++; }
      else if (r?.mode === "dropoff") { pickup_type = 1;  METRICS.stopTimes.modified++; }
      else if (r?.mode === "custom")  { pickup_type = 1;  drop_off_type = 0; METRICS.stopTimes.modified++; }
      const arr = toHHMMSS(st.arrival_time);
      const dep = toHHMMSS(st.departure_time);
      outStopTimes.push({ trip_id: upId, stop_id: st.stop_id, stop_sequence: 0, arrival_time: arr, departure_time: dep, pickup_type, drop_off_type });
      addedUp++; touchTrip(t.trip_id); touchStop(st.stop_id);
    }
    METRICS.stopTimes.added += addedUp;

    const downId = `${t.trip_id}__segB`;
    METRICS.trips.createdSegments++;
    outTrips.push({ ...t, trip_id: downId });
    let addedDown = 0;
    for (let i = firstC; i < rows.length; i++) {
      const st = rows[i];
      const r = rulesByIdx.get(i);
      let pickup_type = 0, drop_off_type = 0;
      if (r?.mode === "pickup")       { drop_off_type = 1; METRICS.stopTimes.modified++; }
      else if (r?.mode === "dropoff") { pickup_type = 1;  METRICS.stopTimes.modified++; }
      else if (r?.mode === "custom")  { pickup_type = 0;  drop_off_type = 1; METRICS.stopTimes.modified++; }
      const arr = toHHMMSS(st.arrival_time);
      const dep = toHHMMSS(st.departure_time);
      outStopTimes.push({ trip_id: downId, stop_id: st.stop_id, stop_sequence: 0, arrival_time: arr, departure_time: dep, pickup_type, drop_off_type });
      addedDown++; touchTrip(t.trip_id); touchStop(st.stop_id);
    }
    METRICS.stopTimes.added += addedDown;

    const bridgeId = `${t.trip_id}__bridge`;
    METRICS.trips.createdSegments++;
    outTrips.push({ ...t, trip_id: bridgeId });
    let addedBridge = 0;
    for (let i = 0; i < rows.length; i++) {
      const st = rows[i];
      const r = rulesByIdx.get(i);
      let pickup_type = 0, drop_off_type = 0;
      if (r?.mode === "custom")       { pickup_type = 1; drop_off_type = 1; METRICS.stopTimes.modified++; }
      else if (r?.mode === "pickup")  { drop_off_type = 1; METRICS.stopTimes.modified++; }
      else if (r?.mode === "dropoff") { pickup_type  = 1; METRICS.stopTimes.modified++; }
      const arr = toHHMMSS(st.arrival_time);
      const dep = toHHMMSS(st.departure_time);
      outStopTimes.push({ trip_id: bridgeId, stop_id: st.stop_id, stop_sequence: 0, arrival_time: arr, departure_time: dep, pickup_type, drop_off_type });
      addedBridge++; touchTrip(t.trip_id); touchStop(st.stop_id);
    }
    METRICS.stopTimes.added += addedBridge;
  }

  const grouped = new Map();
  for (const st of outStopTimes) {
    if (!grouped.has(st.trip_id)) grouped.set(st.trip_id, []);
    grouped.get(st.trip_id).push(st);
  }
  const finalStopTimes = [];
  for (const [, arr] of grouped) {
    arr.forEach((st, i) => { st.stop_sequence = i + 1; });
    finalStopTimes.push(...arr);
  }

  return { trips: outTrips, stop_times: finalStopTimes };
}

/* -------------------------------- main ------------------------------------ */
(async () => {
  try {
    let zipBuffer;
    let sourceDescriptor = "";

    const hasHttpUrl = isHttpUrl(SRC_URL_ENV);

    if (hasHttpUrl) {
      // Remote URL mode (only true http/https)
      console.log(`Downloading GTFS (${SLUG}) from URL:`, SRC_URL_ENV);
      const headers = {
        Accept: "application/zip, application/octet-stream,*/*",
        "User-Agent": "curl/8.7.1",
      };

      const res = await fetch(SRC_URL_ENV, { headers });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      zipBuffer = await res.arrayBuffer();
      sourceDescriptor = SRC_URL_ENV;
    } else if (LOCAL_PATH_ENV) {
      // Local file mode
      const localPath = path.join("automation", LOCAL_PATH_ENV);
      console.log(`Loading GTFS (${SLUG}) from local file:`, localPath);
      try {
        zipBuffer = await fs.readFile(localPath);
      } catch (err) {
        throw new Error(
          `Failed to read local GTFS file at ${localPath}: ${err.message || err}`
        );
      }
      sourceDescriptor = `local:${localPath}`;
    } else if (SRC_URL_ENV) {
      // FEED_URL was set but is not a valid http/https URL
      throw new Error(`FEED_URL is not a valid http/https URL: ${SRC_URL_ENV}`);
    } else {
      throw new Error("No FEED_URL or FEED_LOCAL_PATH provided for GTFS source.");
    }

    const zip = await JSZip.loadAsync(zipBuffer);
    const tables = {};
    const raw = {};
    for (const entry of Object.values(zip.files)) {
      const f = entry;
      if (f.dir) continue;
      if (!f.name?.toLowerCase().endsWith(".txt")) continue;
      const base = f.name.replace(/\.txt$/i, "");
      tables[base] = await f.async("string");
      raw[base] = await f.async("uint8array");
    }

    const agencies  = tables.agency     ? parseCsv(tables.agency)     : [];
    const stops     = tables.stops      ? parseCsv(tables.stops)      : [];
    const routes    = tables.routes     ? parseCsv(tables.routes)     : [];
    const services  = tables.calendar   ? parseCsv(tables.calendar)   : [];
    const trips     = tables.trips      ? parseCsv(tables.trips)      : [];
    const stopTimes = tables.stop_times ? parseCsv(tables.stop_times) : [];
    const shapes    = tables.shapes     ? parseCsv(tables.shapes)     : [];

    trips.forEach(t => { t.trip_headsign ??= ""; t.shape_id ??= ""; t.direction_id ??= ""; });

    const { text: overridesText, source: effectiveOverridesSource } = await loadOverridesText();

    let overridesRaw = {};
    try {
      const j = JSON.parse(overridesText || "{}");

      if (j && typeof j === "object" && j.overrides && typeof j.overrides === "object") {
        if (j.overrides[SLUG]) {
          overridesRaw = j.overrides[SLUG];                          // exact match for this feed
        } else {
          const keys = Object.keys(j.overrides);
          overridesRaw = keys.length === 1 ? j.overrides[keys[0]] : {}; // single other slug -> accept, else empty
        }
      } else {
        overridesRaw = j; // already a body (rules at top-level or array form)
      }
    } catch {
      overridesRaw = {};
    }

    console.log("[overrides] source =", effectiveOverridesSource || "(none)");
    console.log("[overrides] slug =", SLUG, "| top keys =", Object.keys(overridesRaw || {}).slice(0,5));

    const restrictions = importOverridesTolerant(overridesRaw, stopTimes);

    const entries = Object.entries(restrictions);
    METRICS.overrides.total = entries.length;
    for (const [, v] of entries) {
      const m = v?.mode || "normal";
      if (!METRICS.overrides.byMode[m]) METRICS.overrides.byMode[m] = 0;
      METRICS.overrides.byMode[m]++;
    }

    const present = new Set(stopTimes.map(st => `${st.trip_id}::${st.stop_id}`));
    for (const k of Object.keys(restrictions)) {
      if (!present.has(k)) {
        METRICS.missing.tripStopPairs++;
        METRICS.warnings.push(`Rule key not found in feed: ${k}`);
      }
    }

    const { trips: outTrips, stop_times: outStopTimes } =
      compileTripsWithOD({ trips, stop_times: stopTimes }, restrictions);

    await fs.mkdir(OUT_DIR, { recursive: true });
    const outZip = new JSZip();

    if (agencies.length)
      outZip.file("agency.txt", csvify(
        agencies.map(a => ({
          agency_id: a.agency_id, agency_name: a.agency_name,
          agency_url: a.agency_url, agency_timezone: a.agency_timezone
        })), ["agency_id","agency_name","agency_url","agency_timezone"]));

    if (stops.length)
      outZip.file("stops.txt", csvify(
        stops.map(s => ({
          stop_id: s.stop_id, stop_name: s.stop_name,
          stop_lat: s.stop_lat, stop_lon: s.stop_lon
        })), ["stop_id","stop_name","stop_lat","stop_lon"]));

    if (routes.length)
      outZip.file("routes.txt", csvify(
        routes.map(r => ({
          route_id: r.route_id, route_short_name: r.route_short_name,
          route_long_name: r.route_long_name, route_type: r.route_type, agency_id: r.agency_id
        })), ["route_id","route_short_name","route_long_name","route_type","agency_id"]));

    if (services.length)
      outZip.file("calendar.txt", csvify(
        services.map(s => ({
          service_id: s.service_id, monday: s.monday, tuesday: s.tuesday, wednesday: s.wednesday,
          thursday: s.thursday, friday: s.friday, saturday: s.saturday, sunday: s.sunday,
          start_date: s.start_date, end_date: s.end_date
        })), ["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"]));

    if (raw.shapes) outZip.file("shapes.txt", raw.shapes, { binary: true });

    outZip.file("trips.txt", csvify(outTrips.map(tr => ({
      route_id: tr.route_id, service_id: tr.service_id, trip_id: tr.trip_id,
      trip_headsign: tr.trip_headsign ?? "", shape_id: tr.shape_id ?? "", direction_id: tr.direction_id ?? ""
    })), ["route_id","service_id","trip_id","trip_headsign","shape_id","direction_id"]));

    outZip.file("stop_times.txt", csvify(outStopTimes.map(st => ({
      trip_id: st.trip_id,
      arrival_time: toHHMMSS(st.arrival_time),
      departure_time: toHHMMSS(st.departure_time),
      stop_id: st.stop_id,
      stop_sequence: st.stop_sequence,
      pickup_type: st.pickup_type ?? 0,
      drop_off_type: st.drop_off_type ?? 0
    })), ["trip_id","arrival_time","departure_time","stop_id","stop_sequence","pickup_type","drop_off_type"]));

    const blob = await outZip.generateAsync({ type: "nodebuffer", compression: "DEFLATE", compressionOptions: { level: 9 } });

    const outPath = path.join(OUT_DIR, OUT_ZIP);
    await fs.writeFile(outPath, blob);
    console.log("Wrote:", outPath);

    const report = {
      feed: SLUG,
      overrides: METRICS.overrides,
      trips: { touchedCount: METRICS.trips.touched.size, createdSegments: METRICS.trips.createdSegments },
      stops: { touchedCount: METRICS.stops.touched.size },
      stopTimes: METRICS.stopTimes,
      missing: METRICS.missing,
      warnings: METRICS.warnings,
      generatedAt: new Date().toISOString(),
      source: sourceDescriptor,
      overridesSource: effectiveOverridesSource || "",
      artifacts: { zip: path.join(OUT_DIR, OUT_ZIP) },
    };

    const reportPath = path.join(OUT_DIR, OUT_REPORT);
    await fs.writeFile(reportPath, JSON.stringify(report, null, 2), "utf8");
    console.log("Report:", reportPath);

    const lines = [
      `=== GTFS Rebuild — ${SLUG} ===`,
      `Overrides: total=${report.overrides.total}  (pickup=${report.overrides.byMode.pickup || 0}, dropoff=${report.overrides.byMode.dropoff || 0}, custom=${report.overrides.byMode.custom || 0})`,
      `Trips: touched=${report.trips.touchedCount}, createdSegments=${report.trips.createdSegments}`,
      `Stops: touched=${report.stops.touchedCount}`,
      `StopTimes: modified=${report.stopTimes.modified}, added=${report.stopTimes.added}, deleted=${report.stopTimes.deleted}`,
      `Missing pairs ignored: ${report.missing.tripStopPairs}`,
      report.warnings.length ? `Warnings:\n- ${report.warnings.join("\n- ")}` : "",
    ].filter(Boolean);
    console.log("\n" + lines.join("\n") + "\n");
  } catch (err) {
    console.error("Build failed:", err?.stack || err);
    process.exitCode = 1;
  }
})();