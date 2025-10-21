/* eslint-disable no-console */
import fs from "node:fs/promises";
import path from "node:path";
import fetch from "node-fetch";
import JSZip from "jszip";
import Papa from "papaparse";

// ---------- metrics ----------
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

// ---------- config ----------
const SRC_URL = process.env.FLIX_URL || "http://gtfs.gis.flix.tech/gtfs_generic_eu.zip";
const OUT_DIR = process.env.OUT_DIR || "dist";
const OUT_ZIP = process.env.OUT_ZIP || "gtfs_flixbus_fixed.zip";
const OUT_REPORT = process.env.OUT_REPORT || "report.json";
const OVERRIDES_PATH = process.env.OVERRIDES || "automation/overrides.json";
const OVERRIDES_URL = process.env.OVERRIDES_URL || "";

// ---------- tiny csv helpers ----------
const parseCsv = (text) =>
  (Papa.parse(text, { header: true, dynamicTyping: true, skipEmptyLines: true }).data || []);

const csvify = (rows, headerOrder) => {
  if (!rows || !rows.length) return "";
  const headers = headerOrder?.length ? headerOrder : Object.keys(rows[0]);
  const out = [headers.join(",")];
  for (const r of rows) out.push(headers.map(h => String(r[h] ?? "")).join(","));
  return out.join("\n");
};

const toHHMMSS = (s) => {
  if (!s) return "";
  const m = String(s).match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return s;
  return `${m[1].padStart(2,"0")}:${m[2]}:${(m[3] ?? "00").padStart(2,"0")}`;
};

// ---------- overrides loader ----------
async function loadOverridesText() {
  if (OVERRIDES_URL) {
    console.log(`Overrides: fetching from ${OVERRIDES_URL}`);
    const r = await fetch(OVERRIDES_URL);
    if (!r.ok) throw new Error(`Failed to fetch overrides from URL: HTTP ${r.status}`);
    return await r.text();
  }
  console.log(`Overrides: reading from ${OVERRIDES_PATH}`);
  return await fs.readFile(OVERRIDES_PATH, "utf8");
}

// ---------- helpers for OD gating ----------
function collectTripCustomGates(tripId, restrictions) {
  // restrictions keys are `${trip_id}::${stop_id}` → { mode, pickupOnlyTo?, dropoffOnlyFrom? }
  const gate = { pickupOnlyTo: new Set(), dropoffOnlyFrom: new Set(), hasCustom: false };
  for (const [k, v] of Object.entries(restrictions)) {
    if (!k.startsWith(`${tripId}::`)) continue;
    if (v?.mode !== "custom") continue;
    gate.hasCustom = true;
    for (const s of v.pickupOnlyTo || []) gate.pickupOnlyTo.add(String(s));
    for (const s of v.dropoffOnlyFrom || []) gate.dropoffOnlyFrom.add(String(s));
  }
  return gate;
}

// ---------- compile OD ----------
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

    // map index → rule when explicitly present on that (trip, stop)
    const rulesByIdx = new Map();
    rows.forEach((st, i) => {
      const key = `${t.trip_id}::${st.stop_id}`;
      const r = restrictions[key];
      if (r && r.mode && r.mode !== "normal") rulesByIdx.set(i, r);
    });

    // collect OD gates for this trip from any custom rule entries
    const gates = collectTripCustomGates(t.trip_id, restrictions);

    // If there is at least one "custom", we’ll keep 2 segments (A+B) for clarity,
    // but compute pickup/dropoff using pickupOnlyTo / dropoffOnlyFrom if given.
    const hasCustom = gates.hasCustom || Array.from(rulesByIdx.values()).some(r => r.mode === "custom");

    if (!hasCustom) {
      // simple per-stop toggles only
      outTrips.push({ ...t });
      for (let i = 0; i < rows.length; i++) {
        const st = rows[i];
        const r = rulesByIdx.get(i);
        let pickup_type = 0, drop_off_type = 0;
        if (r?.mode === "pickup")  { drop_off_type = 1; METRICS.stopTimes.modified++; touchTrip(t.trip_id); touchStop(st.stop_id); }
        if (r?.mode === "dropoff") { pickup_type  = 1; METRICS.stopTimes.modified++; touchTrip(t.trip_id); touchStop(st.stop_id); }

        const arr = toHHMMSS(st.arrival_time);
        const dep = toHHMMSS(st.departure_time);
        if (!arr && !dep) continue;

        outStopTimes.push({
          trip_id: t.trip_id,
          stop_id: st.stop_id,
          stop_sequence: 0,
          arrival_time: arr,
          departure_time: dep,
          pickup_type,
          drop_off_type,
          shape_dist_traveled: st.shape_dist_traveled ?? ""
        });
      }
      continue;
    }

    // --- CUSTOM: derive boundaries from gates (if provided) ---
    const indexOf = (id) => rows.findIndex(r => String(r.stop_id) === String(id));
    let leftBound = Infinity;   // earliest index that is in pickupOnlyTo
    for (const id of gates.pickupOnlyTo) {
      const idx = indexOf(id);
      if (idx >= 0) leftBound = Math.min(leftBound, idx);
    }
    let rightBound = -Infinity; // latest index that is in dropoffOnlyFrom
    for (const id of gates.dropoffOnlyFrom) {
      const idx = indexOf(id);
      if (idx >= 0) rightBound = Math.max(rightBound, idx);
    }

    // If no arrays are provided, fall back to the first/last “custom” indices seen on explicit rules
    if (!Number.isFinite(leftBound)) {
      const firstCustom = rows.findIndex((_, i) => rulesByIdx.get(i)?.mode === "custom");
      if (firstCustom >= 0) leftBound = firstCustom;
    }
    if (!Number.isFinite(rightBound)) {
      const lastCustom = (() => {
        let last = -1;
        rows.forEach((_, i) => { if (rulesByIdx.get(i)?.mode === "custom") last = i; });
        return last;
      })();
      if (lastCustom >= 0) rightBound = lastCustom;
    }

    // clamp bounds
    if (!Number.isFinite(leftBound)) leftBound = 0;
    if (!Number.isFinite(rightBound)) rightBound = rows.length - 1;

    // two segments for readability (A: 0..rightBound, B: leftBound..end)
    const segs = [
      { id: `${t.trip_id}__segA`, start: 0, end: Math.max(rightBound, 0) + 1 },
      { id: `${t.trip_id}__segB`, start: Math.min(leftBound, rows.length - 1), end: rows.length },
    ];

    for (const seg of segs) {
      METRICS.trips.createdSegments++;
      outTrips.push({ ...t, trip_id: seg.id });
      let added = 0;

      for (let i = seg.start; i < seg.end; i++) {
        const st = rows[i];
        const explicit = rulesByIdx.get(i);
        let pickup_type = 0, drop_off_type = 0;

        // 1) Apply global gates first
        //    - no alight on/before leftBound
        if (i <= leftBound) { drop_off_type = 1; }
        //    - no board on/after rightBound
        if (i >= rightBound) { pickup_type = 1; }

        // 2) Apply explicit per-stop mode overrides (wins if set)
        if (explicit?.mode === "pickup")  { drop_off_type = 1; }
        if (explicit?.mode === "dropoff") { pickup_type  = 1; }
        // (explicit?.mode === "custom") just means we’re in the OD-split zone; gates already applied

        if (pickup_type || drop_off_type) { METRICS.stopTimes.modified++; touchTrip(t.trip_id); touchStop(st.stop_id); }

        const arrS = toHHMMSS(st.arrival_time);
        const depS = toHHMMSS(st.departure_time);
        if (!arrS && !depS) continue;

        outStopTimes.push({
          trip_id: seg.id,
          stop_id: st.stop_id,
          stop_sequence: 0,
          arrival_time: arrS,
          departure_time: depS,
          pickup_type,
          drop_off_type,
          shape_dist_traveled: st.shape_dist_traveled ?? ""
        });
        added++;
      }
      METRICS.stopTimes.added += added;
    }
  }

  // resequence stop_times within each trip
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

// ---------- main ----------
(async () => {
  try {
    console.log("Downloading:", SRC_URL);
    const res = await fetch(SRC_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const buf = await res.arrayBuffer();

    const zip = await JSZip.loadAsync(buf);
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

    // parse needed tables
    const agencies = tables.agency ? parseCsv(tables.agency) : [];
    const stops    = tables.stops  ? parseCsv(tables.stops)  : [];
    const routes   = tables.routes ? parseCsv(tables.routes) : [];
    const services = tables.calendar ? parseCsv(tables.calendar) : [];
    const trips    = tables.trips  ? parseCsv(tables.trips)  : [];
    const stopTimes= tables.stop_times ? parseCsv(tables.stop_times) : [];

    // load overrides.json (URL preferred, fallback to file)
    const overridesText = await loadOverridesText();
    const overridesRaw = JSON.parse(overridesText || "{}");
    const rules = overridesRaw.rules || {};

    // metrics
    const ruleEntries = Object.entries(rules);
    METRICS.overrides.total = ruleEntries.length;
    for (const [, v] of ruleEntries) {
      const mode = v?.mode || "normal";
      METRICS.overrides.byMode[mode] = (METRICS.overrides.byMode[mode] || 0) + 1;
    }
    // existence check for trip::stop keys
    const presentPairs = new Set(stopTimes.map(st => `${st.trip_id}::${st.stop_id}`));
    for (const k of Object.keys(rules)) {
      if (!presentPairs.has(k)) {
        METRICS.missing.tripStopPairs++;
        METRICS.warnings.push(`Rule key not found in feed: ${k}`);
      }
    }

    // compile
    const { trips: outTrips, stop_times: outStopTimes } =
      compileTripsWithOD({ trips, stop_times: stopTimes }, rules);

    // write new zip
    await fs.mkdir(OUT_DIR, { recursive: true });
    const outZip = new JSZip();

    // passthrough unchanged tables
    if (agencies.length) outZip.file("agency.txt", csvify(agencies, ["agency_id","agency_name","agency_url","agency_timezone"]));
    if (stops.length)    outZip.file("stops.txt",  csvify(stops, ["stop_id","stop_name","stop_lat","stop_lon"]));
    if (routes.length)   outZip.file("routes.txt", csvify(routes, ["route_id","route_short_name","route_long_name","route_type","agency_id"]));
    if (services.length) outZip.file("calendar.txt", csvify(services, ["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"]));

    // compiled trips + stop_times
    outZip.file("trips.txt", csvify(outTrips.map(tr => ({
      route_id: tr.route_id, service_id: tr.service_id, trip_id: tr.trip_id,
      trip_headsign: tr.trip_headsign ?? "", shape_id: tr.shape_id ?? "", direction_id: tr.direction_id ?? ""
    })), ["route_id","service_id","trip_id","trip_headsign","shape_id","direction_id"]));

    const hasShapeDist = outStopTimes.some(st => st.shape_dist_traveled != null && st.shape_dist_traveled !== "");
    outZip.file("stop_times.txt", csvify(outStopTimes.map(st => ({
      trip_id: st.trip_id,
      arrival_time: toHHMMSS(st.arrival_time),
      departure_time: toHHMMSS(st.departure_time),
      stop_id: st.stop_id,
      stop_sequence: st.stop_sequence,
      pickup_type: st.pickup_type ?? 0,
      drop_off_type: st.drop_off_type ?? 0,
      ...(hasShapeDist ? { shape_dist_traveled: st.shape_dist_traveled ?? "" } : {})
    })), hasShapeDist
      ? ["trip_id","arrival_time","departure_time","stop_id","stop_sequence","pickup_type","drop_off_type","shape_dist_traveled"]
      : ["trip_id","arrival_time","departure_time","stop_id","stop_sequence","pickup_type","drop_off_type"]));

    const blob = await outZip.generateAsync({
      type: "nodebuffer",
      compression: "DEFLATE",
      compressionOptions: { level: 9 }
    });

    const outPath = path.join(OUT_DIR, OUT_ZIP);
    await fs.writeFile(outPath, blob);
    console.log("Wrote:", outPath);

    // ---------- report ----------
    const report = {
      overrides: METRICS.overrides,
      trips: {
        touchedCount: METRICS.trips.touched.size,
        createdSegments: METRICS.trips.createdSegments,
      },
      stops: { touchedCount: METRICS.stops.touched.size },
      stopTimes: METRICS.stopTimes,
      missing: METRICS.missing,
      warnings: METRICS.warnings,
      generatedAt: new Date().toISOString(),
      source: SRC_URL,
      overridesSource: OVERRIDES_URL || OVERRIDES_PATH,
      artifacts: { zip: path.join(OUT_DIR, OUT_ZIP) },
    };

    const reportPath = path.join(OUT_DIR, OUT_REPORT);
    await fs.writeFile(reportPath, JSON.stringify(report, null, 2), "utf8");
    console.log("Report:", reportPath);

    // pretty print
    const lines = [
      "=== Flixbus GTFS Fix — Summary ===",
      `Overrides: total=${report.overrides.total}  (pickup=${report.overrides.byMode.pickup || 0}, dropoff=${report.overrides.byMode.dropoff || 0}, custom=${report.overrides.byMode.custom || 0})`,
      `Trips: touched=${report.trips.touchedCount}, createdSegments=${report.trips.createdSegments}`,
      `Stops: touched=${report.stops.touchedCount}`,
      `StopTimes: modified=${report.stopTimes.modified}, added=${report.stopTimes.added}, deleted=${report.stopTimes.deleted}`,
      `Missing pairs ignored: ${report.missing.tripStopPairs}`,
      report.warnings.length ? `Warnings:\n- ${report.warnings.join("\n- ")}` : "",
    ].filter(Boolean);
    console.log("\n" + lines.join("\n") + "\n");

    if (process.env.GITHUB_STEP_SUMMARY) {
      const md = [
        `# Flixbus GTFS Fix — Summary`,
        `**Generated**: ${report.generatedAt}`,
        ``,
        `**Overrides**: ${report.overrides.total}  (pickup=${report.overrides.byMode.pickup || 0}, dropoff=${report.overrides.byMode.dropoff || 0}, custom=${report.overrides.byMode.custom || 0})`,
        `**Trips**: touched=${report.trips.touchedCount}, createdSegments=${report.trips.createdSegments}`,
        `**Stops**: touched=${report.stops.touchedCount}`,
        `**StopTimes**: modified=${report.stopTimes.modified}, added=${report.stopTimes.added}, deleted=${report.stopTimes.deleted}`,
        `**Missing pairs ignored**: ${report.missing.tripStopPairs}`,
        `\nArtifact: \`${path.join(OUT_DIR, OUT_ZIP)}\`  ·  Report: \`${reportPath}\``,
        report.warnings.length ? `\n<details><summary>Warnings</summary>\n\n${report.warnings.map(w => `- ${w}`).join("\n")}\n\n</details>` : "",
      ].join("\n");
      await fs.appendFile(process.env.GITHUB_STEP_SUMMARY, md + "\n");
    }
  } catch (err) {
    console.error("Build failed:", err?.stack || err);
    process.exitCode = 1;
  }
})();