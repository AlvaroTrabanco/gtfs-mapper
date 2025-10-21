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
  missing: { tripStopPairs: 0 }, // overrides that didn’t match any row
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

// ---------- compile OD (mirrors your App’s logic) ----------
function compileTripsWithOD({ trips, stop_times }, restrictions) {
  // Group stop_times by trip, sorted by stop_sequence
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

    // Collect all rules that target this trip
    // - direct per-stop "pickup"/"dropoff" rules (exact key match)
    // - "custom" pivot rules with arrays (dropoffOnlyFrom, pickupOnlyTo)
    const perIndex = rows.map(() => ({ pickup: 0, dropoff: 0 })); // desired flags per original index
    const customPivotIdxs = new Set();

    // index stops for quick lookups
    const indicesByStopId = new Map();
    rows.forEach((st, i) => {
      if (!indicesByStopId.has(st.stop_id)) indicesByStopId.set(st.stop_id, []);
      indicesByStopId.get(st.stop_id).push(i);
    });

    // Pass 1: apply explicit per-stop pickup/dropoff rules and collect custom pivots
    for (let i = 0; i < rows.length; i++) {
      const key = `${t.trip_id}::${rows[i].stop_id}`;
      const rule = restrictions[key];
      if (!rule || !rule.mode || rule.mode === "normal") continue;

      if (rule.mode === "pickup") {
        // pickup-only => cannot drop off
        perIndex[i].dropoff = 1;
        METRICS.stopTimes.modified++; touchTrip(t.trip_id); touchStop(rows[i].stop_id);
      } else if (rule.mode === "dropoff") {
        // dropoff-only => cannot pick up
        perIndex[i].pickup = 1;
        METRICS.stopTimes.modified++; touchTrip(t.trip_id); touchStop(rows[i].stop_id);
      } else if (rule.mode === "custom") {
        customPivotIdxs.add(i);
      }
    }

    // Pass 2: for each custom pivot, honor arrays:
    // - For any stop at or BEFORE pivot whose stop_id is in dropoffOnlyFrom => pickup=1 (dropoff-only)
    // - For any stop at or AFTER pivot whose stop_id is in pickupOnlyTo     => dropoff=1 (pickup-only)
    if (customPivotIdxs.size) {
      for (const pivotIdx of customPivotIdxs) {
        const pivotKey = `${t.trip_id}::${rows[pivotIdx].stop_id}`;
        const rule = restrictions[pivotKey] || {};
        const dropFrom = new Set(rule.dropoffOnlyFrom || []);
        const pickTo   = new Set(rule.pickupOnlyTo || []);

        if (dropFrom.size) {
          for (let i = 0; i <= pivotIdx; i++) {
            const sid = rows[i].stop_id;
            if (dropFrom.has(sid)) {
              if (perIndex[i].pickup !== 1) {
                perIndex[i].pickup = 1;
                METRICS.stopTimes.modified++;
              }
              touchTrip(t.trip_id); touchStop(sid);
            }
          }
        }

        if (pickTo.size) {
          for (let i = pivotIdx; i < rows.length; i++) {
            const sid = rows[i].stop_id;
            if (pickTo.has(sid)) {
              if (perIndex[i].dropoff !== 1) {
                perIndex[i].dropoff = 1;
                METRICS.stopTimes.modified++;
              }
              touchTrip(t.trip_id); touchStop(sid);
            }
          }
        }
      }
    }

    // If there are no custom pivots, just output the single trip with toggles
    if (!customPivotIdxs.size) {
      outTrips.push({ ...t });
      for (let i = 0; i < rows.length; i++) {
        const st = rows[i];
        const arr = toHHMMSS(st.arrival_time);
        const dep = toHHMMSS(st.departure_time);
        if (!arr && !dep) continue;
        outStopTimes.push({
          trip_id: t.trip_id,
          stop_id: st.stop_id,
          stop_sequence: 0, // resequenced later
          arrival_time: arr,
          departure_time: dep,
          pickup_type: perIndex[i].pickup,
          drop_off_type: perIndex[i].dropoff,
        });
      }
      continue;
    }

    // With custom pivots: split into segments wherever "isCustom at index" toggles
    // A pivot marks index i itself as "custom".
    const isCustomIndex = new Set(customPivotIdxs);
    const N = rows.length;
    const boundaries = [0];
    for (let i = 1; i < N; i++) {
      const a = isCustomIndex.has(i - 1);
      const b = isCustomIndex.has(i);
      if (a !== b) boundaries.push(i);
    }
    boundaries.push(N);

    for (let s = 0; s < boundaries.length - 1; s++) {
      const start = boundaries[s], end = boundaries[s + 1]; // [start, end)
      const segId = `${t.trip_id}__seg${s + 1}`;
      METRICS.trips.createdSegments++;

      outTrips.push({ ...t, trip_id: segId });

      let added = 0;
      for (let i = start; i < end; i++) {
        const st = rows[i];
        const arrS = toHHMMSS(st.arrival_time);
        const depS = toHHMMSS(st.departure_time);
        if (!arrS && !depS) continue;

        outStopTimes.push({
          trip_id: segId,
          stop_id: st.stop_id,
          stop_sequence: 0, // resequenced later
          arrival_time: arrS,
          departure_time: depS,
          pickup_type: perIndex[i].pickup,
          drop_off_type: perIndex[i].dropoff,
        });
        added++; touchTrip(t.trip_id); touchStop(st.stop_id);
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
    const shapes   = tables.shapes ? parseCsv(tables.shapes) : [];

    // load overrides.json (URL preferred, fallback to file)
    const overridesText = await loadOverridesText();
    const overridesRaw = JSON.parse(overridesText || "{}");
    const rules = overridesRaw.rules || {};

    // metrics: overrides quick counts
    const ruleEntries = Object.entries(rules);
    METRICS.overrides.total = ruleEntries.length;
    for (const [, v] of ruleEntries) {
      const mode = v?.mode || "normal";
      METRICS.overrides.byMode[mode] = (METRICS.overrides.byMode[mode] || 0) + 1;
    }

    // pre-check: figure out which override keys exist in the actual (trip_id, stop_id) pairs
    const presentPairs = new Set();
    for (const st of stopTimes) presentPairs.add(`${st.trip_id}::${st.stop_id}`);
    for (const k of Object.keys(rules)) {
      if (!presentPairs.has(k)) {
        METRICS.missing.tripStopPairs++;
        METRICS.warnings.push(`Rule key not found in feed: ${k}`);
      }
    }

    // compile OD with proper stop types
    const { trips: outTrips, stop_times: outStopTimes } =
      compileTripsWithOD({ trips, stop_times: stopTimes }, rules);

    // write new zip
    await fs.mkdir(OUT_DIR, { recursive: true });
    const outZip = new JSZip();

    // passthrough (keep vendor formatting for shapes by default)
    if (agencies.length) outZip.file("agency.txt", csvify(agencies, ["agency_id","agency_name","agency_url","agency_timezone"]));
    if (stops.length)    outZip.file("stops.txt",  csvify(stops, ["stop_id","stop_name","stop_lat","stop_lon"]));
    if (routes.length)   outZip.file("routes.txt", csvify(routes, ["route_id","route_short_name","route_long_name","route_type","agency_id"]));
    if (services.length) outZip.file("calendar.txt", csvify(services, ["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"]));
    if (raw.shapes)      outZip.file("shapes.txt", raw.shapes, { binary: true });

    // compiled trips + stop_times
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
      artifacts: {
        zip: path.join(OUT_DIR, OUT_ZIP),
      },
    };

    const reportPath = path.join(OUT_DIR, OUT_REPORT);
    await fs.writeFile(reportPath, JSON.stringify(report, null, 2), "utf8");
    console.log("Report:", reportPath);

    // console pretty print
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

    // GitHub Actions step summary (if available)
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
        report.warnings.length ? `\n<details><summary>Warnings</summary>\n\n${report.warnings.map(w => `- ${w}`).join("\n")}\n\n</details>` : "",
        `\nArtifact: \`${path.join(OUT_DIR, OUT_ZIP)}\`  ·  Report: \`${reportPath}\``,
      ].join("\n");
      await fs.appendFile(process.env.GITHUB_STEP_SUMMARY, md + "\n");
    }
  } catch (err) {
    console.error("Build failed:", err?.stack || err);
    process.exitCode = 1;
  }
})();