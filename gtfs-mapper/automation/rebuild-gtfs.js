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

// When true, we ignore generic overrides and instead use Spanish decisions
const AUTO_SPANISH_OVERRIDES =
  /^(1|true|yes)$/i.test(process.env.AUTO_SPANISH_OVERRIDES || "");

// Location of the Spanish auto decisions JSON
const SPANISH_DECISIONS_PATH =
  process.env.SPANISH_DECISIONS || "automation/spanish-country-decisions.json";

  // Comma-separated list of border countries (ES, CH, etc.), from Admin / feeds.json
const BORDER_COUNTRIES = (process.env.BORDER_COUNTRIES || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);



// Add this debug block:
console.log("[border-env] FEED_SLUG =", SLUG);
console.log("[border-env] process.env.BORDER_COUNTRIES =", process.env.BORDER_COUNTRIES || "(none)");
console.log("[border-env] parsed BORDER_COUNTRIES =", BORDER_COUNTRIES);
console.log("[border-env] process.env.BORDER_TZ_MAP_JSON =", process.env.BORDER_TZ_MAP_JSON || "(none)");

/* -------------------------- overrides / border auto-discovery ------------ */
// Base timezone → country mapping (fallback)
const BASE_TZ_TO_COUNTRY = {
  "Europe/Madrid": "ES",
  "Atlantic/Canary": "ES",
  "Europe/Paris": "FR",
  "Europe/Lisbon": "PT",
  // a few obvious extras; can be extended if needed
  "Europe/Zurich": "CH",
  "Europe/Berlin": "DE",
  "Europe/Rome": "IT",
};

// Start with the base map, then overlay the Admin config
let TZ_TO_COUNTRY = { ...BASE_TZ_TO_COUNTRY };

(function applyEnvTimezoneMap() {
  let parsed = {};
  try {
    parsed = JSON.parse(process.env.BORDER_TZ_MAP_JSON || "{}");
  } catch {
    parsed = {};
  }
  if (!parsed || typeof parsed !== "object") return;

  for (const [country, tzList] of Object.entries(parsed)) {
    if (!Array.isArray(tzList)) continue;
    for (const tz of tzList) {
      if (!tz) continue;
      TZ_TO_COUNTRY[String(tz).trim()] = String(country).trim();
    }
  }
})();

/**
 * Map a GTFS stop_timezone string to a country code.
 * If Admin provided a mapping in BORDER_TZ_MAP_JSON it wins;
 * else we fall back to BASE_TZ_TO_COUNTRY.
 */
function tzToCountry(tz) {
  if (!tz) return "UNKNOWN";
  const key = String(tz).trim();
  return TZ_TO_COUNTRY[key] || "UNKNOWN";
}


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

async function loadSpanishDecisionsForSlug(slug) {
  // Use this file whenever we are in "border auto" mode (countries configured)
  // OR in legacy AUTO_SPANISH_OVERRIDES mode.
  if (!AUTO_SPANISH_OVERRIDES && (!BORDER_COUNTRIES || !BORDER_COUNTRIES.length)) {
    return { raw: {}, source: "" };
  }

  if (!(await fileExists(SPANISH_DECISIONS_PATH))) {
    console.log(`[spanish] decisions file not found at ${SPANISH_DECISIONS_PATH}`);
    return { raw: {}, source: "" };
  }

  try {
    const text = await fs.readFile(SPANISH_DECISIONS_PATH, "utf8");
    const json = JSON.parse(text || "{}");
    console.log(
      `[spanish] loaded decisions JSON for slug '${slug}' from ${SPANISH_DECISIONS_PATH}`
    );
    // We keep the whole JSON; later code will read json.decisions["slug::stop_id"]
    return { raw: json, source: SPANISH_DECISIONS_PATH };
  } catch (err) {
    console.error(
      "[spanish] Failed to read/parse decisions:",
      err?.message || err
    );
    return { raw: {}, source: SPANISH_DECISIONS_PATH };
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
  (Papa.parse(text, { header: true, dynamicTyping: false, skipEmptyLines: true }).data || []);

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

  // Allow several wrapper shapes:
  // - { rules: [...] }
  // - { restrictions: [...] }
  // - plain array [...]
  const src = raw?.rules ?? raw?.restrictions ?? raw ?? {};

  // --- ARRAY FORM: [{ trip_id/stop_id or tripId/stopId, ... }, ...] ---
  if (Array.isArray(src)) {
    for (const row of src) {
      // Support both snake_case and camelCase keys
      const tid = String(row.trip_id ?? row.tripId ?? "").trim();
      const sid = String(row.stop_id ?? row.stopId ?? "").trim();
      const mode = String(row.mode ?? "normal").trim();

      if (!tid || !sid || !mode) continue;

      // Be tolerant with dropoffOnlyFrom / pickupOnlyTo naming
      let drop = row.dropoffOnlyFrom ?? row.dropOffOnlyFrom ?? row.dropoff_from;
      let pick = row.pickupOnlyTo   ?? row.pickUpOnlyTo   ?? row.pickup_to;

      if (Array.isArray(drop)) drop = drop.map(String);
      if (Array.isArray(pick)) pick = pick.map(String);

      // If mapper uses "custom" without explicit lists, clamp them here
      if (mode === "custom") {
        const seq = byTripSeq.get(tid) ?? [];
        ({ drop, pick } = clampToTrip(seq, sid, drop, pick));
      }

      out[`${tid}::${sid}`] = {
        mode,
        dropoffOnlyFrom: drop,
        pickupOnlyTo: pick,
      };
    }
    return out;
  }

  // --- OBJECT FORM: { "<trip>::<stop>": { mode, ... }, ... } ---
  if (src && typeof src === "object") {
    for (const k of Object.keys(src)) {
      const r = src[k] || {};
      const [tid, sid] = splitKey(k);
      const mode = String(r.mode ?? "normal").trim();
      if (!tid || !sid || !mode) continue;

      let drop = r.dropoffOnlyFrom ?? r.dropOffOnlyFrom ?? r.dropoff_from;
      let pick = r.pickupOnlyTo   ?? r.pickUpOnlyTo   ?? r.pickup_to;

      if (Array.isArray(drop)) drop = drop.map(String);
      if (Array.isArray(pick)) pick = pick.map(String);

      if (mode === "custom") {
        const seq = byTripSeq.get(tid) ?? [];
        ({ drop, pick } = clampToTrip(seq, sid, drop, pick));
      }

      out[`${tid}::${sid}`] = {
        mode,
        dropoffOnlyFrom: drop,
        pickupOnlyTo: pick,
      };
    }
  }

  return out;
}
// ---------------------- Border auto rules builder (multi-country) --------- //
function buildBorderAutoRestrictions({
  slug,
  stops,
  stopTimes,
  borderCountries,   // e.g. ["ES","CH"]
  borderDecisions,   // JSON from SPANISH_DECISIONS_PATH
}) {
  const borderSet = new Set(
    (borderCountries || []).map((c) => String(c || "").trim()).filter(Boolean)
  );
  if (!borderSet.size) return {};

  // decisions JSON shape we expect:
  // { decisions: { "<slug>::<stop_id>": { newCountry: "ES" | "CH" | ... }, ... } }
  const decisionsBlock =
    borderDecisions && typeof borderDecisions === "object"
      ? borderDecisions.decisions || {}
      : {};

  // 1) Stop → country from timezone
  const stopCountry = new Map();
  for (const s of stops || []) {
    const id = String(s.stop_id ?? "").trim();
    if (!id) continue;
    const tz = s.stop_timezone ?? s.stop_tz;
    const base = tzToCountry(tz);
    stopCountry.set(id, base);
  }

  // 2) Apply explicit decisions (override timezone country) per slug
  for (const [fullKey, val] of Object.entries(decisionsBlock)) {
    const [slugPrefix, stopId] = String(fullKey).split("::");
    if (slugPrefix !== slug) continue;
    if (!stopId) continue;
    const newCountry = (val && val.newCountry) || (val && val.country);
    if (!newCountry) continue;
    stopCountry.set(stopId, String(newCountry).trim());
  }

  // Quick lookup set of "border-country stops"
  const isBorderStop = new Set();
  for (const [sid, country] of stopCountry.entries()) {
    if (borderSet.has(country)) isBorderStop.add(sid);
  }

  // 3) Group stop_times by trip (ordered)
  const byTrip = new Map();
  for (const st of stopTimes || []) {
    const tid = String(st.trip_id ?? "").trim();
    const sid = String(st.stop_id ?? "").trim();
    if (!tid || !sid) continue;
    if (!byTrip.has(tid)) byTrip.set(tid, []);
    byTrip.get(tid).push(st);
  }
  for (const arr of byTrip.values()) {
    arr.sort((a, b) => Number(a.stop_sequence) - Number(b.stop_sequence));
  }

  const restrictions = {};

  for (const [tripId, arr] of byTrip.entries()) {
    const seq = arr.map((st) => String(st.stop_id));

    // Countries this trip touches
    const tripCountries = new Set(
      seq.map((sid) => stopCountry.get(sid) || "UNKNOWN")
    );

    // Only bother with trips that cross at least one border (multi-country)
    if (tripCountries.size <= 1) {
      // Domestic only (e.g. entirely in ES or entirely in CH) → skip for now
      continue;
    }

    // For every stop whose country is in borderSet on a cross-border trip,
    // mark as `custom`. The OD compiler then does the segment split.
    seq.forEach((sid) => {
      if (!isBorderStop.has(sid)) return;
      const key = `${tripId}::${sid}`;
      restrictions[key] = {
        mode: "custom",
        // dropoffOnlyFrom / pickupOnlyTo left undefined:
        // the compiler only needs `mode: "custom"`.
      };
    });
  }

  return restrictions;
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

    // Resolve effective URL vs local path, mirroring the workflow:
    const hasHttpUrl = isHttpUrl(SRC_URL_ENV);
    const effectiveUrl   = hasHttpUrl ? SRC_URL_ENV : "";
    const effectiveLocal = LOCAL_PATH_ENV || (!hasHttpUrl && SRC_URL_ENV ? SRC_URL_ENV : "");

    console.log("[source] SLUG =", SLUG);
    console.log("[source] FEED_URL =", SRC_URL_ENV || "(empty)");
    console.log("[source] FEED_LOCAL_PATH =", LOCAL_PATH_ENV || "(empty)");
    console.log("[source] effectiveUrl =", effectiveUrl || "(none)");
    console.log("[source] effectiveLocal =", effectiveLocal || "(none)");

    // Debug: show whether Spanish auto overrides are enabled for this run
    console.log(
      "[spanish] AUTO_SPANISH_OVERRIDES =",
      AUTO_SPANISH_OVERRIDES ? "true" : "false"
    );
    if (AUTO_SPANISH_OVERRIDES) {
      console.log(
        "[spanish] decisions file =",
        SPANISH_DECISIONS_PATH
      );
    }

    if (effectiveUrl) {
      // Remote URL mode (only true http/https)
      console.log(`Downloading GTFS (${SLUG}) from URL:`, effectiveUrl);
      const headers = {
        Accept: "application/zip, application/octet-stream,*/*",
        "User-Agent": "curl/8.7.1",
      };

      const res = await fetch(effectiveUrl, { headers });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      zipBuffer = await res.arrayBuffer();
      sourceDescriptor = effectiveUrl;
    } else if (effectiveLocal) {
      // Local file mode
      const localPath = path.join("automation", effectiveLocal);
      console.log(`Loading GTFS (${SLUG}) from local file:`, localPath);
      try {
        zipBuffer = await fs.readFile(localPath);
      } catch (err) {
        throw new Error(
          `Failed to read local GTFS file at ${localPath}: ${err.message || err}`
        );
      }
      sourceDescriptor = `local:${localPath}`;
    } else {
      throw new Error(
        "No valid FEED_URL (http/https) or FEED_LOCAL_PATH / local-like FEED_URL provided for GTFS source."
      );
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

    let overridesRaw = {};
    let borderDecisions = null;
    let effectiveOverridesSource = "";

    const borderModeEnabled =
      (BORDER_COUNTRIES && BORDER_COUNTRIES.length > 0) || AUTO_SPANISH_OVERRIDES;


    console.log(
      "[border-mode]",
      "slug =", SLUG,
      "| borderModeEnabled =", borderModeEnabled,
      "| BORDER_COUNTRIES =", BORDER_COUNTRIES,
      "| AUTO_SPANISH_OVERRIDES =", AUTO_SPANISH_OVERRIDES
    );

    if (borderModeEnabled) {
      // "By country" mode: ignore per-trip overrides and rely on
      // timezone + KML decisions (multi-country) for this slug.
      const { raw, source } = await loadSpanishDecisionsForSlug(SLUG);
      borderDecisions = raw || {};
      effectiveOverridesSource = source || "";
      console.log(
        "[overrides] BORDER mode enabled; countries =",
        BORDER_COUNTRIES.length ? BORDER_COUNTRIES.join(",") : "ES (legacy)"
      );
    } else {
      // Default behaviour: generic overrides resolution (unchanged)
      const { text: overridesText, source } = await loadOverridesText();
      effectiveOverridesSource = source;

      try {
        const j = JSON.parse(overridesText || "{}");

        const unwrap = (root) => {
          if (!root || typeof root !== "object") return {};

          // 1) Direct rules/restrictions or array (same as mapper)
          if (
            Array.isArray(root.rules) ||
            Array.isArray(root.restrictions) ||
            Array.isArray(root)
          ) {
            return root;
          }

          // 2) { overrides: { [slug]: {...} } }
          if (root.overrides && typeof root.overrides === "object") {
            if (root.overrides[SLUG]) return root.overrides[SLUG];
            const keys = Object.keys(root.overrides);
            if (keys.length === 1) return root.overrides[keys[0]];
          }

          // 3) { feeds: { [slug]: {...} } } (alternative naming)
          if (root.feeds && typeof root.feeds === "object") {
            if (root.feeds[SLUG]) return root.feeds[SLUG];
            const keys = Object.keys(root.feeds);
            if (keys.length === 1) return root.feeds[keys[0]];
          }

          // Fallback: treat as already-unwrapped body
          return root;
        };

        overridesRaw = unwrap(j);
      } catch (err) {
        console.error("[overrides] Failed to parse JSON:", err?.message || err);
        overridesRaw = {};
      }
    }

    console.log("[overrides] source =", effectiveOverridesSource || "(none)");
    if (!borderModeEnabled) {
      console.log(
        "[overrides] slug =",
        SLUG,
        "| top keys =",
        Object.keys(overridesRaw || {}).slice(0, 5)
      );
    }

    let restrictions = {};
    if (borderModeEnabled) {
      const effectiveCountries =
        BORDER_COUNTRIES && BORDER_COUNTRIES.length
          ? BORDER_COUNTRIES
          : ["ES"]; // legacy Spanish-only mode

      restrictions = buildBorderAutoRestrictions({
        slug: SLUG,
        stops,
        stopTimes,
        borderCountries: effectiveCountries,
        borderDecisions,
      });
    } else {
      restrictions = importOverridesTolerant(overridesRaw, stopTimes);
    }

    const entries = Object.entries(restrictions);
    METRICS.overrides.total = entries.length;
    for (const [, v] of entries) {
      const m = v?.mode || "normal";
      if (!METRICS.overrides.byMode[m]) METRICS.overrides.byMode[m] = 0;
      METRICS.overrides.byMode[m]++;
    }

    const present = new Set(stopTimes.map(st => `${st.trip_id}::${st.stop_id}`));

    METRICS.overrides.matchedPairs = 0;
    METRICS.overrides.unmatchedPairs = 0;

    for (const [k, r] of Object.entries(restrictions)) {
      if (present.has(k)) {
        METRICS.overrides.matchedPairs++;
      } else {
        METRICS.overrides.unmatchedPairs++;
        METRICS.missing.tripStopPairs++;
        METRICS.warnings.push(`Rule key not found in feed: ${k}`);
      }
    }

    const { trips: outTrips, stop_times: outStopTimes } =
      compileTripsWithOD({ trips, stop_times: stopTimes }, restrictions);

    // ---------------- routesAffected (by route_id) -----------------
    // METRICS.trips.touched contains trip_ids whose stop_times were changed
    const touchedTripIds = METRICS.trips.touched;
    const affectedRouteIdSet = new Set();

    for (const tr of trips) {
      if (touchedTripIds.has(tr.trip_id)) {
        const rid = tr.route_id;
        if (rid != null && rid !== "") {
          affectedRouteIdSet.add(String(rid));
        }
      }
    }

    // Sorted for stability (numeric-ish sort)
    const routesAffected = Array.from(affectedRouteIdSet).sort((a, b) =>
      String(a).localeCompare(String(b), "en", { numeric: true, sensitivity: "base" })
    );

    // ---------------- debug samples -----------------
    const restrictionEntries = Object.entries(restrictions);
    const sampleOverrides = restrictionEntries.slice(0, 20).map(([key, val]) => ({
      key,
      mode: val.mode,
      dropoffOnlyFrom: val.dropoffOnlyFrom,
      pickupOnlyTo: val.pickupOnlyTo,
    }));

    const modifiedSamples = outStopTimes
      .filter(st => (st.pickup_type ?? 0) !== 0 || (st.drop_off_type ?? 0) !== 0)
      .slice(0, 50)
      .map(st => ({
        trip_id: st.trip_id,
        stop_id: st.stop_id,
        stop_sequence: st.stop_sequence,
        pickup_type: st.pickup_type,
        drop_off_type: st.drop_off_type,
      }));

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

    // Build ZIP buffer (this was missing in your current file)
    const blob = await outZip.generateAsync({
      type: "nodebuffer",
      compression: "DEFLATE",
      compressionOptions: { level: 9 },
    });

    const outPath = path.join(OUT_DIR, OUT_ZIP);
    await fs.writeFile(outPath, blob);
    console.log("Wrote:", outPath);

    // NEW: write gtfs_rules file based on affected routes
    try {
      // Allow overriding the feed name used in the rule file; fallback to slug.
      const ruleFeedName = process.env.RULE_FEED_NAME || SLUG;

      const rulesLines = [];
      rulesLines.push(`| feed(name == "${ruleFeedName}")`);
      rulesLines.push("{");

      if (routesAffected.length) {
        rulesLines.push("    - route(");
        routesAffected.forEach((rid, idx) => {
          const prefix = idx === 0 ? "        " : "        && ";
          rulesLines.push(`${prefix}id != "${rid}"`);
        });
        rulesLines.push("    );");
      } else {
        rulesLines.push("    // No routes affected by overrides for this feed.");
      }

      rulesLines.push("}");

      const rulesContent = rulesLines.join("\n");
      const rulesFilePath = path.join(OUT_DIR, "gtfs_rules");
      await fs.writeFile(rulesFilePath, rulesContent, "utf8");
      console.log("Wrote rules:", rulesFilePath);
    } catch (err) {
      console.error("Failed to write gtfs_rules file:", err?.message || err);
    }

    const report = {
      feed: SLUG,
      overrides: METRICS.overrides,
      trips: {
        touchedCount: METRICS.trips.touched.size,
        createdSegments: METRICS.trips.createdSegments,
      },
      stops: { touchedCount: METRICS.stops.touched.size },
      stopTimes: METRICS.stopTimes,

      // NEW: routes affected by overrides / OD split
      routes: {
        affectedCount: routesAffected.length,
        affectedRouteIds: routesAffected,
      },

      missing: METRICS.missing,
      warnings: METRICS.warnings,
      generatedAt: new Date().toISOString(),
      source: sourceDescriptor,
      overridesSource: effectiveOverridesSource || "",
      artifacts: {
        zip: path.join(OUT_DIR, OUT_ZIP),
        gtfs_rules: path.join(OUT_DIR, "gtfs_rules"),
      },

      // debug info
      debug: {
        sampleOverrides,
        modifiedStopTimesSample: modifiedSamples,
        spanishAutoEnabled: AUTO_SPANISH_OVERRIDES,
        spanishDecisionsFile: borderModeEnabled ? SPANISH_DECISIONS_PATH : "",
        borderModeEnabled,
        borderCountries: BORDER_COUNTRIES,
      },
    };

    const reportPath = path.join(OUT_DIR, OUT_REPORT);
    await fs.writeFile(reportPath, JSON.stringify(report, null, 2), "utf8");
    console.log("Report:", reportPath);

    const lines = [
      `=== GTFS Rebuild — ${SLUG} ===`,
      `Overrides: total=${report.overrides.total}  (pickup=${report.overrides.byMode.pickup || 0}, dropoff=${report.overrides.byMode.dropoff || 0}, custom=${report.overrides.byMode.custom || 0})`,
      `Trips: touched=${report.trips.touchedCount}, createdSegments=${report.trips.createdSegments}`,
      `Routes: affected=${report.routes.affectedCount}`,
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