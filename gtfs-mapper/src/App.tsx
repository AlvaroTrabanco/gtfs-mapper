import { unstable_batchedUpdates } from "react-dom";
import { useEffect, useMemo, useState, useCallback, useRef, startTransition } from "react";
import { MapContainer, TileLayer, Polyline, CircleMarker, useMapEvents, useMap, Pane, Marker, GeoJSON } from "react-leaflet";
import L from "leaflet";
import JSZip from "jszip";
import { saveAs } from "file-saver";
import * as Papa from "papaparse";
import { v4 as uuidv4 } from "uuid";
import PatternMatrix from "./PatternMatrix";



/** ---------- Misc ---------- */
const defaultTZ: string = String(Intl.DateTimeFormat().resolvedOptions().timeZone || "Europe/Madrid");
const MIN_ADD_ZOOM = 11;
const MIN_STOP_ZOOM = 6;
const MIN_ROUTE_ZOOM = 6;      // ← NEW: don’t draw route lines when zoomed way out
const MAX_ROUTE_POINTS = 2000; // ← NEW: cap points per polyline drawn to the screen
const DIM_ROUTE_COLOR = "#2b2b2b";
const MAX_TRIP_GROUPS_RENDERED = 12; // render at most 12 trip sections at once


/** ---------- Types ---------- */
type ShapeByRoute = Record<string, string>; // route_id -> shape_id
type Stop = { uid: string; stop_id: string; stop_name: string; stop_lat: number; stop_lon: number };
type RouteRow = { route_id: string; route_short_name: string; route_long_name: string; route_type: number; agency_id: string };
type Service = {
  service_id: string;
  monday: number; tuesday: number; wednesday: number; thursday: number; friday: number; saturday: number; sunday: number;
  start_date: string; end_date: string;
};
type Trip = {
  route_id: string;
  service_id: string;
  trip_id: string;
  trip_headsign?: string;
  shape_id?: string;
  direction_id?: number;      // ← number (not string)
};

type StopTime = {
  trip_id: string;
  arrival_time: string;
  departure_time: string;
  stop_id: string;
  stop_sequence: number;      // ← number only
  pickup_type?: number;
  drop_off_type?: number;
};
type ShapePt = { shape_id: string; lat: number; lon: number; seq: number };
type StopTimesByTrip = Map<string, StopTime[]>;
type Agency = { agency_id: string; agency_name: string; agency_url: string; agency_timezone: string };

type Banner = { kind: "success" | "error" | "info"; text: string } | null;




type StopRuleMode = "normal" | "pickup" | "dropoff" | "custom";
type ODRestriction = {
  mode: StopRuleMode;
  dropoffOnlyFrom?: string[];
  pickupOnlyTo?: string[];
};
type DirStr = "" | "0" | "1";
type ScopedStopKey = `${string}::${DirStr}::${string}`;
export const stopDefaultKey = (
  route_id: string,
  direction_id: 0 | 1 | undefined,
  stop_id: string
): ScopedStopKey =>
  `${route_id}::${direction_id == null ? "" : (direction_id as 0 | 1)}::${stop_id}`;

export const parseStopDefaultKey = (k: ScopedStopKey) => {
  const [route_id, dir, stop_id] = k.split("::") as [string, DirStr, string];
  const direction_id = dir === "" ? undefined : (Number(dir) as 0 | 1);
  return { route_id, direction_id, stop_id };
};

type StopDefaultsMap = Record<ScopedStopKey, ODRestriction>;


const asMode = (m?: StopRuleMode): StopRuleMode => (m ?? "normal");


type RestrictionsMap = Record<string, ODRestriction>;

type StopDefaults = { dwell?: number; pickup?: number; dropoff?: number };




/** ---------- Helpers ---------- */

// -- micro-yield so the browser can breathe during huge imports
const tick = () => new Promise<void>(r => setTimeout(r, 0));

// -- CSV parse off the main thread (Papa worker + chunking)
function parseCsvFast<T = any>(text: string): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const rows: T[] = [];
    Papa.parse<T>(text, {
      header: true,
      worker: true,            // parse in a Web Worker
      skipEmptyLines: true,
      dynamicTyping: false,    // coerce later; parsing is faster
      chunkSize: 1024 * 1024,  // ~1MB chunks keep UI responsive
      chunk: (result: Papa.ParseResult<T>, _parser: Papa.Parser) => {
        rows.push(...result.data);
      },
      complete: (_result: Papa.ParseResult<T>) => resolve(rows),
      error: (err: unknown) => reject(err),
    } as Papa.ParseConfig<T>);
  });
}


// --- DEBUG logger ---
// --- DEBUG logger ---
const DEBUG =
  (typeof window !== "undefined" && (window as any).GTFS_DEBUG === true) ||
  localStorage.getItem("GTFS_DEBUG") === "1";

export const enableGtfsDebug = () => { localStorage.setItem("GTFS_DEBUG", "1"); };
export const disableGtfsDebug = () => { localStorage.removeItem("GTFS_DEBUG"); };

const log = (...args: any[]) => {
  if (!DEBUG) return;
  (console.debug || console.log)("[GTFS]", ...args);
};

function normalizeRule(raw: any): ODRestriction {
  const mode: StopRuleMode =
    raw?.mode === "pickup" || raw?.mode === "dropoff" || raw?.mode === "custom"
      ? raw.mode
      : "normal";
  return {
    mode,
    dropoffOnlyFrom: Array.isArray(raw?.dropoffOnlyFrom) ? raw.dropoffOnlyFrom.map(String) : undefined,
    pickupOnlyTo: Array.isArray(raw?.pickupOnlyTo) ? raw.pickupOnlyTo.map(String) : undefined,
  };
}

function normalizeSavedStopDefaults(raw: unknown): StopDefaultsMap {
  const out: StopDefaultsMap = {} as any;
  if (!raw || typeof raw !== "object") return out;
  for (const [k, v] of Object.entries(raw as Record<string, any>)) {
    if (k.includes("::")) {
      out[k as ScopedStopKey] = normalizeRule(v);
    } else {
      // legacy: promote to global (no route, any direction)
      out[stopDefaultKey("", undefined, k)] = normalizeRule(v);
    }
  }
  return out;
}

/** ---------- Overrides import helpers (tolerant) ---------- */
const KEY_DELIMS = ["::", "|", "/", "—", "–", "-"];
function splitKey(k: string): [string, string] {
  for (const d of KEY_DELIMS) {
    if (k.includes(d)) {
      const [a, b] = k.split(d);
      return [String(a ?? "").trim(), String(b ?? "").trim()];
    }
  }
  // fallback: try "<trip> <stop>" where last token looks like an id
  const m = String(k).match(/^(.+?)\s+([A-Za-z0-9._:-]{3,})$/);
  return m ? [m[1].trim(), m[2].trim()] : ["", ""];
}

type ImportLike = {
  // either a map {"trip::stop": {...}} or an array of rows
  restrictions?:
    | Record<string, ODRestriction>
    | Array<{
        trip_id: string;
        stop_id: string;
        mode: StopRuleMode;
        dropoffOnlyFrom?: string[];
        pickupOnlyTo?: string[];
      }>;
  // either a map {"stop_id": {...}} or an array of rows
  stopDefaults?:
    | Record<string, ODRestriction>
    | Array<{
        stop_id: string;
        mode: StopRuleMode;
        dropoffOnlyFrom?: string[];
        pickupOnlyTo?: string[];
      }>;
};

function indexStopTimes(rows: StopTime[]) {
  const byTrip = new Map<string, string[]>();
  for (const st of rows) {
    if (!byTrip.has(st.trip_id)) byTrip.set(st.trip_id, []);
    byTrip.get(st.trip_id)!.push(st.stop_id);
  }
  return byTrip;
}

function clampToTrip(
  seq: string[],
  sid: string,
  drop?: string[],
  pick?: string[]
) {
  if (!seq.length) return { drop, pick };
  const idx = seq.indexOf(sid);
  if (idx === -1) return { drop, pick };
  const up = new Set(seq.slice(0, idx));
  const down = new Set(seq.slice(idx + 1));
  const d = drop?.filter((x) => up.has(x));
  const p = pick?.filter((x) => down.has(x));
  return { drop: d?.length ? d : undefined, pick: p?.length ? p : undefined };
}

/** Accepts map/array forms, trims keys, accepts multiple delimiters */
function importOverridesTolerant(
  raw: unknown,
  allStopTimes: StopTime[]
): { restrictions: RestrictionsMap; stopDefaults: StopDefaultsMap } {
  const tripIndex = indexStopTimes(allStopTimes);
  const outR: RestrictionsMap = {};
  const outD: StopDefaultsMap = {};
  const data = (raw ?? {}) as ImportLike;

  // restrictions
  const r = data.restrictions;
  if (Array.isArray(r)) {
    for (const row of r) {
      const tid = String(row.trip_id ?? "").trim();
      const sid = String(row.stop_id ?? "").trim();
      const mode = row.mode as StopRuleMode;
      if (!tid || !sid || !mode) continue;
      let drop = row.dropoffOnlyFrom,
        pick = row.pickupOnlyTo;
      if (mode === "custom") {
        const seq = tripIndex.get(tid) ?? [];
        const clamped = clampToTrip(seq, sid, drop, pick);
        drop = clamped.drop;
        pick = clamped.pick;
      }
      outR[`${tid}::${sid}`] = { mode, dropoffOnlyFrom: drop, pickupOnlyTo: pick };
    }
  } else if (r && typeof r === "object") {
    for (const k of Object.keys(r)) {
      const [tid, sid] = splitKey(k);
      const val = (r as Record<string, ODRestriction>)[k];
      if (!tid || !sid || !val?.mode) continue;
      let drop = val.dropoffOnlyFrom,
        pick = val.pickupOnlyTo;
      if (val.mode === "custom") {
        const seq = tripIndex.get(tid) ?? [];
        const clamped = clampToTrip(seq, sid, drop, pick);
        drop = clamped.drop;
        pick = clamped.pick;
      }
      outR[`${tid}::${sid}`] = {
        mode: val.mode,
        dropoffOnlyFrom: drop,
        pickupOnlyTo: pick,
      };
    }
  }

  // per-stop defaults


  const d = data.stopDefaults;
  if (Array.isArray(d)) {
    for (const it of d) {
      const sid = String(it.stop_id ?? "").trim();
      if (!sid || !it.mode) continue;
      outD[stopDefaultKey("", undefined, sid) as ScopedStopKey] = {
        mode: it.mode,
        dropoffOnlyFrom: it.dropoffOnlyFrom,
        pickupOnlyTo: it.pickupOnlyTo,
      };
    }
  } else if (d && typeof d === "object") {
    for (const sid of Object.keys(d)) {
      const it = (d as Record<string, ODRestriction>)[sid];
      if (!it?.mode) continue;
      outD[stopDefaultKey("", undefined, String(sid)) as ScopedStopKey] = {
        mode: it.mode,
        dropoffOnlyFrom: it.dropoffOnlyFrom,
        pickupOnlyTo: it.pickupOnlyTo,
      };
    }
  }

  return { restrictions: outR, stopDefaults: outD };
}

/** Auditor: counts and shows a few examples of non-matches */
function auditOverrides(imported: RestrictionsMap, allStopTimes: StopTime[]) {
  const byTrip = indexStopTimes(allStopTimes);
  let ok = 0,
    badTrip = 0,
    badStop = 0,
    notOnTrip = 0;
  const examples: string[] = [];

  for (const k of Object.keys(imported)) {
    const [tid, sid] = splitKey(k);
    const seq = byTrip.get(tid);
    if (!seq) {
      badTrip++;
      if (examples.length < 5) examples.push(`No trip: ${tid}`);
      continue;
    }
    if (!sid) {
      badStop++;
      if (examples.length < 5) examples.push(`Bad key (no stop): ${k}`);
      continue;
    }
    if (!seq.includes(sid)) {
      notOnTrip++;
      if (examples.length < 5) examples.push(`Stop ${sid} not on trip ${tid}`);
      continue;
    }
    ok++;
  }

  return { ok, badTrip, badStop, notOnTrip, examples };
}

function toYYYYMMDD(d: string | Date) {
  const date = typeof d === "string" ? new Date(d) : d;
  const y = date.getFullYear();
  const m = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  return `${y}${m}${day}`;
}
function csvify<T extends Record<string, any>>(rows: T[], headerOrder?: string[]) {
  const headers = (headerOrder?.length
    ? headerOrder
    : (rows && rows.length ? Object.keys(rows[0]) : [])) as string[];

  // always emit headers so files aren't empty (GTFS tools expect headers)
  const out: string[] = [];
  if (headers.length) out.push(headers.join(","));

  const escape = (val: unknown) => {
    const s = String(val ?? "");
    // escape if it contains comma, quote, or newline
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
    };

  for (const r of (rows || [])) {
    out.push(headers.map(h => escape((r as any)[h])).join(","));
  }
  return out.join("\n");
}
const num = (x: any, def = 0) => {
  const n = Number(x);
  return Number.isFinite(n) ? n : def;
};
function timeToSeconds(t?: string | null) {
  if (!t) return null;
  const m = String(t).match(/^\s*(\d+):(\d{2})(?::(\d{2}))?\s*$/);
  if (!m) return null;
  const h = parseInt(m[1], 10), mi = parseInt(m[2], 10), s = parseInt(m[3] || "0", 10);
  return h*3600 + mi*60 + s;
}
function isMonotonicNonDecreasing(times: (string | undefined)[]) {
  const seq = times.map(timeToSeconds).filter(v => v !== null) as number[];
  for (let i = 1; i < seq.length; i++) if (seq[i-1] > seq[i]) return false;
  return true;
}

/** UI shows HH:MM, storage HH:MM:00 (or "" for empty) */
function uiFromGtfs(t: string | undefined): string {
  if (!t) return "";
  const m = t.match(/^\s*(\d+):(\d{2})(?::\d{2})?\s*$/);
  if (!m) return t;
  return `${m[1].padStart(2, "0")}:${m[2]}`;
}
function gtfsFromUi(t: string | undefined): string {
  if (!t) return "";
  const trimmed = t.trim();
  if (!trimmed) return ""; // empty allowed
  const m = trimmed.match(/^(\d{1,2}):(\d{2})$/);
  if (!m) return trimmed; // allow pasting full GTFS time too
  const hh = m[1].padStart(2, "0");
  const mm = m[2];
  return `${hh}:${mm}:00`;
}
// Export helper: HH:MM -> HH:MM:SS
function toHHMMSS(s?: string | null) {
  if (!s) return "";
  const m = String(s).match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return s;
  return `${m[1].padStart(2, "0")}:${m[2]}:${(m[3] ?? "00").padStart(2, "0")}`;
}

/** ---------- OSRM polyline decoder ---------- */
function decodePolyline(polyline: string, precision = 1e-5): [number, number][] {
  let index = 0, lat = 0, lon = 0, coords: [number, number][] = [];
  while (index < polyline.length) {
    let b, shift = 0, result = 0;
    do { b = polyline.charCodeAt(index++) - 63; result |= (b & 0x1f) << shift; shift += 5; } while (b >= 0x20);
    const dlat = (result & 1) ? ~(result >> 1) : (result >> 1);
    lat += dlat;
    shift = 0; result = 0;
    do { b = polyline.charCodeAt(index++) - 63; result |= (b & 0x1f) << shift; shift += 5; } while (b >= 0x20);
    const dlon = (result & 1) ? ~(result >> 1) : (result >> 1);
    lon += dlon;
    coords.push([lat * precision, lon * precision]);
  }
  return coords;
}


// --- geometry helpers for route caching ---
type LatLng = [number, number];
type RouteGeom = { coords: LatLng[]; bbox: L.LatLngBounds };

const bboxOf = (coords: LatLng[]): L.LatLngBounds => {
  let minLat =  90, minLng = 180, maxLat = -90, maxLng = -180;
  for (const [lat, lng] of coords) {
    if (lat < minLat) minLat = lat;
    if (lng < minLng) minLng = lng;
    if (lat > maxLat) maxLat = lat;
    if (lng > maxLng) maxLng = lng;
  }
  return L.latLngBounds(L.latLng(minLat, minLng), L.latLng(maxLat, maxLng));
};

const decimate = (coords: LatLng[], maxPts = MAX_ROUTE_POINTS): LatLng[] => {
  const n = coords.length;
  if (n <= maxPts) return coords;
  const step = Math.ceil(n / maxPts);
  const out: LatLng[] = [];
  for (let i = 0; i < n; i += step) out.push(coords[i]);
  if (out[out.length - 1] !== coords[n - 1]) out.push(coords[n - 1]);
  return out;
};

/** ---------- Map bits ---------- */



function MapStateTracker({
  onChange,
}: { onChange: (z: number, b: L.LatLngBounds, c: L.LatLng) => void }) {
  const map = useMap();
  useEffect(() => {
    const emit = () => onChange(map.getZoom(), map.getBounds(), map.getCenter());
    emit();
    map.on("moveend", emit);
    map.on("zoomend", emit);
    return () => {
      map.off("moveend", emit);
      map.off("zoomend", emit);
    };
  }, [map, onChange]);
  return null;
}

/** Click anywhere to relocate the currently selected stop */

function RelocateSelectedStopOnMapClick({
  onClick,
}: {
  onClick: (lat: number, lng: number) => void;
}) {
  useMapEvents({
    click(e) {
      onClick(e.latlng.lat, e.latlng.lng);
    },
  });
  return null;
}





/** ---------- Advanced filter parsing ---------- */
function looksAdvanced(q: string) { return /(&&|\|\||==|!=|>=|<=|>|<|~=|!~=)/.test(q); }
function tryNumber(v: string) { const n = Number(v); return Number.isFinite(n) ? n : null; }
function normalizeValue(raw: any): string { return raw == null ? "" : String(raw); }
function parseValueToken(tok: string): string {
  const t = tok.trim();
  if ((t.startsWith('"') && t.endsWith('"')) || (t.startsWith("'") && t.endsWith("'"))) return t.slice(1, -1);
  return t;
}
type Cond = { field: string; op: string; value: string };
function splitByLogical(expr: string): string[][] {
  const orParts = expr.split(/\|\|/);
  return orParts.map(part => part.split(/&&/));
}
function parseCond(raw: string): Cond | null {
  const m = raw.match(/^\s*([a-zA-Z0-9_]+)\s*(==|!=|>=|<=|>|<|~=|!~=)\s*(.+?)\s*$/);
  if (!m) return null;
  const [, field, op, rhs] = m;
  return { field, op, value: parseValueToken(rhs) };
}
function cmp(op: string, left: any, right: any): boolean {
  const lstr = normalizeValue(left);
  const rstr = normalizeValue(right);
  const ln = tryNumber(lstr);
  const rn = tryNumber(rstr);
  const bothNums = ln !== null && rn !== null;

  if (bothNums) {
    switch (op) {
      case "==": return ln === rn;
      case "!=": return ln !== rn;
      case ">":  return ln > rn;
      case "<":  return ln < rn;
      case ">=": return ln >= rn;
      case "<=": return ln <= rn;
      case "~=": return String(lstr).toLowerCase().includes(String(rstr).toLowerCase());
      case "!~=": return !String(lstr).toLowerCase().includes(String(rstr).toLowerCase());
      default: return false;
    }
  } else {
    switch (op) {
      case "==": return lstr === rstr;
      case "!=": return lstr !== rstr;
      case ">":  return lstr >  rstr;
      case "<":  return lstr <  rstr;
      case ">=": return lstr >= rstr;
      case "<=": return lstr <= rstr;
      case "~=": return lstr.toLowerCase().includes(rstr.toLowerCase());
      case "!~=": return !lstr.toLowerCase().includes(rstr.toLowerCase());
      default: return false;
    }
  }
}
function matchesAdvancedRow(row: Record<string, any>, expr: string): boolean {
  const orGroups = splitByLogical(expr);
  for (const andGroup of orGroups) {
    let ok = true;
    for (let raw of andGroup) {
      const c = parseCond(raw);
      if (!c) { ok = false; break; }
      const val = row[c.field];
      if (!cmp(c.op, val, c.value)) { ok = false; break; }
    }
    if (ok) return true;
  }
  return false;
}

/** ---------- Generic table with delete & multi-select ---------- */
// ⬆️ in the props:
function PaginatedEditableTable<T extends Record<string, any>>({
  title, rows, onChange,
  visibleIndex,
  initialPageSize = 5,
  onRowClick,
  selectedPredicate,
  selectedIcon = "✓",
  clearSignal = 0,
  onIconClick,
  selectOnCellFocus = false,
  onDeleteRow,
  enableMultiSelect = false,
  primaryAction,
  cellRenderer,
  onAddRow,
  addRowLabel = "Add",
  headerExtras,
  emptyText = "No rows.",
  onSearchTyping,

}: {
  title: string;
  onSearchTyping?: (value: string) => void
  rows: T[];
  onChange: (next: T[]) => void;
  visibleIndex?: number[];
  initialPageSize?: 5|10|20|50|100;
  onRowClick?: (row: T, e: React.MouseEvent) => void;
  selectedPredicate?: (row: T) => boolean;
  selectedIcon?: string;
  clearSignal?: number;
  onIconClick?: (row: T) => void;
  selectOnCellFocus?: boolean;
  onDeleteRow?: (row: T) => void;
  enableMultiSelect?: boolean;
  primaryAction?: { label: string; onClick: () => void };
  onAddRow?: () => void;
  addRowLabel?: string;
  cellRenderer?: (args: {
    row: T;
    column: string;
    globalIndex: number;
    onEdit: (key: string, value: any) => void;
  }) => React.ReactNode;
  headerExtras?: React.ReactNode;
  emptyText?: string;
}) {

  const editCell = (globalIndex: number, key: string, v: any) => {
    const next = rows.slice();
    next[globalIndex] = { ...next[globalIndex], [key]: v };
    onChange(next);
  };

  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState<5|10|20|50|100>(initialPageSize);
  const [query, setQuery] = useState("");


  useEffect(() => { setQuery(""); setPage(1); }, [clearSignal]);

  const baseIdx = useMemo(
    () => (visibleIndex && visibleIndex.length ? visibleIndex.slice() : rows.map((_, i) => i)),
    [visibleIndex, rows]
  );

  const cols = useMemo<string[]>(
    () => (rows.length ? Object.keys(rows[0] as object) : []),
    [rows]
  );

  const filteredIdx = useMemo(() => {
    const q = query.trim();
    if (!q) return baseIdx;

    if (looksAdvanced(q)) {
      const out: number[] = [];
      for (const gi of baseIdx) {
        const r = rows[gi] as Record<string, any>;
        if (matchesAdvancedRow(r, q)) out.push(gi);
      }
      return out;
    }

    const ql = q.toLowerCase();
    const out: number[] = [];
    for (const gi of baseIdx) {
      const r = rows[gi] as Record<string, any>;
      if (cols.some(c => String(r[c] ?? "").toLowerCase().includes(ql))) out.push(gi);
    }
    return out;
  }, [query, baseIdx, rows, cols]);

  const pageCount = Math.max(1, Math.ceil(filteredIdx.length / pageSize));
  const safePage = Math.min(page, pageCount);
  const start = (safePage - 1) * pageSize;
  const pageIdx = filteredIdx.slice(start, start + pageSize);

  useEffect(() => { setPage(1); }, [pageSize, query, visibleIndex, rows]);

  // ✅ Prevent crashes if indices go stale — only keep valid ones
  const safePageIdx = useMemo(
    () => pageIdx.filter((gi) => rows[gi] !== undefined),
    [pageIdx, rows]
  );



  return (
    <div className="card section" style={{ marginTop: 12 }}>
      <div className="card-body">
        <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 12 }}>
          <h3 style={{ margin: 0 }}>
            {title}
            <span style={{ opacity: .6, fontWeight: 400 }}>
              ({rows.length} rows{filteredIdx.length !== rows.length ? ` | filtered: ${filteredIdx.length}` : ""})
            </span>
          </h3>

          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
            {/* NEW: put your custom buttons here */}
            {headerExtras}

            {primaryAction && (
              <button className="btn" onClick={primaryAction.onClick}>
                {primaryAction.label}
              </button>
            )}

            {onAddRow && (
              <button className="btn" onClick={onAddRow}>
                + {addRowLabel || "Add"}
              </button>
            )}

            <input
              value={query}
              onChange={(e) => {
                const v = e.target.value;
                setQuery(v);
                onSearchTyping?.(v);        // ← notify parent on every keystroke
              }}
              placeholder={`Search…  (e.g. route_id == "18" && service_id == "6")`}
              style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #e3e3e3", width: 320 }}
            />
            <label style={{ fontSize: 13, opacity: .75 }}>Show</label>
            <select
              value={pageSize}
              onChange={(e) => setPageSize(Number(e.target.value) as any)}
              style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #e3e3e3" }}
            >
              {[5,10,20,50,100].map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
        </div>

        <div className="overflow-auto" style={{ borderRadius: 12, border: "1px solid #eee", marginTop: 8 }}>
          <table style={{ width: "100%", fontSize: 13, minWidth: 820 }}>
            <thead>
              <tr>
                {!!selectedPredicate && <th style={{ width: 28 }}></th>}
                {cols.map(c => <th key={c} style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>{c}</th>)}
                {!!onDeleteRow && <th style={{ width: 30 }}></th>}
              </tr>
            </thead>
            <tbody>
              {safePageIdx.length ? safePageIdx.map((gi) => {
                const r = rows[gi];
                if (!r) return null;

                const isSelected = selectedPredicate ? !!selectedPredicate(r) : false;
                
                return (
                  <tr
                    key={gi}
                    onClick={(e) => onRowClick?.(r, e)}
                    style={{
                      cursor: onRowClick ? "pointer" : "default",
                      background: isSelected ? "rgba(232, 242, 255, 0.7)" : "transparent",
                      outline: isSelected ? "2px solid #7db7ff" : "none",
                      outlineOffset: -2
                    }}
                  >
                    {!!selectedPredicate && (
                      <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4, textAlign: "center" }}>
                        {isSelected ? (
                          <button
                            title="Deselect"
                            onClick={(e) => { e.stopPropagation(); onIconClick?.(r); }}
                            style={{ border:"none", background:"transparent", cursor:"pointer", fontSize:14 }}
                          >
                            ✓
                          </button>
                        ) : ""}
                      </td>
                    )}

                    {cols.map((c) => (
                      <td key={c} style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                        <input
                          value={(r as any)[c] ?? ""}
                          name={`cell-${c}`}               // avoid autofill heuristics
                          autoComplete="off"               // prevent Chrome suggestions overshadowing datalist
                          list={c === "stop_id" ? "gtfs-stop-ids" : c === "stop_name" ? "gtfs-stop-names" : undefined}
                          onFocus={selectOnCellFocus && onRowClick ? (e) => onRowClick(r, e as any) : undefined}
                          onChange={(e) => editCell(gi, c, e.target.value)}
                          placeholder={c === "stop_id" || c === "stop_name" ? "type to search…" : undefined}
                          style={{ width: "100%", outline: "none", border: "1px solid #e8e8e8", padding: "4px 6px", borderRadius: 8, background: "white" }}
                        />
                      </td>
                    ))}

                    {!!onDeleteRow && (
                      <td style={{ borderBottom: "1px solid #f3f3f3", padding: 0, textAlign: "center" }}>
                        <button
                          title="Delete row"
                          onClick={(e) => { e.stopPropagation(); onDeleteRow(r); }}
                          style={{ border:"none", background:"transparent", cursor:"pointer", width: 28, height: 28, lineHeight: "28px" }}
                        >×</button>
                      </td>
                    )}
                  </tr>
                );
              }) : (
                <tr>
                  <td
                    colSpan={(selectedPredicate ? 1 : 0) + Math.max(1, cols.length) + (onDeleteRow ? 1 : 0)}
                    style={{ padding: 12, opacity: .6 }}
                  >
                    {emptyText ?? "No rows."}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, alignItems: "center" }}>
          <div style={{ fontSize: 12, opacity: .7 }}>
            Page {pageIdx.length ? safePage : 1} of {pageCount}
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <button className="btn" disabled={safePage <= 1} onClick={() => setPage(p => Math.max(1, p - 1))}>Prev</button>
            <button className="btn" disabled={safePage >= pageCount} onClick={() => setPage(p => Math.min(pageCount, p + 1))}>Next</button>
          </div>
        </div>
      </div>
    </div>
  );
}

/** ---------- Pattern helpers ---------- */
function isSubsequence(small: string[], big: string[]) {
  if (!small.length) return true;
  let i = 0;
  for (const x of big) {
    if (x === small[i]) { i++; if (i === small.length) return true; }
  }
  return false;
}

/** ---------- Tiny chip ---------- */
function ServiceChip({
  svc, onToggle, active,
  days, range,
}: {
  svc: string; active: boolean; onToggle: () => void;
  days?: { mo:number; tu:number; we:number; th:number; fr:number; sa:number; su:number };
  range?: { start: string; end: string };
}) {
  const dayStr = days
    ? ["M","T","W","T","F","S","S"].map((d, i) => {
        const on = [days.mo,days.tu,days.we,days.th,days.fr,days.sa,days.su][i] ? 1 : 0;
        return `<span style="opacity:${on?1:.3}">${d}</span>`;
      }).join("")
    : "";
  const dateStr = range ? `${range.start.slice(4,6)}/${range.start.slice(6)}–${range.end.slice(4,6)}/${range.end.slice(6)}` : "";
  return (
    <button
      className="chip"
      onClick={onToggle}
      title={`service_id ${svc}`}
      style={{
        padding: "2px 6px",
        borderRadius: 999,
        border: "1px solid #e1e5ea",
        background: active ? "#e8f2ff" : "#fff",
        fontSize: 11,
        lineHeight: 1.2,
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        cursor: "pointer"
      }}
    >
      <span style={{ fontWeight: 600 }}>{svc}</span>
      <span dangerouslySetInnerHTML={{ __html: dayStr }} />
      <span style={{ opacity: .6 }}>{dateStr}</span>
    </button>
  );
}


function MapClickMenuTrigger({
  onShow,
  onDeselect,
  hasRouteSelection = false,
}: {
  onShow: (info: { x: number; y: number; lat: number; lng: number }) => void;
  onDeselect: () => void;                     // ← called on background click when a route is selected
  hasRouteSelection?: boolean;                // ← true if any route is currently selected
}) {
  const map = useMap();

  // detect “real click” vs “dragged”
  const downPtRef = useRef<L.Point | null>(null);
  const movedRef  = useRef(false);
  const DRAG_TOL = 6; // px

  const isInteractiveTarget = (t: HTMLElement | null) =>
    !!t &&
    (t.closest(".leaflet-marker-pane") ||
     t.closest(".leaflet-interactive") ||
     t.closest(".leaflet-popup") ||
     t.closest(".leaflet-tooltip"));

  useMapEvents({
    mousedown(e) {
      downPtRef.current = map.latLngToContainerPoint(e.latlng);
      movedRef.current = false;
    },
    mousemove(e) {
      if (!downPtRef.current) return;
      const p = map.latLngToContainerPoint(e.latlng);
      if (p.distanceTo(downPtRef.current) > DRAG_TOL) movedRef.current = true;
    },
    mouseup() {
      // leave refs set; we’ll read them in click
    },

    click(e) {
      // If map was dragged, ignore this click
      if (movedRef.current) return;

      const oe = (e as any).originalEvent as MouseEvent | undefined;
      const target = (oe?.target as HTMLElement) ?? null;

      // Ignore clicks on interactive features (routes, markers, tooltips/popups)
      if (isInteractiveTarget(target)) return;

      // Background click:
      if (hasRouteSelection) {
        // First click on background clears the selection
        onDeselect();
        return;
      }

      // No active selection → show the add/misc menu (only if zoomed in enough)
      if (map.getZoom() >= MIN_ADD_ZOOM) {
        const p = map.latLngToContainerPoint(e.latlng);
        onShow({ x: p.x, y: p.y, lat: e.latlng.lat, lng: e.latlng.lng });
      }
    },
  });

  return null;
}

function AddStopOnMapClick({
  onAdd,
  onTooFar,
}: {
  onAdd: (lat: number, lng: number) => void;
  onTooFar: () => void;
}) {
  const map = useMap();
  useMapEvents({
    click(e) {
      const oe = (e as any).originalEvent as MouseEvent | undefined;

      // If a marker/interactive element handled this, bail.
      if (oe) {
        // Leaflet sets a private _stopped flag when L.DomEvent.stop(...) was called
        // (used by Marker/Circle/Polyline handlers).
        if ((oe as any)._stopped || oe.defaultPrevented) return;

        const target = oe.target as HTMLElement | null;
        if (
          target &&
          (target.closest(".leaflet-marker-pane") ||  // marker icons
           target.closest(".leaflet-popup")      ||   // popups
           target.closest(".leaflet-tooltip")    ||   // tooltips
           target.closest(".leaflet-interactive"))    // vectors like CircleMarker/Polyline
        ) {
          return; // don’t add a stop when clicking an existing feature
        }
      }

      const z = map.getZoom();
      if (z >= MIN_ADD_ZOOM) onAdd(e.latlng.lat, e.latlng.lng);
      else onTooFar();
    },
  });
  return null;
}

/** ---------- App ---------- */
export default function App() {
  const [project, setProject] = useState<any>({
    extras: { restrictions: {}, stopDefaults: {} as StopDefaultsMap, shapeByRoute: {} as ShapeByRoute }
  });

  // ⬇️ NEW: prevents heavy recomputation while importing large files
  const suppressCompute = useRef(false);

  const handleRestrictionsChange = useCallback((map: Record<string, any>) => {
    setProject((prev: any) => ({
      ...(prev ?? {}),
      extras: { ...(prev?.extras ?? {}), restrictions: map },
    }));
  }, []);


  // Apply a pickup/dropoff/custom rule to only the trips in one section (Summary block)
  const applySectionRule = useCallback(
    (args: {
      tripIds: string[];      // the trips in the Summary section being edited
      stopId: string;         // the stop you're editing inside that section
      rule: ODRestriction | null; // the new rule for those trips (or null to clear)
    }) => {
      const { tripIds, stopId, rule } = args;

      setProject((prev: any) => {
        const curr: RestrictionsMap = { ...(prev?.extras?.restrictions ?? {}) };

        for (const tid of tripIds) {
          const k = `${tid}::${stopId}`;
          if (rule) curr[k] = normalizeRule(rule);
          else delete curr[k];
        }

        return {
          ...(prev ?? {}),
          extras: { ...(prev?.extras ?? {}), restrictions: curr },
        };
      });
    },
    []
  );



  const handleStopDefaultChange = useCallback((
    route_id: string,
    direction_id: 0 | 1 | undefined,
    stop_id: string,
    nextRule: ODRestriction | null
  ) => {
    setProject((prev: any) => {
      const curr: StopDefaultsMap = prev?.extras?.stopDefaults ?? {};
      const k: ScopedStopKey = stopDefaultKey(route_id ?? "", direction_id, stop_id);
      const next: StopDefaultsMap = { ...curr };
      if (nextRule) next[k] = nextRule;
      else delete next[k];
      return { ...(prev ?? {}), extras: { ...(prev?.extras ?? {}), stopDefaults: next } };
    });
  }, []);

  const getStopDefault = useCallback((
    route_id: string | undefined,
    direction_id: 0 | 1 | undefined,
    stop_id: string
  ): ODRestriction | undefined => {
    const map = (project?.extras?.stopDefaults ?? {}) as StopDefaultsMap;
    if (!stop_id) return undefined;

    // 1) exact (route + dir + stop)
    const exact = map[stopDefaultKey(route_id ?? "", direction_id, stop_id)];
    if (exact) return exact;

    // 2) route + any-direction
    const anyDir = map[stopDefaultKey(route_id ?? "", undefined, stop_id)];
    if (anyDir) return anyDir;

    // 3) global (no route, any-direction)
    return map[stopDefaultKey("", undefined, stop_id)];
  }, [project?.extras?.stopDefaults]);


  useEffect(() => {
    const onErr = (ev: ErrorEvent) => {
      console.error("GLOBAL ERROR:", ev.error ?? ev.message);
    };
    const onRej = (ev: PromiseRejectionEvent) => {
      console.error("UNHANDLED REJECTION:", ev.reason);
    };
    window.addEventListener("error", onErr);
    window.addEventListener("unhandledrejection", onRej);
    return () => {
      window.removeEventListener("error", onErr);
      window.removeEventListener("unhandledrejection", onRej);
    };
  }, []);

  /** Leaflet icons */
  useEffect(() => {
    // @ts-ignore
    delete L.Icon.Default.prototype._getIconUrl;
    L.Icon.Default.mergeOptions({
      iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
      iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
      shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
    });
  }, []);

  /** Data */
  const [agencies, setAgencies] = useState<Agency[]>([{
    agency_id: "agency_1",
    agency_name: "My Agency",
    agency_url: "https://example.com",
    agency_timezone: defaultTZ,
  }]);
  const [stops, setStops] = useState<Stop[]>([]);
  const [routes, setRoutes] = useState<RouteRow[]>([]);
  const [services, setServices] = useState<Service[]>([{
    service_id: "WKDY",
    monday: 1, tuesday: 1, wednesday: 1, thursday: 1, friday: 1, saturday: 0, sunday: 0,
    start_date: toYYYYMMDD(new Date()),
    end_date: toYYYYMMDD(new Date(new Date().setFullYear(new Date().getFullYear() + 1))),
  }]);
  const [trips, setTrips] = useState<Trip[]>([]);
  const [stopTimes, setStopTimes] = useState<StopTime[]>([]);
  const [shapePts, setShapePts] = useState<ShapePt[]>([]);
  const stopTimesAllRef = useRef<StopTime[] | null>(null);
  const stopsById = useMemo(() => {
    const m = new Map<string, Stop>();
    for (const s of stops) m.set(s.stop_id, s);
    return m;
  }, [stops]);
  const shapesById = useMemo(() => {
    const m = new Map<string, ShapePt[]>();
    for (const p of shapePts) {
      const arr = m.get(p.shape_id) ?? [];
      arr.push(p);
      m.set(p.shape_id, arr);
    }
    for (const [k, arr] of m) m.set(k, arr.slice().sort((a,b)=>a.seq-b.seq));
    return m;
  }, [shapePts]);

  // ---- Route geometry cache (per-route, lazy) ----
  const routeGeomCacheRef = useRef<Map<string, RouteGeom>>(new Map());

  const coordsForRoute = useCallback((routeId: string): LatLng[] | null => {
    // 1) try shapes first (longest shape across the route's trips)
    const rTrips = trips.filter(t => t.route_id === routeId);
    if (!rTrips.length) return null; // nothing to build
    let best: LatLng[] | null = null;
    for (const t of rTrips) {
      if (!t.shape_id) continue;
      const pts = shapesById.get(t.shape_id);
      if (!pts || pts.length < 2) continue;
      const c = pts.map(p => [p.lat, p.lon] as LatLng);
      if (!best || c.length > best.length) best = c;
    }
    if (best && best.length >= 2) return best;

    // 2) fallback from stop_times — prefer the FULL feed if available
    const rowsSource = (stopTimesAllRef.current?.length ? stopTimesAllRef.current! : stopTimes);
    const rowsByTrip = groupStopTimesByTrip(rowsSource);
    const routeTrips = trips.filter(t => t.route_id === routeId);

    let fallback: LatLng[] | null = null;
    for (const t of routeTrips) {
      const rows = (rowsByTrip.get(t.trip_id) ?? [])
        .slice()
        .sort((a,b)=>num(a.stop_sequence)-num(b.stop_sequence));
      if (rows.length < 2) continue;
      const c: LatLng[] = [];
      for (const r of rows) {
        const s = stopsById.get(r.stop_id);
        if (s && Number.isFinite(s.stop_lat) && Number.isFinite(s.stop_lon)) c.push([s.stop_lat, s.stop_lon]);
      }
      if (c.length >= 2 && (!fallback || c.length > fallback.length)) fallback = c;
    }
    return fallback;
  }, [trips, shapesById, stopsById, stopTimes]);

  // --- Soft throttling for huge feeds ---
  let _geomBudget = 400; // number of routes allowed to compute per frame

  const getRouteGeom = useCallback((routeId: string): RouteGeom | null => {
    const cache = routeGeomCacheRef.current;
    const cached = cache.get(routeId);
    if (cached) return cached;

    // Soft throttle: if we’ve computed a lot this tick, defer
    if (_geomBudget <= 0) return null;
    _geomBudget--;

    const coords = coordsForRoute(routeId);
    if (!coords || coords.length < 2) return null;

    const slim = decimate(coords, MAX_ROUTE_POINTS);
    const geom: RouteGeom = { coords: slim, bbox: bboxOf(slim) };
    cache.set(routeId, geom);
    return geom;
  }, [coordsForRoute]);

  // Reset budget every tick (0ms interval → each JS task)
  useEffect(() => {
    const id = setInterval(() => { _geomBudget = 400; }, 0);
    return () => clearInterval(id);
  }, []);

  // targeted invalidations
  useEffect(() => {
    // shapes changed → most likely geometry changed; clear cache
    routeGeomCacheRef.current.clear();
  }, [shapePts]);

  useEffect(() => {
    // stops or trips changed materially → clear cache
    routeGeomCacheRef.current.clear();
  }, [stops, trips]);


  /** UI */
  const [showRoutes, setShowRoutes] = useState<boolean>(true);

  const [showStops, setShowStops] = useState<boolean>(true);

  // scope tables/map to the active route(s)
  const [isScopedView, setIsScopedView] = useState<boolean>(true);
  
  const [mapClickMenu, setMapClickMenu] = useState<{
    x: number; y: number; lat: number; lng: number;
  } | null>(null);

  const addStopAt = useCallback((lat: number, lng: number) => {
    const base = "Stop";
  setStops(prev => {
    const numStr = (prev.length + 1).toString().padStart(3, "0");
    const newStop: Stop = {
      uid: uuidv4(),
      stop_id: `S_${numStr}`,
      stop_name: `${base} ${numStr}`,
      stop_lat: lat,
      stop_lon: lng,
    };
    // ok to set another state here
    setSelectedStopId(newStop.stop_id);
    return [...prev, newStop];
  });
  }, []);



  // --- Export options (UI) ---
  const [exportOnlySelectedRoutes, setExportOnlySelectedRoutes] = useState<boolean>(false);

  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null);
  const [selectedRouteIds, setSelectedRouteIds] = useState<Set<string>>(new Set()); // NEW multi-select
  // Robustly pick an "active" route if selection looks empty
  function resolveActiveRouteId(): string | null {
    if (selectedRouteId) return selectedRouteId;
    if (selectedRouteIds.size === 1) return Array.from(selectedRouteIds)[0];
    if (routes.length === 1) return routes[0].route_id;
    return null;
  }
  // --- Lazy load stop_times only for the selected route ---
    useEffect(() => {
      // don’useEffect(() => {
    if (suppressCompute.current) return;

    const rid = resolveActiveRouteId();
    const all = stopTimesAllRef.current;

    if (!rid || !all?.length) {
      if (stopTimes.length) setStopTimes([]);
      return;
    }

    const tids = new Set(trips.filter(t => t.route_id === rid).map(t => t.trip_id));
    if (!tids.size) {
      if (stopTimes.length) setStopTimes([]);
      return;
    }

    const run = () => {
      const filtered = all.filter(st => tids.has(st.trip_id));
      startTransition(() => setStopTimes(filtered));
    };

    if ("requestIdleCallback" in window) {
      // @ts-ignore
      (window as any).requestIdleCallback(run, { timeout: 120 });
    } else {
      setTimeout(run, 0);
    }
  }, [selectedRouteId, selectedRouteIds, trips]);


  const [selectedStopId, setSelectedStopId] = useState<string | null>(null);

  const [selectedStopTime, setSelectedStopTime] =
    useState<{ trip_id: string; stop_sequence: number } | null>(null);

  // If a stop_time row is selected, recover its stop_id so we can force-render that stop
  const selectedStopIdFromRow = useMemo(() => {
    if (!selectedStopTime) return null;
    const row = stopTimes.find(
      st =>
        st.trip_id === selectedStopTime.trip_id &&
        Number(st.stop_sequence) === Number(selectedStopTime.stop_sequence)
    );
    return row?.stop_id ?? null;
  }, [selectedStopTime, stopTimes]);

    // --- DnD state for stop_times ---
  const [dragInfo, setDragInfo] = useState<{ trip_id: string; from: number } | null>(null);

  // --- Renumber helper: for each trip_id set stop_sequence = 1..N in order ---
  function renumberAllStopSequences(rows: StopTime[]): StopTime[] {
    const byTrip = new Map<string, StopTime[]>();
    for (const r of rows) {
      const a = byTrip.get(r.trip_id) ?? [];
      a.push({ ...r });
      byTrip.set(r.trip_id, a);
    }
    const out: StopTime[] = [];
    for (const [trip_id, arr] of byTrip) {
      arr.sort((a,b) => num(a.stop_sequence) - num(b.stop_sequence));
      arr.forEach((r, i) => { r.stop_sequence = i + 1; });
      out.push(...arr);
    }
    return out;
  }

  // --- Keep stop_sequence tidy after ANY change to stopTimes ---
    // --- Detect time ordering errors per trip (arrival/departure must be non-decreasing) ---
  function computeTimeErrorKeys(rows: StopTime[]): Set<string> {
    const bad = new Set<string>();
    const byTrip = new Map<string, StopTime[]>();

    // ✅ robust grouping (no chained set/get)
    for (const r of rows) {
      if (!byTrip.has(r.trip_id)) byTrip.set(r.trip_id, []);
      byTrip.get(r.trip_id)!.push(r);
    }

    const toSec = (s?: string | null) => timeToSeconds((s ?? "").trim());

    for (const [trip_id, arr] of byTrip) {
      const sorted = arr.slice().sort((a, b) => num(a.stop_sequence) - num(b.stop_sequence));
      let prev: number | null = null;

      for (const r of sorted) {
        const key = `${trip_id}::${r.stop_sequence}`;
        const a = toSec(r.arrival_time);
        const d = toSec(r.departure_time);

        // Row-level: arrival must not be after departure
        if (a != null && d != null && a > d) bad.add(key);

        // Effective start/end for monotonic checks
        const start = a ?? d;
        const end   = d ?? a;

        if (prev != null) {
          if (start != null && start < prev) bad.add(key);
          if (end   != null && end   < prev) bad.add(key);
        }

        if (end != null) prev = end;
        else if (start != null) prev = start;
      }
    }
    return bad;
  }
  

  // Memoized set of keys "trip_id::stop_sequence" that are bad
  const badTimeKeys = useMemo(() => {
    // ⬇️ Skip during bulk imports or heavy edits
    if (suppressCompute.current) return new Set<string>();

    try {
      return computeTimeErrorKeys(stopTimes);
    } catch (e) {
      console.error("computeTimeErrorKeys crashed:", e);
      return new Set<string>();
    }
  }, [stopTimes]);

  useEffect(() => {
    // ⬇️ Skip auto-renumber during imports
    if (suppressCompute.current) return;

    if (!stopTimes.length) return;
    const ren = renumberAllStopSequences(stopTimes);

    let changed = false;
    for (let i = 0; i < stopTimes.length; i++) {
      const a = stopTimes[i], b = ren[i];
      if (
        a.trip_id !== b.trip_id ||
        a.stop_id !== b.stop_id ||
        a.stop_sequence !== b.stop_sequence ||
        a.arrival_time !== b.arrival_time ||
        a.departure_time !== b.departure_time ||
        a.pickup_type !== b.pickup_type ||
        a.drop_off_type !== b.drop_off_type
      ) {
        changed = true;
        break;
      }
    }
    if (changed) setStopTimes(ren);
  }, [stopTimes.length]);

  // --- New UI state for adding stop_times ---
  const [showAddExisting, setShowAddExisting] = useState<{
    open: boolean;
    trip_id?: string;
    afterSeq: number | null;
  }>({ open: false, trip_id: undefined, afterSeq: null });

  const [existingStopToAdd, setExistingStopToAdd] = useState<string>("");

  // Map state for culling heavy layers
  const [mapZoom, setMapZoom] = useState(6);
  const [mapBounds, setMapBounds] = useState<L.LatLngBounds | null>(null);



  const visibleRouteIds = useMemo(() => {
    if (!mapBounds || mapZoom < MIN_ROUTE_ZOOM || !showRoutes) return [];

    // If there are LOTS of routes and nothing is selected/scoped,
    // don't compute geometry at all — avoid freezing on huge feeds.
    const hasSelection =
      !!selectedRouteId || (selectedRouteIds && selectedRouteIds.size > 0);

    if (routes.length > 1200 && !hasSelection) {
      return []; // draw nothing until user selects a route
    }

    // fast path: if scoping and we have a selection, just draw the selection
    if (isScopedView) {
      const selected = new Set<string>(selectedRouteIds);
      if (selectedRouteId) selected.add(selectedRouteId);
      if (selected.size) return Array.from(selected);
    }

    const padded = mapBounds.pad(0.10);
    const out: string[] = [];
    for (const r of routes) {
      const g = getRouteGeom(r.route_id);
      if (g && g.bbox.intersects(padded)) out.push(r.route_id);
    }
    return out;
  }, [
    mapBounds, mapZoom, showRoutes,
    isScopedView, selectedRouteId, selectedRouteIds,
    routes, getRouteGeom
  ]);

  // Stable callback so MapStateTracker's effect doesn't re-run forever
  // Stable callback so MapStateTracker's effect doesn't re-run forever
  const [mapCenter, setMapCenter] = useState<L.LatLng | null>(null);

  // keep a live Leaflet map ref + one-shot guard for first fit
  const mapRef = useRef<L.Map | null>(null);
  const didInitialFitRef = useRef(false);

  const onMapState = useCallback((z: number, b: L.LatLngBounds, c: L.LatLng) => {
    setMapZoom(prev => (prev === z ? prev : z));
    setMapBounds(prev => (prev && prev.equals(b) ? prev : b));
    setMapCenter(prev => (prev && prev.equals(c) ? prev : c));
  }, []);

  // ---- Auto-rebuild shape debounce (USED WHEN MOVING A STOP) ----
  const rebuildTimer = useRef<number | null>(null);
  const queueRebuildShapeForSelectedRoute = useCallback(() => {
    if (!selectedRouteId) return;
    if (rebuildTimer.current) window.clearTimeout(rebuildTimer.current);
    rebuildTimer.current = window.setTimeout(() => {
      buildShapeAutoForSelectedRoute();
    }, 350) as unknown as number;
  }, [selectedRouteId]);


  // --- Scoped keep sets (when a route is selected, hide unrelated data) ---
  const scopedKeep = useMemo(() => {
    // Scope is active if there’s any selected route (single or multi)
    const hasSelection = !!selectedRouteId || (selectedRouteIds && selectedRouteIds.size > 0);
    if (!hasSelection) return null;

    // Build the set of route_ids we’re scoping to
    const keepRouteIds = new Set<string>(selectedRouteIds ?? []);
    if (selectedRouteId) keepRouteIds.add(selectedRouteId);

    // If nothing explicitly selected, fall back to resolved active route
    const fallbackRid = resolveActiveRouteId();
    if (!keepRouteIds.size && fallbackRid) keepRouteIds.add(fallbackRid);

    if (!keepRouteIds.size) return null;

    // Collect dependent ids
    const keepTripIds = new Set<string>();
    const keepSvcIds = new Set<string>();
    const keepShapeIds = new Set<string>();

    for (const t of trips) {
      if (!keepRouteIds.has(t.route_id)) continue;
      keepTripIds.add(t.trip_id);
      if (t.service_id) keepSvcIds.add(t.service_id);
      if (t.shape_id) keepShapeIds.add(String(t.shape_id));
    }

    const keepStopIds = new Set<string>();
    for (const st of stopTimes) {
      if (keepTripIds.has(st.trip_id)) keepStopIds.add(st.stop_id);
    }

    return { keepRouteIds, keepTripIds, keepSvcIds, keepStopIds, keepShapeIds };
  }, [selectedRouteId, selectedRouteIds, trips, stopTimes]);


  // Stop-times, scoped to selected route(s) (falls back to all)
  const stopTimesScoped = useMemo(() => {
    if (!scopedKeep) return stopTimes;
    return stopTimes.filter(st => scopedKeep.keepTripIds.has(st.trip_id));
  }, [scopedKeep, stopTimes]);


  // Only draw stops that are in view and when zoomed in enough
  const visibleStops = useMemo(() => {
    if (!showStops) return [];
    if (!mapBounds || mapZoom < MIN_STOP_ZOOM) return [];

    const b = mapBounds;

    // Always include explicitly selected stop(s), even if they’re not yet linked to a trip in the selected route
    const extras = new Set<string>();
    if (selectedStopId) extras.add(selectedStopId);
    if (selectedStopIdFromRow) extras.add(selectedStopIdFromRow);

    const base = scopedKeep
      ? stops.filter(s => scopedKeep.keepStopIds.has(s.stop_id) || extras.has(s.stop_id))
      : stops;

    return base.filter(s => b.contains(L.latLng(s.stop_lat, s.stop_lon)));
  }, [showStops, mapBounds, mapZoom, stops, scopedKeep, selectedStopId, selectedStopIdFromRow]);




  /** Filters / clearing */
  const [activeServiceIds, setActiveServiceIds] = useState<Set<string>>(new Set());
  const [clearSignal, setClearSignal] = useState(0);

  /** Validation & banner */
  const [banner, setBanner] = useState<Banner>(null);
  // Busy/lock UI while long ops run
  const [isBusy, setIsBusy] = useState(false);
  const [busyLabel, setBusyLabel] = useState<string | null>(null);
  const withBusy = async (label: string, fn: () => Promise<void> | void) => {
    if (isBusy) return;           // ignore re-entrancy
    setIsBusy(true);
    setBusyLabel(label);
    try {
      await fn();
    } finally {
      setIsBusy(false);
      setBusyLabel(null);
    }
  };
  const saveTimer = useRef<number | undefined>(undefined);
  const suppressPersist = useRef(false);

  /** Persistence */
  const STORAGE_KEY = "gtfs_builder_state_v1";
  const [hydrated, setHydrated] = useState(false);


  // One-time seeding: if shapeByRoute is empty after loading, populate it from existing trips
  useEffect(() => {
    if (!hydrated) return;
    const current = project?.extras?.shapeByRoute ?? {};
    if (Object.keys(current).length === 0 && trips.length) {
      const seeded: ShapeByRoute = {};
      for (const t of trips) {
        if (t.shape_id && !seeded[t.route_id]) seeded[t.route_id] = t.shape_id;
      }
      if (Object.keys(seeded).length) {
        setProject((prev: any) => ({
          ...(prev ?? {}),
          extras: { ...(prev?.extras ?? {}), shapeByRoute: { ...current, ...seeded } },
        }));
        log("[GTFS] seeded shapeByRoute from trips", seeded);
      }
    }
  }, [hydrated, trips, project?.extras?.shapeByRoute]);

  // ---------- Stop_times insertion helpers ----------
  function pickTargetTripAndAfterSeq(): { targetTrip?: Trip; afterSeq: number } {
    let targetTrip: Trip | undefined;
    let insertAfterSeq: number | null = null;

    if (selectedStopTime) {
      targetTrip = trips.find(t => t.trip_id === selectedStopTime.trip_id);
      insertAfterSeq = selectedStopTime.stop_sequence;
    } else {
      targetTrip = selectedRouteId
        ? trips.find(t => t.route_id === selectedRouteId)
        : trips[0];
    }

    if (!targetTrip) return { targetTrip: undefined, afterSeq: 0 };

    const inTrip = stopTimes
      .filter(r => r.trip_id === targetTrip.trip_id)
      .slice()
      .sort((a,b) => a.stop_sequence - b.stop_sequence);

    const afterSeq = insertAfterSeq ?? (inTrip.length ? inTrip[inTrip.length - 1].stop_sequence : 0);
    return { targetTrip, afterSeq };
  }

  function insertStopTimeRow(trip_id: string, stop_id: string, afterSeq: number) {
    const inTrip = stopTimes
      .filter(r => r.trip_id === trip_id)
      .slice()
      .sort((a,b) => a.stop_sequence - b.stop_sequence);

    const baseRow = inTrip.find(r => r.stop_sequence === afterSeq) ||
                    (inTrip.length ? inTrip[inTrip.length - 1] : undefined);
    const baseDep = baseRow?.departure_time || "08:00:00";
    const [hh, mm] = baseDep.split(":");
    const hhN = Number(hh) || 8;
    const mmN = Number(mm) || 0;
    const plus5 = `${String(hhN).padStart(2,"0")}:${String((mmN + 5) % 60).padStart(2,"0")}:00`;
    const newSeq  = afterSeq + 1;

    setStopTimes(prev => {
      const next = prev.map(r => ({ ...r }));
      for (const r of next) {
        if (r.trip_id === trip_id && r.stop_sequence >= newSeq) {
          r.stop_sequence = Number(r.stop_sequence) + 1;
        }
      }
      next.push({
        trip_id,
        stop_id,
        stop_sequence: newSeq,
        arrival_time: baseDep,
        departure_time: plus5,
        pickup_type: 0,
        drop_off_type: 0,
      });
      return next;
    });

    setSelectedStopTime({ trip_id, stop_sequence: newSeq });
  }


  // Inserts the selected stop into a trip, right after the currently selected row
  // in that trip (if any), otherwise at the end.
  const confirmAddExistingStop = (tripId?: string) => {
    // 1) Pick target trip: explicit arg → selected row’s trip → first trip
    const targetTripId =
      tripId ?? selectedStopTime?.trip_id ?? trips[0]?.trip_id;

    if (!targetTripId) {
      setBanner({ kind: "info", text: "Create a trip first." });
      setTimeout(() => setBanner(null), 1500);
      return;
    }

    // 2) Resolve the stop the user chose (ID or name)
    let sid = (existingStopToAdd || "").trim();
    if (!sid) {
      setBanner({ kind: "error", text: "Pick a stop from the list." });
      setTimeout(() => setBanner(null), 1500);
      return;
    }
    const maybeByName = stopNameToId.get(sid);
    if (maybeByName) sid = maybeByName;

    if (!stopsById.has(sid)) {
      setBanner({ kind: "error", text: "Unknown stop. Choose one from the list." });
      setTimeout(() => setBanner(null), 1800);
      return;
    }

    // 3) Decide insertion point: after the selected row in that trip, else append
    const inTrip = stopTimes
      .filter(r => r.trip_id === targetTripId)
      .slice()
      .sort((a,b) => a.stop_sequence - b.stop_sequence);

    const afterSeq =
      selectedStopTime?.trip_id === targetTripId
        ? Number(selectedStopTime.stop_sequence)
        : (inTrip.length ? inTrip[inTrip.length - 1].stop_sequence : 0);

    // 4) Insert and finish
    insertStopTimeRow(targetTripId, sid, afterSeq);
    setExistingStopToAdd("");
    setBanner({ kind: "success", text: "Stop inserted." });
    setTimeout(() => setBanner(null), 1200);
  };

  const addBlankStopTimeRow = () => {
    // Prefer the trip chosen in the dropdown, else fall back to existing logic
    const preferredTripId = activeTripIdForTimes ?? undefined;

    const { targetTrip, afterSeq } = ((): { targetTrip?: Trip; afterSeq: number } => {
      if (preferredTripId) {
        const t = trips.find(tt => tt.trip_id === preferredTripId);
        if (t) {
          const inTrip = stopTimes
            .filter(r => r.trip_id === t.trip_id)
            .slice()
            .sort((a,b) => a.stop_sequence - b.stop_sequence);
          return { targetTrip: t, afterSeq: inTrip.length ? inTrip[inTrip.length - 1].stop_sequence : 0 };
        }
      }
      // fallback to your existing picker
      return pickTargetTripAndAfterSeq();
    })();

    if (!targetTrip) {
      setBanner({ kind: "info", text: "Create a trip first." });
      setTimeout(() => setBanner(null), 1500);
      return;
    }

    const newSeq = afterSeq + 1;

    setStopTimes(prev => {
      const next = prev.map(r => ({ ...r }));
      for (const r of next) {
        if (r.trip_id === targetTrip.trip_id && r.stop_sequence >= newSeq) {
          r.stop_sequence = Number(r.stop_sequence) + 1;
        }
      }
      next.push({
        trip_id: targetTrip.trip_id,
        stop_id: "",
        stop_sequence: newSeq,
        arrival_time: "",
        departure_time: "",
        pickup_type: 0,
        drop_off_type: 0,
      });
      return next;
    });

    setSelectedStopTime({ trip_id: targetTrip.trip_id, stop_sequence: newSeq });
    setBanner({ kind: "success", text: `Blank row inserted at seq ${newSeq}.` });
    setTimeout(() => setBanner(null), 1200);
  };

  

  // Re-uses your nextId helper:
  // --- Reorder rows within the same trip (drag & drop) ---
  function moveRowWithinTrip(trip_id: string, fromIdx: number, toIdx: number) {
    // Build per-trip list (sorted by current sequence)
    const inTrip = stopTimes
      .filter(r => r.trip_id === trip_id)
      .sort((a,b) => num(a.stop_sequence) - num(b.stop_sequence));

    if (fromIdx < 0 || fromIdx >= inTrip.length || toIdx < 0 || toIdx >= inTrip.length) return;

    const item = inTrip.splice(fromIdx, 1)[0];
    inTrip.splice(toIdx, 0, item);

    // Recombine with all other trips
    const others = stopTimes.filter(r => r.trip_id !== trip_id);
    const reseq = inTrip.map((r, i) => ({ ...r, stop_sequence: i + 1 }));
    setStopTimes([...others, ...reseq]);
  }
  // --- GTFS pickup/dropoff select helper ---
  const STOP_TYPE_OPTIONS = [
    { value: 0, label: "0 — Regularly scheduled" },
    { value: 1, label: "1 — No pickup/drop-off" },
    { value: 2, label: "2 — Must phone agency" },
    { value: 3, label: "3 — Must coordinate with driver" },
  ];

  function StopTypeSelect({
    value,
    onChange,
    title,
  }: {
    value: number | undefined;
    onChange: (next: number) => void;
    title?: string;
  }) {
    return (
      <select
        value={value ?? 0}
        onChange={(e) => onChange(Number(e.target.value))}
        title={title}
        style={{
          fontSize: 12,
          border: "1px solid #e8e8e8",
          borderRadius: 8,
          padding: "4px 6px",
          width: 130,
        }}
      >
        {STOP_TYPE_OPTIONS.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    );
  }
const addAgencyRow = () => {
  const id = `agency_${agencies.length + 1}`;
  const row: Agency = {
    agency_id: id,
    agency_name: `Agency ${agencies.length + 1}`,
    agency_url: "https://example.com",
    agency_timezone: defaultTZ,
  };
  setAgencies(prev => [...prev, row]);
  agenciesRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
};

const addStopRow = () => {
  const base = "Stop";
  const numStr = (stops.length + 1).toString().padStart(3, "0");
  const row: Stop = {
    uid: uuidv4(),
    stop_id: `S_${numStr}`,
    stop_name: `${base} ${numStr}`,
    stop_lat: mapCenter?.lat ?? 40.4168,
    stop_lon: mapCenter?.lng ?? -3.7038,
  };
  setStops(prev => [...prev, row]);
  setSelectedStopId(row.stop_id);
  stopsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
};

const addServiceRow = () => {
  const id = `SVC_${services.length + 1}`;
  const today = toYYYYMMDD(new Date());
  const nextYear = toYYYYMMDD(new Date(new Date().setFullYear(new Date().getFullYear() + 1)));
  const row: Service = {
    service_id: id,
    monday: 1, tuesday: 1, wednesday: 1, thursday: 1, friday: 1, saturday: 0, sunday: 0,
    start_date: today, end_date: nextYear,
  };
  setServices(prev => [...prev, row]);
  calendarRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
};

const addTripRow = () => {
  // Prefer the currently selected route; otherwise first available
  const rid = selectedRouteId ?? routes[0]?.route_id;
  if (!rid) {
    setBanner({ kind: "info", text: "Create a route first to add trips." });
    setTimeout(() => setBanner(null), 1600);
    return;
  }
  const tid = nextId("T_", trips.map(t => t.trip_id));
  const row: Trip = {
    route_id: rid,
    service_id: services[0]?.service_id ?? "WKDY",
    trip_id: tid,
    trip_headsign: "",
    shape_id: "",
    direction_id: undefined,
  };
  setTrips(prev => [...prev, row]);
  setSelectedRouteId(rid);
  setSelectedRouteIds(new Set([rid]));
  tripsTableRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  setBanner({ kind: "success", text: `Trip ${tid} added to ${rid}.` });
  setTimeout(() => setBanner(null), 1200);
};

/** ---------- Build shapes from a trip’s stops ---------- */

// ROAD (car/bus) via OSRM — requires public OSRM server + CORS OK
async function buildShape_OSRM_forTrip(trip_id: string) {
  const rows = (stopTimesByTrip.get(trip_id) ?? []).slice().sort((a,b)=>a.stop_sequence-b.stop_sequence);
  if (rows.length < 2) {
    setBanner({ kind: "info", text: "Trip needs at least two stops." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }

  const coords: [number, number][] = [];
  for (const r of rows) {
    const s = stopsById.get(r.stop_id);
    if (s) coords.push([s.stop_lat, s.stop_lon]);
  }
  if (coords.length < 2) {
    setBanner({ kind: "info", text: "Stops lack coordinates." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }

  const waypoints = coords.map(([lat, lon]) => `${lon},${lat}`).join(";");
  const url = `https://router.project-osrm.org/route/v1/driving/${waypoints}?overview=full&geometries=polyline`;

  let geometry: [number, number][];
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`OSRM ${res.status}`);
    const json = await res.json();
    const route = json?.routes?.[0];
    if (!route?.geometry) throw new Error("No route found");
    geometry = decodePolyline(route.geometry);
  } catch (e) {
    console.error(e);
    setBanner({ kind: "error", text: "Routing failed (OSRM). This only works for roads and may be blocked by CORS." });
    setTimeout(() => setBanner(null), 2500);
    return;
  }

  const shape_id = nextId("shape_", Array.from(new Set(shapePts.map(p => p.shape_id))));
  const pts = geometry.map(([lat, lon], i) => ({ shape_id, lat, lon, seq: i + 1 }));

  setShapePts(prev => [...prev, ...pts]);
  setTrips(prev => prev.map(t => t.trip_id === trip_id ? { ...t, shape_id } : t));
  setBanner({ kind: "success", text: `Shape ${shape_id} created from road routing (OSRM).` });
  setTimeout(() => setBanner(null), 1800);
}

// RAIL — simple straight segments connecting stop order (no rail router available)
function buildShape_Rail_forTrip(trip_id: string) {
  const rows = (stopTimesByTrip.get(trip_id) ?? []).slice().sort((a,b)=>a.stop_sequence-b.stop_sequence);
  if (rows.length < 2) {
    setBanner({ kind: "info", text: "Trip needs at least two stops." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }
  const coords: [number, number][] = [];
  for (const r of rows) {
    const s = stopsById.get(r.stop_id);
    if (s) coords.push([s.stop_lat, s.stop_lon]);
  }
  if (coords.length < 2) {
    setBanner({ kind: "info", text: "Stops lack coordinates." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }

  const shape_id = nextId("shape_", Array.from(new Set(shapePts.map(p => p.shape_id))));
  const pts = coords.map(([lat, lon], i) => ({ shape_id, lat, lon, seq: i + 1 }));

  setShapePts(prev => [...prev, ...pts]);
  setTrips(prev => prev.map(t => t.trip_id === trip_id ? { ...t, shape_id } : t));
  setBanner({ kind: "success", text: `Shape ${shape_id} created (rail straight-line).` });
  setTimeout(() => setBanner(null), 1800);
}

// FERRY — straight segments across water (no public water router)
function buildShape_Ferry_forTrip(trip_id: string) {
  const rows = (stopTimesByTrip.get(trip_id) ?? []).slice().sort((a,b)=>a.stop_sequence-b.stop_sequence);
  if (rows.length < 2) {
    setBanner({ kind: "info", text: "Trip needs at least two stops." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }
  const coords: [number, number][] = [];
  for (const r of rows) {
    const s = stopsById.get(r.stop_id);
    if (s) coords.push([s.stop_lat, s.stop_lon]);
  }
  if (coords.length < 2) {
    setBanner({ kind: "info", text: "Stops lack coordinates." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }

  const shape_id = nextId("shape_", Array.from(new Set(shapePts.map(p => p.shape_id))));
  const pts = coords.map(([lat, lon], i) => ({ shape_id, lat, lon, seq: i + 1 }));

  setShapePts(prev => [...prev, ...pts]);
  setTrips(prev => prev.map(t => t.trip_id === trip_id ? { ...t, shape_id } : t));
  setBanner({ kind: "success", text: `Shape ${shape_id} created (ferry straight-line).` });
  setTimeout(() => setBanner(null), 1800);
}

/** ---------- Build driving shape using OSRM ---------- */
async function buildCarShapeForTrip_OSRM(trip_id: string) {
  const rows = (stopTimesByTrip.get(trip_id) ?? []).slice().sort((a,b)=>a.stop_sequence-b.stop_sequence);
  if (rows.length < 2) {
    setBanner({ kind: "info", text: "Trip needs at least two stops." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }

  const coords: [number, number][] = [];
  for (const r of rows) {
    const s = stopsById.get(r.stop_id);
    if (s) coords.push([s.stop_lat, s.stop_lon]);
  }
  if (coords.length < 2) {
    setBanner({ kind: "info", text: "Stops lack coordinates." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }

  // Build OSRM URL
  const waypoints = coords.map(([lat, lon]) => `${lon},${lat}`).join(";");
  const url = `https://router.project-osrm.org/route/v1/driving/${waypoints}?overview=full&geometries=polyline`;

  let geometry: [number, number][];
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`OSRM ${res.status}`);
    const json = await res.json();
    const route = json?.routes?.[0];
    if (!route?.geometry) throw new Error("No route found");
    geometry = decodePolyline(route.geometry);
  } catch (e) {
    console.error(e);
    setBanner({ kind: "error", text: "Routing failed (OSRM). Check CORS/network." });
    setTimeout(() => setBanner(null), 2500);
    return;
  }

  const shape_id = nextId("shape_", Array.from(new Set(shapePts.map(p => p.shape_id))));
  const pts = geometry.map(([lat, lon], i) => ({ shape_id, lat, lon, seq: i + 1 }));

  setShapePts(prev => [...prev, ...pts]);
  setTrips(prev => prev.map(t => t.trip_id === trip_id ? { ...t, shape_id } : t));

  setBanner({ kind: "success", text: `Shape ${shape_id} created from driving route.` });
  setTimeout(() => setBanner(null), 1800);
}

const addStopTimeRow = () => {
  // Decide target trip & insertion point
  let targetTrip: Trip | undefined;
  let insertAfterSeq: number | null = null;

  if (selectedStopTime) {
    targetTrip = trips.find(t => t.trip_id === selectedStopTime.trip_id);
    insertAfterSeq = selectedStopTime.stop_sequence;
  } else {
    targetTrip = selectedRouteId
      ? trips.find(t => t.route_id === selectedRouteId)
      : trips[0];
  }

  if (!targetTrip) {
    setBanner({ kind: "info", text: "Create a trip first." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }
  if (!stops.length) {
    setBanner({ kind: "info", text: "You need at least one stop." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }

  const inTrip = stopTimes
    .filter(r => r.trip_id === targetTrip!.trip_id)
    .slice()
    .sort((a,b) => a.stop_sequence - b.stop_sequence);

  const afterSeq = insertAfterSeq ?? (inTrip.length ? inTrip[inTrip.length - 1].stop_sequence : 0);
  const newSeq  = afterSeq + 1;

  // Prefer an unused stop in this trip
  const used = new Set(inTrip.map(r => r.stop_id));
  const firstUnused = stops.find(s => !used.has(s.stop_id)) ?? stops[0];

  // Time defaults (+5 minutes from the "after" row)
  const baseRow = inTrip.find(r => r.stop_sequence === afterSeq) ||
                  (inTrip.length ? inTrip[inTrip.length - 1] : undefined);
  const baseDep = baseRow?.departure_time || "08:00:00";
  const [hh, mm] = baseDep.split(":");
  const hhN = Number(hh) || 8;
  const mmN = Number(mm) || 0;
  const plus5 = `${String(hhN).padStart(2,"0")}:${String((mmN + 5) % 60).padStart(2,"0")}:00`;

  setStopTimes(prev => {
    const next = prev.map(r => ({ ...r }));
    // make room for the new sequence
    for (const r of next) {
      if (r.trip_id === targetTrip!.trip_id && r.stop_sequence >= newSeq) {
        r.stop_sequence = Number(r.stop_sequence) + 1;
      }
    }
    next.push({
      trip_id: targetTrip!.trip_id,
      stop_id: firstUnused.stop_id,
      stop_sequence: newSeq,
      arrival_time: baseDep,
      departure_time: plus5,
      pickup_type: 0,
      drop_off_type: 0,
    });
    return next;
  });

  setSelectedStopTime({ trip_id: targetTrip.trip_id, stop_sequence: newSeq });
  stopTimesRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  setBanner({ kind: "success", text: `Added stop_time after seq ${afterSeq || 0}.` });
  setTimeout(() => setBanner(null), 1200);
};

  useEffect(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const obj = JSON.parse(raw);

        setAgencies(obj.agencies ?? []);
        setStops((obj.stops ?? []).map((s: any) => ({ uid: s.uid || uuidv4(), ...s })));
        setRoutes(obj.routes ?? []);
        setServices(obj.services ?? []);
        setTrips(
          (obj.trips ?? []).map((t: any) => ({
            ...t,
            shape_id: t.shape_id && String(t.shape_id).trim() !== "" ? String(t.shape_id) : undefined,
          }))
        );
        setStopTimes(obj.stopTimes ?? []);
        setShapePts((obj.shapePts ?? []).map((p: any) => ({
          shape_id: String(p.shape_id ?? ""),
          lat: Number(p.lat ?? p.shape_pt_lat ?? 0),
          lon: Number(p.lon ?? p.shape_pt_lon ?? 0),
          seq: Number(p.seq ?? p.shape_pt_sequence ?? 0),
        })));

        // ⬇️ restore OD rules (supports old/new shapes of the saved object)
        const savedRules =
          obj.project?.extras?.restrictions ??
          obj.extras?.restrictions ??
          obj.restrictions ?? {};

        const savedStopDefaults: StopDefaultsMap = normalizeSavedStopDefaults(
          obj.project?.extras?.stopDefaults ??
          obj.extras?.stopDefaults ??
          obj.stopDefaults ?? // older saves, just in case
          {}
        );

        // ✅ add this:
        const savedShapeByRoute: ShapeByRoute =
          obj.project?.extras?.shapeByRoute ??
          obj.extras?.shapeByRoute ??
          obj.shapeByRoute ??
          {};

        // keep: setProject(...), etc.
        setProject((prev: any) => ({
          ...(prev ?? {}),
          extras: {
            ...(prev?.extras ?? {}),
            restrictions: savedRules,
            stopDefaults: savedStopDefaults,
            shapeByRoute: savedShapeByRoute,
          },
        }));
      }
    } catch {
      /* ignore */
    }
    setHydrated(true);
  }, []);


  // Auto-fit to the current stops exactly once after data loads
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    if (didInitialFitRef.current) return;
    if (!stops || stops.length === 0) return;

    const pts = stops
      .map(s => L.latLng(s.stop_lat, s.stop_lon))
      .filter(p => Number.isFinite(p.lat) && Number.isFinite(p.lng));

    if (pts.length === 0) return;

    // prevent refitting on every small change; only first time after import/load
    didInitialFitRef.current = true;

    if (pts.length === 1) {
      map.setView(pts[0], 12, { animate: false });
    } else {
      const bounds = L.latLngBounds(pts);
      map.fitBounds(bounds, { padding: [24, 24], maxZoom: 12, animate: false });
    }
  }, [stops]);

  useEffect(() => {
    if (!hydrated || suppressPersist.current) return;

    // Clear any pending save
    if (saveTimer.current) window.clearTimeout(saveTimer.current);

    // Debounce ~600ms after the latest change
    saveTimer.current = window.setTimeout(() => {
      try {
        const snapshot = {
          agencies,
          stops,
          routes,
          services,
          trips,
          stopTimes,
          shapePts,
          project: {
            extras: {
              restrictions: project?.extras?.restrictions ?? {},
              stopDefaults: project?.extras?.stopDefaults ?? {},
              shapeByRoute: project?.extras?.shapeByRoute ?? {},
            }
          },
        };
        const json = JSON.stringify(snapshot);
        if (json.length >= 5_000_000) {
          console.warn("Skipping autosave: feed too large for localStorage");
          return;
        }
        localStorage.setItem(STORAGE_KEY, json);
      } catch (err) {
        console.warn("LocalStorage save failed:", err);
      }
    }, 600) as unknown as number;

    return () => {
      if (saveTimer.current) window.clearTimeout(saveTimer.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hydrated,
    agencies, stops, routes, services, trips, stopTimes, shapePts,
    project?.extras?.restrictions,
    project?.extras?.stopDefaults,
    project?.extras?.shapeByRoute]);

  /** Colors */
  function hashCode(str: string) { let h = 0; for (let i=0;i<str.length;i++) h = ((h<<5)-h) + str.charCodeAt(i) | 0; return h; }
  const PALETTE = ["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf"];
  const routeColor = (routeId: string) => PALETTE[Math.abs(hashCode(routeId)) % PALETTE.length];

  /** Add stop by clicking map */
  

  /** Project import/export (JSON) */
  const exportProjectJSON = useCallback(() => {
    try {
      const payload = {
        agencies,
        stops,
        routes,
        services,
        trips,
        stopTimes,
        shapePts,
        // include your extras so custom rules & mappings are saved
        project: {
          extras: {
            restrictions: project?.extras?.restrictions ?? {},
            stopDefaults: project?.extras?.stopDefaults ?? {},
            shapeByRoute: project?.extras?.shapeByRoute ?? {},
          },
        },
        // small version tag can help future migrations
        _meta: { kind: "gtfs_builder_project", version: 1 },
      };

      const json = JSON.stringify(payload, null, 2);
      const blob = new Blob([json], { type: "application/json;charset=utf-8" });

      // primary path
      if (typeof saveAs === "function") {
        saveAs(blob, "project.json");
        return;
      }

      // fallback (Safari / odd bundlers)
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "project.json";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Export project failed:", err);
      alert("Export failed. See console for details.");
    }
  }, [
    agencies,
    stops,
    routes,
    services,
    trips,
    stopTimes,
    shapePts,
    project?.extras?.restrictions,
    project?.extras?.stopDefaults,
    project?.extras?.shapeByRoute,
  ]);
  const importProject = async (file: File) =>
    withBusy("Loading project…", async () => {
      try {
        const obj = JSON.parse(await file.text());

        suppressPersist.current = true;

        didInitialFitRef.current = false; // allow auto-fit after loading a project

        suppressCompute.current = true; // ⬇️ NEW: pause all recomputations

        setAgencies(obj.agencies ?? []);
        setStops((obj.stops ?? []).map((s: any) => ({ uid: s.uid || uuidv4(), ...s })));
        setRoutes(obj.routes ?? []);
        setServices(obj.services ?? []);
        setTrips(obj.trips ?? []);
        setStopTimes(obj.stopTimes ?? []);
        setShapePts(obj.shapePts ?? []);

        // restore rules
        const savedRules =
          obj.project?.extras?.restrictions ??
          obj.extras?.restrictions ??
          obj.restrictions ?? {};
        setProject((prev: any) => ({
          ...(prev ?? {}),
          extras: {
            ...(prev?.extras ?? {}),
            restrictions: (
              obj.project?.extras?.restrictions ??
              obj.extras?.restrictions ??
              obj.restrictions ??
              {}
            ),
            stopDefaults: normalizeSavedStopDefaults(
              obj.project?.extras?.stopDefaults ??
              obj.extras?.stopDefaults ??
              obj.stopDefaults ??
              {}
            ),
            shapeByRoute: (
              obj.project?.extras?.shapeByRoute ??
              obj.extras?.shapeByRoute ??
              {}
            ),
          },
        }));

              setBanner({ kind: "success", text: "Project imported." });
              setTimeout(() => setBanner(null), 2200);

              // (keep) shapeByRoute remap
              const importedShapeByRoute: ShapeByRoute = (
                obj.project?.extras?.shapeByRoute ??
                obj.extras?.shapeByRoute ??
                obj.shapeByRoute ??
                {}
              ) as ShapeByRoute;

              queueMicrotask(() => {
                const map = (obj.project?.extras?.shapeByRoute ??
                            obj.extras?.shapeByRoute ??
                            obj.shapeByRoute ??
                            {}) as ShapeByRoute;

                setTrips(prev => {
                  let changed = false;
                  const next = prev.map(t => {
                    const sid = map[t.route_id];
                    if (sid && (!t.shape_id || String(t.shape_id).trim() === "")) {
                      changed = true;
                      return { ...t, shape_id: sid };
                    }
                    return t;
                  });
                  return changed ? next : prev;
                });
              });
            } catch {
              setBanner({ kind: "error", text: "Invalid project JSON." });
              setTimeout(() => setBanner(null), 3200);
            } finally {
              // ⬇️ restore both flags safely after React flushes
              queueMicrotask(() => {
                suppressCompute.current = false;
                suppressPersist.current = false;
              });
            }
    });

  const importOverrides = async (file: File) => {
    try {
      const json = JSON.parse(await file.text());

      // Accept both:
      //  1) { rules: { "trip::stop": {...} }, stopDefaults?: {...} }
      //  2) { restrictions: { ... }, stopDefaults?: {...} }
      //  3) Array forms for either field
      //  4) Raw { "tripId|stopId": {...} } maps (we’ll wrap as restrictions)

      // Normalize the raw object to the tolerant shape
      const raw: any = (() => {
        if (json?.rules && !json?.restrictions) {
          return { restrictions: json.rules, stopDefaults: json.stopDefaults };
        }
        if (json && (json.restrictions || json.stopDefaults)) return json;
        // fallback: treat the whole file as the restrictions map
        return { restrictions: json };
      })();

      // Use ALL stop_times if available (from the GTFS import), else the UI subset
      const sourceRows: StopTime[] =
        (stopTimesAllRef.current && stopTimesAllRef.current.length)
          ? stopTimesAllRef.current
          : stopTimes;

      // Parse + clamp to valid (trip,stop) pairs, accept multiple delimiters, arrays, etc.
      const { restrictions: importedR, stopDefaults: importedD } =
        importOverridesTolerant(raw, sourceRows);

      // Optional diagnostics (helps answer “why didn’t X apply?”)
      const audit = auditOverrides(importedR, sourceRows);
      console.log("[Overrides] audit:", audit);

      // Merge with existing (don’t wipe any rules already set/edited in UI or inferred from GTFS)
      setProject((prev: any) => ({
        ...(prev ?? {}),
        extras: {
          ...(prev?.extras ?? {}),
          restrictions: { ...(prev?.extras?.restrictions ?? {}), ...importedR },
          stopDefaults: { ...(prev?.extras?.stopDefaults ?? {}), ...importedD },
        },
      }));

      const total = Object.keys(importedR).length;
      setBanner({
        kind: total ? "success" : "info",
        text: total
          ? `Overrides: applied ${audit.ok}/${total}` +
            (audit.notOnTrip || audit.badTrip || audit.badStop
              ? ` (skipped ${audit.notOnTrip + audit.badTrip + audit.badStop})`
              : "")
          : "Overrides: no applicable entries found.",
      });
      setTimeout(() => setBanner(null), 3200);
    } catch (e) {
      console.error(e);
      setBanner({ kind: "error", text: "Invalid overrides.json" });
      setTimeout(() => setBanner(null), 3200);
    }
  };

  /** Import GTFS .zip */
  const importGTFSZip = async (file: File) =>
    withBusy("Parsing GTFS…", async () => {
      try {
        // Clear OD rules/defaults when importing a raw operator feed
        setProject((prev: any) => ({
          ...(prev ?? {}),
          extras: { restrictions: {}, stopDefaults: {}, shapeByRoute: {} },
        }));
        // During huge imports, don't render thousands of markers
        setShowStops(false);
        didInitialFitRef.current = false; // allow auto-fit after this import

        suppressPersist.current = true;
        suppressCompute.current = true;

        const zip = await JSZip.loadAsync(file);

        // Collect .txt contents, yielding between files
        const tables: Record<string, string> = {};
        const files = Object.values(zip.files)
          .filter((f: any) => !f.dir && f.name?.toLowerCase().endsWith(".txt"));

        for (const entry of files) {
          const name = entry.name.replace(/\.txt$/i, "");
          tables[name] = await (zip.files[entry.name] as any).async("string");
          await tick(); // let the browser breathe
        }

        rawCsvRef.current.stop_times = tables["stop_times"] ?? undefined;
        rawCsvRef.current.shapes     = tables["shapes"] ?? undefined;

        // Parse each CSV off the main thread
        const parse = <T = any>(name: string) =>
          tables[name] ? parseCsvFast<T>(tables[name]) : Promise.resolve([] as T[]);

        const [
          agenciesRaw,
          stopsRaw,
          routesRaw,
          servicesRaw,
          tripsRaw,
          stopTimesRaw,
          shapesRaw,
        ] = await Promise.all([
          parse<any>("agency"),
          parse<any>("stops"),
          parse<any>("routes"),
          parse<any>("calendar"),
          parse<any>("trips"),
          parse<any>("stop_times"),
          parse<any>("shapes"),
        ]);

        // Map/normalize once, then one render via batched updates
        unstable_batchedUpdates(() => {
          if (agenciesRaw.length) {
            setAgencies(
              agenciesRaw.map((r: any) => ({
                agency_id: String(r.agency_id ?? ""),
                agency_name: String(r.agency_name ?? ""),
                agency_url: String(r.agency_url ?? ""),
                agency_timezone: String(r.agency_timezone ?? defaultTZ),
              }))
            );
          }

          if (stopsRaw.length) {
            setStops(
              stopsRaw.map((r: any) => ({
                uid: uuidv4(),
                stop_id: String(r.stop_id ?? ""),
                stop_name: String(r.stop_name ?? ""),
                stop_lat: Number(r.stop_lat ?? r.stop_latitude ?? r.lat ?? 0),
                stop_lon: Number(r.stop_lon ?? r.stop_longitude ?? r.lon ?? 0),
              }))
            );
          }

          if (routesRaw.length) {
            setRoutes(
              routesRaw.map((r: any) => ({
                route_id: String(r.route_id ?? ""),
                route_short_name: String(r.route_short_name ?? ""),
                route_long_name: String(r.route_long_name ?? ""),
                route_type: Number(r.route_type ?? 3),
                agency_id: String(r.agency_id ?? ""),
              }))
            );
          }

          if (servicesRaw.length) {
            setServices(
              servicesRaw.map((r: any) => ({
                service_id: String(r.service_id ?? ""),
                monday: Number(r.monday ?? 0),
                tuesday: Number(r.tuesday ?? 0),
                wednesday: Number(r.wednesday ?? 0),
                thursday: Number(r.thursday ?? 0),
                friday: Number(r.friday ?? 0),
                saturday: Number(r.saturday ?? 0),
                sunday: Number(r.sunday ?? 0),
                start_date: String(r.start_date ?? ""),
                end_date: String(r.end_date ?? ""),
              }))
            );
          }

          if (tripsRaw.length) {
            setTrips(
              tripsRaw.map((r: any) => ({
                route_id: String(r.route_id ?? ""),
                service_id: String(r.service_id ?? ""),
                trip_id: String(r.trip_id ?? ""),
                trip_headsign: r.trip_headsign != null ? String(r.trip_headsign) : undefined,
                shape_id: r.shape_id != null && String(r.shape_id).trim() !== "" ? String(r.shape_id) : undefined,
                direction_id:
                  r.direction_id != null && r.direction_id !== "" ? Number(r.direction_id) : undefined,
              }))
            );
          }

          // IMPORTANT: lazy stop_times
          if (stopTimesRaw.length) {
            stopTimesAllRef.current = stopTimesRaw.map((r: any) => ({
              trip_id: String(r.trip_id ?? ""),
              arrival_time: String(r.arrival_time ?? ""),
              departure_time: String(r.departure_time ?? ""),
              stop_id: String(r.stop_id ?? ""),
              stop_sequence: Number(r.stop_sequence ?? 0),
              pickup_type: r.pickup_type != null && r.pickup_type !== "" ? Number(r.pickup_type) : undefined,
              drop_off_type: r.drop_off_type != null && r.drop_off_type !== "" ? Number(r.drop_off_type) : undefined,
            }));
            // keep UI lean until a route is selected
            setStopTimes([]);
          } else {
            stopTimesAllRef.current = null;
            setStopTimes([]);
          }

          (() => {
            const rows = stopTimesAllRef.current ?? [];
            if (!rows.length) return;

            const inferred: Record<string, ODRestriction> = {};
            for (const r of rows) {
              const pick = Number(r.pickup_type ?? 0);
              const drop = Number(r.drop_off_type ?? 0);

              // drop-off only
              if (pick === 1 && drop !== 1) {
                inferred[`${r.trip_id}::${r.stop_id}`] = { mode: "dropoff" };
                continue;
              }
              // pickup only
              if (drop === 1 && pick !== 1) {
                inferred[`${r.trip_id}::${r.stop_id}`] = { mode: "pickup" };
                continue;
              }
              // both 1 → “pass-through / timepoint only”: skip (no rule)
            }

            if (Object.keys(inferred).length) {
              setProject((prev: any) => ({
                ...(prev ?? {}),
                extras: {
                  ...(prev?.extras ?? {}),
                  // merge with whatever you reset at the start of import
                  restrictions: {
                    ...(prev?.extras?.restrictions ?? {}),
                    ...inferred,
                  },
                },
              }));
              log("[GTFS Import] inferred restrictions from pickup/dropoff flags:", Object.keys(inferred).length);
            }
          })();

          if (shapesRaw.length) {
            setShapePts(
              shapesRaw.map((r: any) => ({
                shape_id: String(r.shape_id ?? ""),
                lat: Number(r.shape_pt_lat ?? r.lat ?? 0),
                lon: Number(r.shape_pt_lon ?? r.lon ?? 0),
                seq: Number(r.shape_pt_sequence ?? r.seq ?? 0),
              }))
            );
          }
        });

        setBanner({ kind: "success", text: "GTFS zip imported. Select a route to load its stop_times." });
        setTimeout(() => setBanner(null), 2400);
      } catch (e) {
        console.error(e);
        setBanner({ kind: "error", text: "Failed to import GTFS zip." });
        setTimeout(() => setBanner(null), 3200);
      } finally {
        // Let React paint, then resume compute/persist
        queueMicrotask(() => {
          suppressCompute.current = false;
          suppressPersist.current = false;
        });
        // Note: we left "Visualize stops" OFF after import for performance.
        // The user can toggle it back on from the toolbar when ready.
      }
    });

  
  const stopNameToId = useMemo(() => {
    const m = new Map<string, string>();
    for (const s of stops) m.set(s.stop_name, s.stop_id);
    return m;
  }, [stops]);

  const stopIdToName = useCallback((id?: string) => {
    if (!id) return "";
    return stopsById.get(id)?.stop_name ?? "";
  }, [stopsById]);

  const normalizeStopIdAndName = (
    raw: any,
    prev?: { stop_id?: string }
  ) => {
    let stop_id = String(raw.stop_id ?? "");
    let stop_name = String(raw.stop_name ?? "");

    const prev_id = String(prev?.stop_id ?? "");
    const prev_name = prev_id ? (stopsById.get(prev_id)?.stop_name ?? "") : "";

    const idChanged   = !!stop_id && stop_id !== prev_id;
    const nameChanged = !!stop_name && stop_name !== prev_name;

    // If user edited ID only → trust ID, derive name from it.
    if (idChanged && !nameChanged) {
      return { stop_id, stop_name: stopsById.get(stop_id)?.stop_name ?? "" };
    }

    // If user edited name only → map to ID by name, keep the chosen name.
    if (nameChanged && !idChanged) {
      const fromName = stopNameToId.get(stop_name);
      if (fromName) return { stop_id: fromName, stop_name };
      // unknown name → leave as-is (no remap)
      return { stop_id, stop_name };
    }

    // If both changed, prefer ID (it’s unambiguous).
    if (idChanged && nameChanged) {
      return { stop_id, stop_name: stopsById.get(stop_id)?.stop_name ?? stop_name };
    }

    // Nothing changed or blanks → keep them consistent if possible.
    const nameFromId = stopsById.get(stop_id)?.stop_name;
    if (nameFromId) return { stop_id, stop_name: nameFromId };

    if (stop_name) {
      const fromName = stopNameToId.get(stop_name);
      if (fromName) return { stop_id: fromName, stop_name };
    }
    return { stop_id, stop_name };
  };

  

  const stopTimesByTrip = useMemo(() => {
    const m = new Map<string, StopTime[]>();
    for (const st of stopTimes) {
      const arr = m.get(st.trip_id) ?? [];
      arr.push(st);
      m.set(st.trip_id, arr);
    }
    for (const [k, arr] of m) m.set(k, arr.slice().sort((a,b)=>num(a.stop_sequence)-num(b.stop_sequence)));
    return m;
  }, [stopTimes]);



  // Fallback: build a polyline for a route from one representative trip's stop order
  const buildFallbackCoordsForRoute = useCallback(
    (routeId: string): [number, number][] | null => {
      // Prefer ALL stop_times from the imported feed; fall back to the UI subset
      const rowsSource: StopTime[] =
        (stopTimesAllRef.current && stopTimesAllRef.current.length)
          ? stopTimesAllRef.current
          : stopTimes;

      // Reuse your existing grouper
      const rowsByTrip = groupStopTimesByTrip(rowsSource);

      // Trips for this route
      const routeTrips = trips.filter(t => t.route_id === routeId);
      if (!routeTrips.length) return null;

      // Pick the trip with the most stop_times (gives a decent path)
      let bestCoords: [number, number][] | null = null;

      for (const t of routeTrips) {
        const rows = (rowsByTrip.get(t.trip_id) ?? [])
          .slice()
          .sort((a, b) => num(a.stop_sequence) - num(b.stop_sequence));

        if (rows.length < 2) continue;

        const coords: [number, number][] = [];
        for (const r of rows) {
          const s = stopsById.get(r.stop_id);
          if (s && Number.isFinite(s.stop_lat) && Number.isFinite(s.stop_lon)) {
            coords.push([s.stop_lat, s.stop_lon]);
          }
        }

        if (coords.length >= 2) {
          if (!bestCoords || coords.length > bestCoords.length) {
            bestCoords = coords;
          }
        }
      }

      return bestCoords;
    },
    [trips, stopsById, stopTimes]
  );

  
  




  const routesScoped = useMemo(
    () => (scopedKeep ? routes.filter(r => scopedKeep.keepRouteIds.has(r.route_id)) : routes),
    [routes, scopedKeep]
  );

  const tripsScoped = useMemo(
    () => (scopedKeep ? trips.filter(t => scopedKeep.keepTripIds.has(t.trip_id)) : trips),
    [trips, scopedKeep]
  );

  const stopsScoped = useMemo(
    () => (scopedKeep ? stops.filter(s => scopedKeep.keepStopIds.has(s.stop_id)) : stops),
    [stops, scopedKeep]
  );

  const servicesScoped = useMemo(
    () => (scopedKeep ? services.filter(s => scopedKeep.keepSvcIds.has(s.service_id)) : services),
    [services, scopedKeep]
  );

  const shapePtsScoped = useMemo(
    () => (scopedKeep ? shapePts.filter(p => scopedKeep.keepShapeIds.has(p.shape_id)) : shapePts),
    [shapePts, scopedKeep]
  );









  // Only list stop_times for the selected route (and active services, if any)
  const tripIdsForStopTimes = useMemo(() => {
    if (!selectedRouteId) return [];  // ← critical: don’t render anything unless a route is selected
    const pool = trips
      .filter(t => t.route_id === selectedRouteId)
      .filter(t => activeServiceIds.size === 0 || activeServiceIds.has(t.service_id));
    return pool.map(t => t.trip_id).sort();
  }, [trips, selectedRouteId, activeServiceIds]);

  // Single-block stop_times: which trip is shown
  const [activeTripIdForTimes, setActiveTripIdForTimes] = useState<string | null>(null);

  // Keep activeTripIdForTimes valid when available trip_ids change
  useEffect(() => {
    if (!tripIdsForStopTimes.length) {
      if (activeTripIdForTimes !== null) setActiveTripIdForTimes(null);
      return;
    }
    if (!activeTripIdForTimes || !tripIdsForStopTimes.includes(activeTripIdForTimes)) {
      setActiveTripIdForTimes(tripIdsForStopTimes[0]);
    }
  }, [tripIdsForStopTimes, activeTripIdForTimes]);

  const tripsByRoute = useMemo(() => {
    const m = new Map<string, Trip[]>();
    for (const t of trips) {
      const arr = m.get(t.route_id) ?? [];
      arr.push(t);
      m.set(t.route_id, arr);
    }
    return m;
  }, [trips]);


  

  

  /** ---------- Selection & ordering ---------- */
  // Visual: selected route first
  const routesVisibleIdx = useMemo(() => {
    // IMPORTANT: base must match what you pass as rows to the table
    const base = routesScoped;

    const idxs = base.map((_, i) => i);
    if (!selectedRouteId) return idxs;

    const selIdx = base.findIndex(r => r.route_id === selectedRouteId);
    if (selIdx < 0) return idxs;

    return [selIdx, ...idxs.filter(i => i !== selIdx)];
  }, [isScopedView, scopedKeep, routesScoped, routes, selectedRouteId]);

 useEffect(() => {
  const isTyping = () => {
    const el = document.activeElement as HTMLElement | null;
    if (!el) return false;
    const tag = el.tagName;
    const editable = el.getAttribute?.("contenteditable");
    return (
      tag === "INPUT" ||
      tag === "TEXTAREA" ||
      tag === "SELECT" ||
      editable === "true"
    );
  };

  const onKeyDown = (e: KeyboardEvent) => {
    if (isTyping()) return;

    if (selectedStopId) {
      if (e.key === "Escape") {
        e.preventDefault();
        setSelectedStopId(null);
        setMapClickMenu(null);
        return;
      }
      if (e.key === "Delete" || e.key === "Backspace") {
        e.preventDefault();
        deleteSelectedStop();
        setMapClickMenu(null);
        return;
      }
    } else {
      if (e.key === "Escape") {
        e.preventDefault();
        setMapClickMenu(null);
      }
    }
  };

  window.addEventListener("keydown", onKeyDown);
  return () => window.removeEventListener("keydown", onKeyDown);
}, [selectedStopId]);

  // If selectedRouteId no longer exists, clear it (and the multiselect)
  useEffect(() => {
    if (selectedRouteId && !routes.some(r => r.route_id === selectedRouteId)) {
      setSelectedRouteId(null);
      setSelectedRouteIds(new Set());
    }
  }, [routes, selectedRouteId]);

  // If selected stop no longer exists, clear it
  useEffect(() => {
    if (selectedStopId && !stops.some(s => s.stop_id === selectedStopId)) {
      setSelectedStopId(null);
    }
  }, [stops, selectedStopId]);

  // If service chips were set for a previously selected route, prune to what's still valid
  useEffect(() => {
    if (!selectedRouteId) {
      if (activeServiceIds.size) setActiveServiceIds(new Set());
      return;
    }
    const valid = new Set(
      trips.filter(t => t.route_id === selectedRouteId).map(t => t.service_id)
    );
    let changed = false;
    const next = new Set<string>();
    activeServiceIds.forEach(id => { if (valid.has(id)) next.add(id); else changed = true; });
    if (changed) setActiveServiceIds(next);
  }, [selectedRouteId, trips]); 

  
  async function ensureAllStopTimesForExport() {
    const all = stopTimesAllRef.current;
    if (!all) return; // nothing lazy-loaded
    if (stopTimes.length === all.length) return; // already hydrated
    setStopTimes(all.slice());
    await tick(); // allow React to flush setStopTimes before we read from maps
  }
  const onExportGTFS = async () =>
    withBusy("Preparing GTFS…", async () => {
      // Build the set of routes to export, if the toggle is on
      let onlyRoutes: Set<string> | undefined = undefined;
      if (exportOnlySelectedRoutes) {
        const set = new Set<string>(selectedRouteIds);
        if (!set.size && selectedRouteId) set.add(selectedRouteId);
        if (!set.size) {
          setBanner({ kind: "info", text: "No route selected — exporting all routes." });
          setTimeout(() => setBanner(null), 2200);
        } else {
          onlyRoutes = set;
        }
      }

      // ⬇️ TEMPORARILY PAUSE heavy recomputations while we export
      suppressCompute.current = true;
      try {
        // Use ALL stop_times if we lazily kept the UI list empty
        const rowsSource: StopTime[] =
          (stopTimesAllRef.current && stopTimesAllRef.current.length)
            ? stopTimesAllRef.current
            : stopTimes;

        const rowsByTripForExport = groupStopTimesByTrip(rowsSource);

        await exportGtfsCompiled(
          onlyRoutes,
          undefined,              // defaults kick in: round + decimate = true
          rowsByTripForExport
        );
      } finally {
        queueMicrotask(() => { suppressCompute.current = false; });
      }
  });

  const resetAll = () => {
    if (!confirm("Reset project?")) return;
    setAgencies([]); setStops([]); setRoutes([]); setServices([]); setTrips([]); setStopTimes([]); setShapePts([]);
    localStorage.removeItem(STORAGE_KEY);
    setSelectedRouteId(null);
    setSelectedRouteIds(new Set());
    setSelectedStopId(null);
    setActiveServiceIds(new Set());
    setClearSignal((x) => x + 1);
    setProject({ extras: { restrictions: {}, stopDefaults: {}, shapeByRoute: {} } });
  };

  /** Edit a time (updates stop_times), store HH:MM:00 or "" */
  const handleEditTime = (trip_id: string, stop_id: string, newTime: string) => {
    setStopTimes(prev => {
      const next = prev.map(r => ({ ...r }));

      if (!newTime || !newTime.trim()) {
        for (const r of next) {
          if (r.trip_id === trip_id && r.stop_id === stop_id) {
            r.departure_time = "";
            r.arrival_time = "";
            return next;
          }
        }
        return next;
      }

      let found = false;
      for (const r of next) {
        if (r.trip_id === trip_id && r.stop_id === stop_id) {
          r.departure_time = newTime;
          if (!r.arrival_time) r.arrival_time = newTime;
          found = true;
          break;
        }
      }
      if (!found) {
        const seqs = next.filter(r => r.trip_id === trip_id).map(r => num(r.stop_sequence, 0));
        const newSeq = (seqs.length ? Math.max(...seqs) : 0) + 1;
        next.push({
          trip_id,
          stop_id,
          departure_time: newTime,
          arrival_time: newTime,
          stop_sequence: Number(newSeq),   // ← force number
        });
      }
      return next;
    });
  };

  /** Delete a route and all dependent data (memory-safe & batched) */
  const hardDeleteRoute = (route_id: string) => {
    try {
      const doomedTrips = new Set(trips.filter(t => t.route_id === route_id).map(t => t.trip_id));
      const remainingTrips = trips.filter(t => t.route_id !== route_id);
      const keptShapeIds = new Set(remainingTrips.map(t => t.shape_id).filter(Boolean) as string[]);

      unstable_batchedUpdates(() => {
        setProject((prev: any) => {
          const curr: Record<string, any> = prev?.extras?.restrictions ?? {};
          const next: Record<string, any> = {};
          for (const [k, v] of Object.entries(curr)) {
            const [trip_id] = k.split("::");
            if (!doomedTrips.has(trip_id)) next[k] = v;
          }
          return { ...(prev ?? {}), extras: { ...(prev?.extras ?? {}), restrictions: next } };
        });

        setTrips(remainingTrips);
        setStopTimes(prev => prev.filter(st => !doomedTrips.has(st.trip_id)));
        setShapePts(prev => prev.filter(p => keptShapeIds.has(p.shape_id)));
        setRoutes(prev => prev.filter(r => r.route_id !== route_id));

        // selection cleanup with busy guard already active
        setSelectedRouteIds((prev: Set<string>) => { const n = new Set(prev); n.delete(route_id); return n; });
        if (selectedRouteId === route_id) setSelectedRouteId(null);
      });
    } catch (e) {
      console.error(e);
      setBanner({ kind: "error", text: "Delete failed. Try again." });
      setTimeout(() => setBanner(null), 3000);
    }
  };

    /** Bulk delete routes in ONE pass (prevents thrash & crashes) */
  const hardDeleteRoutesBulk = (routeIds: Set<string>) => {
    if (!routeIds.size) return;

    try {
      suppressPersist.current = true; // avoid autosave thrash while we compute

      const doomedRoutes = new Set(routeIds);
      const doomedTrips = new Set(
        trips.filter(t => doomedRoutes.has(t.route_id)).map(t => t.trip_id)
      );

      // Pre-compute “next” snapshots once
      const remainingTrips = trips.filter(t => !doomedRoutes.has(t.route_id));
      const keptShapeIds = new Set(
        remainingTrips.map(t => t.shape_id).filter(Boolean) as string[]
      );

      const nextRoutes = routes.filter(r => !doomedRoutes.has(r.route_id));
      const nextStopTimes = stopTimes.filter(st => !doomedTrips.has(st.trip_id));
      const nextShapePts = shapePts.filter(p => keptShapeIds.has(p.shape_id));

      // Prune OD restrictions for deleted trips
      const nextRestrictions = (() => {
        const curr: Record<string, any> = project?.extras?.restrictions ?? {};
        const out: Record<string, any> = {};
        for (const [k, v] of Object.entries(curr)) {
          const [trip_id] = k.split("::");
          if (!doomedTrips.has(trip_id)) out[k] = v;
        }
        return out;
      })();

      unstable_batchedUpdates(() => {
        setProject((prev: any) => ({
          ...(prev ?? {}),
          extras: { ...(prev?.extras ?? {}), restrictions: nextRestrictions },
        }));;
        setTrips(remainingTrips);
        setStopTimes(nextStopTimes);
        setShapePts(nextShapePts);
        setRoutes(nextRoutes);

        // Clear selections that reference deleted routes
        setSelectedRouteIds(new Set());
        if (selectedRouteId && doomedRoutes.has(selectedRouteId)) setSelectedRouteId(null);
      });
    } catch (e) {
      console.error(e);
      setBanner({ kind: "error", text: "Bulk delete failed. Try again." });
      setTimeout(() => setBanner(null), 3000);
    } finally {
      // resume autosave after paint
      queueMicrotask(() => { suppressPersist.current = false; });
    }
  };

  // --- Route & Trip helpers ---
const nextId = (prefix: string, existing: string[]) => {
  // finds the next unused number like R_001, T_004, etc.
  let n = existing.length + 1;
  const set = new Set(existing);
  while (true) {
    const id = `${prefix}${String(n).padStart(3, "0")}`;
    if (!set.has(id)) return id;
    n++;
  }
};

const createNewRoute = () => {
  // 1) Make a unique route_id
  const rid = nextId("R_", routes.map(r => r.route_id));

  // 2) Make a unique service_id with weekday defaults + 1 year range
  const sid = nextId("SVC_", services.map(s => s.service_id));
  const newService: Service = {
    service_id: sid,
    monday: 1, tuesday: 1, wednesday: 1, thursday: 1, friday: 1, saturday: 0, sunday: 0,
    start_date: toYYYYMMDD(new Date()),
    end_date: toYYYYMMDD(new Date(new Date().setFullYear(new Date().getFullYear() + 1))),
  };

  // 3) Make a unique trip_id tied to the new route + service
  const tid = nextId("T_", trips.map(t => t.trip_id));
  const newTrip: Trip = {
    route_id: rid,
    service_id: sid,
    trip_id: tid,
    trip_headsign: "",
    shape_id: "",
    direction_id: undefined,
  };

  // 4) Create the route itself
  const newRoute: RouteRow = {
    route_id: rid,
    route_short_name: "",
    route_long_name: "",
    route_type: 3, // bus by default
    agency_id: (agencies[0]?.agency_id ?? "agency_1"),
  };

  // 5) Commit all updates together
  setRoutes(prev => [...prev, newRoute]);
  setServices(prev => [...prev, newService]);
  setTrips(prev => [...prev, newTrip]);

  // 6) Select & reveal the new route; optionally focus the service chips too
  setSelectedRouteId(rid);
  setSelectedRouteIds(new Set([rid]));
  // show the new service chip as active so it’s obvious
  setActiveServiceIds(new Set([sid]));

  // 7) Scroll to the routes table (you already have this ref)
  routesTableRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });

  setBanner({ kind: "success", text: `Route ${rid} created with ${sid} and trip ${tid}.` });
  setTimeout(() => setBanner(null), 1500);
};


const duplicateSelectedRoute = () => {
  if (!selectedRouteId) return;
  const base = routes.find(r => r.route_id === selectedRouteId);
  if (!base) return;
  const rid = nextId("R_", routes.map(r => r.route_id));
  const copy: RouteRow = {
    ...base,
    route_id: rid,
    route_short_name: base.route_short_name ? `${base.route_short_name}` : "",
    route_long_name: base.route_long_name ? `${base.route_long_name}` : "",
  };
  setRoutes(prev => [...prev, copy]);

  // (optional) copy trips too? keep it simple: not copying dependent tables by default
  setSelectedRouteId(rid);
  setSelectedRouteIds(new Set([rid]));
  routesTableRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });

  setBanner({ kind: "success", text: `Duplicated as ${rid}.` });
  setTimeout(() => setBanner(null), 1500);
};
const routesTableRef = useRef<HTMLDivElement | null>(null);
const tripsTableRef  = useRef<HTMLDivElement | null>(null);
const agenciesRef    = useRef<HTMLDivElement | null>(null);
const stopsRef       = useRef<HTMLDivElement | null>(null);
const calendarRef    = useRef<HTMLDivElement | null>(null);
const stopTimesRef   = useRef<HTMLDivElement | null>(null);
const shapesRef      = useRef<HTMLDivElement | null>(null);
  // Stash original CSV text for lazy hydration at export time
const rawCsvRef = useRef<{ stop_times?: string; shapes?: string }>({});

const createTripForSelectedRoute = () => {
  if (!selectedRouteId) {
    setBanner({ kind: "info", text: "Select a route first." });
    setTimeout(() => setBanner(null), 1500);
    return;
  }
  const tid = nextId("T_", trips.map(t => t.trip_id));
  const newTrip: Trip = {
    route_id: selectedRouteId,
    service_id: (services[0]?.service_id ?? "WKDY"),
    trip_id: tid,
    trip_headsign: "",
    shape_id: "",       // optional; you can assign later
    direction_id: undefined,
  };
  setTrips(prev => [...prev, newTrip]);
  setBanner({ kind: "success", text: `Trip ${tid} added.` });
  setTimeout(() => setBanner(null), 1500);
};

  /** Delete selected stop (from stops + all stop_times) */
  const deleteSelectedStop = () => {
    if (!selectedStopId) return;
    if (!confirm(`Delete stop ${selectedStopId}? This removes it from stops and all stop_times.`)) return;
    const sid = selectedStopId;
    setStops(prev => prev.filter(s => s.stop_id !== sid));
    setStopTimes(prev => prev.filter(st => st.stop_id !== sid));
    setSelectedStopId(null);
  };

  // Move currently selected stop to a new lat/lng, flash banner, then deselect
  // keep selection after move (don’t return null)
  const relocateSelectedStop = useCallback((lat: number, lng: number) => {
    setSelectedStopId(prevId => {
      if (!prevId) return prevId;
      setStops(prev =>
        prev.map(row =>
          row.stop_id === prevId ? { ...row, stop_lat: lat, stop_lon: lng } : row
        )
      );
      setBanner({ kind: "success", text: "Position of stop updated" });
      setTimeout(() => setBanner(null), 1600);

      // Rebuild path for the selected route (debounced)
      queueMicrotask(queueRebuildShapeForSelectedRoute);

      return prevId;
    });
  }, [setStops, queueRebuildShapeForSelectedRoute]);

  function groupStopTimesByTrip(rows: StopTime[]) {
    const m = new Map<string, StopTime[]>();
    for (const r of rows) {
      if (!m.has(r.trip_id)) m.set(r.trip_id, []);
      m.get(r.trip_id)!.push(r);
    }
    for (const [k, arr] of m) arr.sort((a,b)=>num(a.stop_sequence)-num(b.stop_sequence));
    return m;
  }

  function compileTripsWithOD(
    restrictions: Record<string, { mode: "normal" | "pickup" | "dropoff" | "custom"; dropoffOnlyFrom?: string[]; pickupOnlyTo?: string[] }>,
    rowsByTrip: Map<string, StopTime[]>
  ) {
    const outTrips: Trip[] = [];
    const outStopTimes: StopTime[] = [];

    const hhmmss = (t?: string) => toHHMMSS(t);

    for (const t of trips) {
      const rows = (rowsByTrip.get(t.trip_id) ?? []).slice().sort((a,b)=>a.stop_sequence-b.stop_sequence);
      if (!rows.length) continue;

      // collect rules by position
      const rulesByIdx = new Map<number, ODRestriction>();
      rows.forEach((st, i) => {
        const r = restrictions[`${t.trip_id}::${st.stop_id}`];
        if (r) rulesByIdx.set(i, r);
      });

      const hasCustom = Array.from(rulesByIdx.values()).some(r => r.mode === "custom");

      // ▶ no custom: just emit single trip with plain pickup/dropoff flags
      if (!hasCustom) {
        outTrips.push({ ...t });
        rows.forEach((st) => {
          const r = restrictions[`${t.trip_id}::${st.stop_id}`];
          let pickup_type = 0, drop_off_type = 0;
          if (r?.mode === "pickup")  drop_off_type = 1;
          if (r?.mode === "dropoff") pickup_type  = 1;
          outStopTimes.push({
            trip_id: t.trip_id,
            stop_id: st.stop_id,
            stop_sequence: 0,
            arrival_time: hhmmss(st.arrival_time),
            departure_time: hhmmss(st.departure_time),
            pickup_type,
            drop_off_type,
          });
        });
        continue;
      }

      // ▶ custom exists: find the span
      const customIdxs = rows.map((_, i) => i).filter(i => rulesByIdx.get(i)?.mode === "custom");
      const firstC = Math.min(...customIdxs);
      const lastC  = Math.max(...customIdxs);

      // A) upstream segment (up to lastC) — same as before
      const upId = `${t.trip_id}__segA`;
      outTrips.push({ ...t, trip_id: upId });
      for (let i = 0; i <= lastC; i++) {
        const st = rows[i];
        const r = rulesByIdx.get(i);
        let pickup_type = 0, drop_off_type = 0;
        if (r?.mode === "pickup")  drop_off_type = 1;
        else if (r?.mode === "dropoff") pickup_type = 1;
        else if (r?.mode === "custom") { pickup_type = 1; drop_off_type = 0; } // no board in middle
        outStopTimes.push({
          trip_id: upId,
          stop_id: st.stop_id,
          stop_sequence: 0,
          arrival_time: hhmmss(st.arrival_time),
          departure_time: hhmmss(st.departure_time),
          pickup_type,
          drop_off_type,
        });
      }

      // B) downstream segment (from firstC) — same as before
      const downId = `${t.trip_id}__segB`;
      outTrips.push({ ...t, trip_id: downId });
      for (let i = firstC; i < rows.length; i++) {
        const st = rows[i];
        const r = rulesByIdx.get(i);
        let pickup_type = 0, drop_off_type = 0;
        if (r?.mode === "pickup")  drop_off_type = 1;
        else if (r?.mode === "dropoff") pickup_type = 1;
        else if (r?.mode === "custom") { pickup_type = 0; drop_off_type = 1; } // no alight in middle
        outStopTimes.push({
          trip_id: downId,
          stop_id: st.stop_id,
          stop_sequence: 0,
          arrival_time: hhmmss(st.arrival_time),
          departure_time: hhmmss(st.departure_time),
          pickup_type,
          drop_off_type,
        });
      }

      // C) “bridge” variant: keep non-custom stops untouched (0/0),
      // and block only the custom-span indices (1/1). Explicit simple rules still override.
      const bridgeId = `${t.trip_id}__bridge`;
      outTrips.push({ ...t, trip_id: bridgeId });

      for (let i = 0; i < rows.length; i++) {
        const st = rows[i];

        // default: full service
        let pickup_type = 0;
        let drop_off_type = 0;

        const r = rulesByIdx.get(i);
        if (r?.mode === "custom") {
          // block both within Spain (your custom span)
          pickup_type = 1;
          drop_off_type = 1;
        } else if (r?.mode === "pickup") {
          // explicit pickup-only at this exact stop (if you set it)
          drop_off_type = 1;
        } else if (r?.mode === "dropoff") {
          // explicit dropoff-only at this exact stop (if you set it)
          pickup_type = 1;
        }

        outStopTimes.push({
          trip_id: bridgeId,
          stop_id: st.stop_id,
          stop_sequence: 0,
          arrival_time: toHHMMSS(st.arrival_time),
          departure_time: toHHMMSS(st.departure_time),
          pickup_type,
          drop_off_type,
        });
      }
    }

    // finalize sequences
    const grouped = new Map<string, StopTime[]>();
    for (const st of outStopTimes) {
      if (!grouped.has(st.trip_id)) grouped.set(st.trip_id, []);
      grouped.get(st.trip_id)!.push(st);
    }
    const finalStopTimes: StopTime[] = [];
    for (const [, arr] of grouped) {
      arr.forEach((st, i) => (st.stop_sequence = i + 1));
      finalStopTimes.push(...arr);
    }
    return { trips: outTrips, stop_times: finalStopTimes };
  }

  // ⬇️ ADD a third param: rowsByTripForExport
  async function exportGtfsCompiled(
    onlyRoutes?: Set<string>,
    opts?: { roundCoords?: boolean; decimateShapes?: boolean; maxShapePts?: number },
    rowsByTripForExport?: Map<string, StopTime[]>
  ) {
    const zip = new JSZip();

    const roundCoords = opts?.roundCoords ?? true;
    const decimateShapes = opts?.decimateShapes ?? true;
    const maxShapePts = opts?.maxShapePts ?? 2000;

    // IMPORTANT: prefer the full-feed rows map we pass in; fall back to the UI’s memo if needed
    const rowsByTrip = rowsByTripForExport ?? stopTimesByTrip;

    // 1) Compile OD rules into concrete trips/stop_times
    const restrictions = (project?.extras?.restrictions ?? {}) as Record<string, any>;
    const { trips: compiledTripsAll, stop_times: compiledStopTimesAll } =
      compileTripsWithOD(restrictions, rowsByTrip);

    // 2) If filtering, keep only trips for selected routes
    const compiledTrips = onlyRoutes
      ? compiledTripsAll.filter(tr => onlyRoutes.has(tr.route_id))
      : compiledTripsAll;

    const keepTripIds = new Set(compiledTrips.map(t => t.trip_id));
    const compiledStopTimes = compiledStopTimesAll.filter(st => keepTripIds.has(st.trip_id));

    // 3) Derive dependent sets
    const keepRouteIds = new Set(compiledTrips.map(t => t.route_id));
    const keepSvcIds   = new Set(compiledTrips.map(t => t.service_id));
    const keepStopIds  = new Set(compiledStopTimes.map(st => st.stop_id));
    const keepShapeIds = new Set(compiledTrips.map(t => t.shape_id).filter(Boolean) as string[]);

    // 4) Write CSVs using *filtered* rows
    const agenciesOut = agencies.length ? agencies : [{
      agency_id: "agency_1",
      agency_name: "Agency",
      agency_url: "https://example.com",
      agency_timezone: defaultTZ,
    }];
    zip.file(
      "agency.txt",
      csvify(agenciesOut, ["agency_id","agency_name","agency_url","agency_timezone"])
    );

    const routesOut = routes.filter(r => keepRouteIds.has(r.route_id));
    zip.file(
      "routes.txt",
      csvify(
        routesOut.map(r => ({
          route_id: r.route_id,
          route_short_name: r.route_short_name ?? "",
          route_long_name: r.route_long_name ?? "",
          route_type: r.route_type,
          agency_id: r.agency_id || agenciesOut[0].agency_id,
        })),
        ["route_id","route_short_name","route_long_name","route_type","agency_id"]
      )
    );

    const servicesOut = services.filter(s => keepSvcIds.has(s.service_id));
    zip.file(
      "calendar.txt",
      csvify(
        servicesOut,
        ["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"]
      )
    );

    const stopsOut = stops.filter(s => keepStopIds.has(s.stop_id));
    zip.file(
      "stops.txt",
      csvify(
        stopsOut.map(s => ({
          stop_id: s.stop_id,
          stop_name: s.stop_name,
          stop_lat: s.stop_lat,
          stop_lon: s.stop_lon
        })),
        ["stop_id","stop_name","stop_lat","stop_lon"]
      )
    );

    const tripsOut = compiledTrips.map(tr => ({
      route_id: tr.route_id,
      service_id: tr.service_id,
      trip_id: tr.trip_id,
      trip_headsign: tr.trip_headsign ?? "",
      shape_id: tr.shape_id ?? "",
      direction_id: tr.direction_id ?? "",
    }));
    zip.file(
      "trips.txt",
      csvify(tripsOut, ["route_id","service_id","trip_id","trip_headsign","shape_id","direction_id"])
    );

    const stopTimesOut = compiledStopTimes.map(st => ({
      trip_id: st.trip_id,
      arrival_time: toHHMMSS(st.arrival_time),
      departure_time: toHHMMSS(st.departure_time),
      stop_id: st.stop_id,
      stop_sequence: st.stop_sequence,
      pickup_type: st.pickup_type ?? 0,
      drop_off_type: st.drop_off_type ?? 0,
    }));
    zip.file(
      "stop_times.txt",
      csvify(
        stopTimesOut,
        ["trip_id","arrival_time","departure_time","stop_id","stop_sequence","pickup_type","drop_off_type"]
      )
    );

    // 5) shapes.txt (filtered + optional rounding/decimation)
    let shapesOut = shapePts
      .filter(p => keepShapeIds.has(p.shape_id))
      .map(p => ({
        shape_id: p.shape_id,
        shape_pt_lat: p.lat,
        shape_pt_lon: p.lon,
        shape_pt_sequence: p.seq
      }));

    if (roundCoords) {
      const r5 = (x: number) => Math.round(x * 1e5) / 1e5;
      shapesOut = shapesOut.map(p => ({
        ...p,
        shape_pt_lat: r5(p.shape_pt_lat),
        shape_pt_lon: r5(p.shape_pt_lon),
      }));
    }

    if (decimateShapes) {
      const groups = new Map<string, typeof shapesOut>();
      for (const p of shapesOut) {
        if (!groups.has(p.shape_id)) groups.set(p.shape_id, []);
        groups.get(p.shape_id)!.push(p);
      }
      const slim: typeof shapesOut = [];
      for (const [sid, arr] of groups) {
        const sorted = arr.slice().sort((a,b)=>a.shape_pt_sequence-b.shape_pt_sequence);
        if (sorted.length <= maxShapePts) {
          slim.push(...sorted);
        } else {
          const step = Math.ceil(sorted.length / maxShapePts);
          for (let i=0;i<sorted.length;i+=step) slim.push(sorted[i]);
          const last = sorted[sorted.length-1];
          if (slim[slim.length-1]?.shape_pt_sequence !== last.shape_pt_sequence) slim.push(last);
        }
      }
      shapesOut = slim;
    }

    if (shapesOut.length) {
      zip.file(
        "shapes.txt",
        csvify(shapesOut, ["shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"])
      );
    }

    // 6) Zip and save
    const blob = await zip.generateAsync({
      type: "blob",
      compression: "DEFLATE",
      compressionOptions: { level: 6 }
    });
    saveAs(blob, "gtfs_compiled.zip");
  }

  const addShapeRow = () => {
    

    const existingShapeIds = Array.from(new Set(shapePts.map(p => p.shape_id)));
    const nextShapeId = nextId("shape_", existingShapeIds);
    const newShapePt: ShapePt = {
      shape_id: nextShapeId,
      lat: mapCenter?.lat ?? 40.4168,
      lon: mapCenter?.lng ?? -3.7038,
      seq: 1,
    };

    setShapePts(prev => [...prev, newShapePt]);
    shapesRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    setBanner({ kind: "success", text: `Shape point added to ${nextShapeId}.` });
    setTimeout(() => setBanner(null), 1200);
  };





  /** ---------- Shape builders (auto by route_type) ---------- */

// Map GTFS route_type -> profile label
function getRouteProfileFromType(route_type?: number) {
  // GTFS types: 0 tram, 1 subway, 2 rail, 3 bus, 4 ferry, 5 cable tram, 6 aerial, 7 funicular,
  // 11 trolleybus, 12 monorail
  if (route_type === 3 || route_type === 11) return "road";  // bus, trolleybus
  if (route_type === 4) return "water";                       // ferry
  if ([0,1,2,5,7,12].includes(Number(route_type))) return "rail"; // tram/subway/rail/cable/funicular/monorail
  return "road"; // default
}

// Get ordered stops for the selected route using the first trip that has stop_times
function getOrderedStopsForSelectedRoute(): { shapeId: string; coords: [number, number][] } | null {
  if (!selectedRouteId) return null;

  // choose a trip in this route that has stop_times
  const routeTrips = trips.filter(t => t.route_id === selectedRouteId);
  if (!routeTrips.length) return null;

  const withRows = routeTrips
    .map(t => ({ t, rows: stopTimesByTrip.get(t.trip_id) ?? [] }))
    .filter(x => x.rows.length >= 2)
    .sort((a, b) => b.rows.length - a.rows.length);

  if (!withRows.length) return null;

  const chosen = withRows[0];
  const rows = chosen.rows.slice().sort((a,b) => num(a.stop_sequence) - num(b.stop_sequence));

  const coords: [number, number][] = [];
  for (const r of rows) {
    const s = stopsById.get(r.stop_id);
    if (s) coords.push([s.stop_lat, s.stop_lon]);
  }
  if (coords.length < 2) return null;

  // choose (or create) a shape_id for this route
  const existingShapeId =
    routeTrips.find(t => t.shape_id)?.shape_id ||
    (() => {
      const existingShapeIds = Array.from(new Set(shapePts.map(p => p.shape_id)));
      return nextId("shape_", existingShapeIds);
    })();

  return { shapeId: existingShapeId, coords };
}

// Write shape points and assign shape_id to all trips in the selected route
function commitShapeToSelectedRoute(routeId: string, shapeId: string, path: [number, number][]) {
  // 1) ensure trips in route point to this shape
  setTrips(prev => prev.map(t =>
    t.route_id === routeId ? { ...t, shape_id: shapeId } : t
  ));

  // 2) replace shape points for this shape_id
  setShapePts(prev => {
    const kept = prev.filter(p => p.shape_id !== shapeId);
    const fresh = path.map((latlon, i) => ({
      shape_id: shapeId,
      lat: latlon[0],
      lon: latlon[1],
      seq: i + 1,
    }));
    return [...kept, ...fresh];
  });

  // 3) persist route -> shape mapping
  setProject((prev: any) => {
    const map: ShapeByRoute = prev?.extras?.shapeByRoute ?? {};
    return {
      ...(prev ?? {}),
      extras: {
        ...(prev?.extras ?? {}),
        shapeByRoute: { ...map, [routeId]: shapeId },
      },
    };
  });

  setBanner({ kind: "success", text: "Shape rebuilt." });
  setTimeout(() => setBanner(null), 1600);
}

// OSRM (road) builder: fetches a routed polyline between ordered stops
async function buildRoadShapeViaOSRM(coords: [number, number][]) {
  // OSRM expects lon,lat; we have lat,lon
  const coordStr = coords.map(([lat, lon]) => `${lon},${lat}`).join(";");
  // public demo server (best-effort; may throttle)
  const url = `https://router.project-osrm.org/route/v1/driving/${coordStr}?overview=full&geometries=geojson&continue_straight=true`;

  const res = await fetch(url);
  if (!res.ok) throw new Error(`OSRM error ${res.status}`);
  const json = await res.json();

  const geom = json?.routes?.[0]?.geometry?.coordinates as [number, number][];
  if (!geom?.length) throw new Error("OSRM returned no geometry");

  // convert [lon,lat] → [lat,lon]
  return geom.map(([lon, lat]) => [lat, lon] as [number, number]);
}


// Get ordered stops for a given route id using the longest trip that has stop_times
function getOrderedStopsForRoute(
    routeId: string,
    rowsByTrip: StopTimesByTrip
  ): { shapeId: string; coords: [number, number][] } | null {
    const routeTrips = trips.filter(t => t.route_id === routeId);
    if (!routeTrips.length) return null;

    const withRows = routeTrips
      .map(t => ({
        t,
        rows: (rowsByTrip.get(t.trip_id) ?? [])
          .slice()
          .sort((a, b) => num(a.stop_sequence) - num(b.stop_sequence)),
      }))
      .filter(x => x.rows.length >= 2)
      .sort((a, b) => b.rows.length - a.rows.length);

    if (!withRows.length) return null;

    const chosen = withRows[0];

    const coords: [number, number][] = [];
    const missing: string[] = [];

    for (const r of chosen.rows) {
      const s = stopsById.get(r.stop_id);
      if (s && Number.isFinite(s.stop_lat) && Number.isFinite(s.stop_lon)) {
        coords.push([s.stop_lat, s.stop_lon]);
      } else {
        missing.push(r.stop_id);
      }
    }

    if (coords.length < 2) return null;

    const existingShapeId =
      routeTrips.find(t => t.shape_id)?.shape_id ||
      (() => {
        const existingShapeIds = Array.from(new Set(shapePts.map(p => p.shape_id)));
        return nextId("shape_", existingShapeIds);
      })();

    return { shapeId: existingShapeId as string, coords };
  }

/** Build shape automatically for selected route based on route_type */
async function buildShapeAutoForSelectedRoute(forceCreate = false) {
  const rid = resolveActiveRouteId();
  log("buildShapeAutoForSelectedRoute start", { selectedRouteId, resolved: rid, forceCreate });

  if (!rid) {
    setBanner({ kind: "info", text: "Select a route first (or keep only one route in the table)." });
    setTimeout(() => setBanner(null), 1400);
    return;
  }

  // NOTE: from here on, use rid instead of selectedRouteId
  const anyShapeId = trips.some(t => t.route_id === rid && t.shape_id);
  log("anyShapeId?", anyShapeId);

  if (!anyShapeId && !forceCreate) {
    setBanner({ kind: "info", text: "No shape yet for this route." });
    setTimeout(() => setBanner(null), 1400);
    log("buildShapeAutoForSelectedRoute abort: no shape_id & not forceCreate");
    return;
  }

  const route = routes.find(r => r.route_id === rid);
  const pack = getOrderedStopsForRoute(rid, stopTimesByTrip as StopTimesByTrip);
  log("route / pack", route?.route_id, !!pack);

  if (!route || !pack) {
    setBanner({ kind: "info", text: "Add stop_times for this route first." });
    setTimeout(() => setBanner(null), 1600);
    return;
  }

  const profile = getRouteProfileFromType(route.route_type);
  const { shapeId, coords } = pack;
  log("profile / chosen shapeId / coords", profile, shapeId, coords.length);

  try {
    if (profile === "road") {
      const routed = await buildRoadShapeViaOSRM(coords);
      log("OSRM returned points:", routed.length);
      commitShapeToSelectedRoute(rid, shapeId, routed);
      return;
    }
    log("Using straight segments (fallback) points:", coords.length);
    commitShapeToSelectedRoute(rid, shapeId, coords.slice());
    setBanner({
      kind: "info",
      text: profile === "rail"
        ? "Rail auto-build uses straight segments as a fallback."
        : "Water auto-build uses straight segments as a fallback.",
    });
    setTimeout(() => setBanner(null), 2200);
  } catch (e) {
    console.error(e);
    setBanner({ kind: "error", text: "Failed to build shape. Try again." });
    setTimeout(() => setBanner(null), 2200);
  }
}

function CaptureMap({ onReady }: { onReady: (m: L.Map) => void }) {
  const m = useMap();
  useEffect(() => {
    onReady(m);
  }, [m, onReady]);
  return null;
}

/** Clear shape points for the selected route’s shape_id */
/** Clear shape points for any selected routes (multiselect-safe) */
function clearShapeForSelectedRoute() {
  const targetRouteIds = (() => {
    if (selectedRouteIds && selectedRouteIds.size > 0) return new Set(selectedRouteIds);
    const rid = resolveActiveRouteId();
    return rid ? new Set([rid]) : new Set<string>();
  })();

  if (!targetRouteIds.size) {
    setBanner({ kind: "info", text: "Select a route first (or keep only one route in the table)." });
    setTimeout(() => setBanner(null), 1400);
    return;
  }

  const routeToShape: ShapeByRoute = project?.extras?.shapeByRoute ?? {};

  // 1) shape_ids from trips
  const fromTrips = new Set(
    trips
      .filter(t => targetRouteIds.has(t.route_id))
      .map(t => (t.shape_id && String(t.shape_id).trim() !== "" ? String(t.shape_id) : null))
      .filter((x): x is string => !!x)
  );

  // 2) shape_ids from the persistent mapping
  const fromMap = new Set<string>();
  targetRouteIds.forEach(rid => {
    const sid = routeToShape[rid];
    if (sid) fromMap.add(sid);
  });

  // union
  const shapeIds = new Set<string>([...fromTrips, ...fromMap]);

  if (!shapeIds.size) {
    setBanner({ kind: "info", text: "No shape to clear for the selected route(s)." });
    setTimeout(() => setBanner(null), 1400);
    return;
  }

  // Remove shape points
  setShapePts(prev => prev.filter(p => !shapeIds.has(p.shape_id)));

  // Unset shape_id on trips in those routes
  setTrips(prev => prev.map(t =>
    targetRouteIds.has(t.route_id) ? { ...t, shape_id: undefined } : t
  ));

  // Remove mapping entries for those routes
  setProject((prev: any) => {
    const map: ShapeByRoute = { ...(prev?.extras?.shapeByRoute ?? {}) };
    targetRouteIds.forEach(rid => { delete map[rid]; });
    return { ...(prev ?? {}), extras: { ...(prev?.extras ?? {}), shapeByRoute: map } };
  });

  setBanner({ kind: "success", text: "Shape cleared." });
  setTimeout(() => setBanner(null), 1200);
}


function StopsLayer({
  stops,
  selectedStopId,
  onPick
}: {
  stops: Stop[];
  selectedStopId: string | null;
  onPick: (sid: string) => void;
}) {
  // Build once per stops/selection change; Leaflet handles pan/zoom transforms
  const data = useMemo(() => ({
    type: "FeatureCollection",
    features: stops.map(s => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [s.stop_lon, s.stop_lat] },
      properties: { stop_id: s.stop_id, selected: selectedStopId === s.stop_id }
    }))
  }), [stops, selectedStopId]);

  const pointToLayer = useCallback((feature: any, latlng: L.LatLng) => {
    const selected = !!feature?.properties?.selected;
    return L.circleMarker(latlng, {
      radius: selected ? 10 : 6,
      weight: selected ? 3 : 1.5,
      color: selected ? "#df007c" : "#222",
      fillColor: selected ? "#fff" : "#fafafa",
      fillOpacity: 1,
      interactive: true,
      pane: "stopsTop",
    });
  }, []);

  const onEachFeature = useCallback((feature: any, layer: L.Layer) => {
    layer.on("click", () => {
      const sid = feature?.properties?.stop_id as string | undefined;
      if (sid) onPick(sid);
    });
  }, [onPick]);

  // Note: react-leaflet types want specific function signatures; cast to any to simplify TS
  return (
    <GeoJSON
      data={data as any}
      pointToLayer={pointToLayer as any}
      onEachFeature={onEachFeature as any}
    />
  );
}

/** Auto-rebuild whenever the selected route’s route_type changes */
const prevRouteTypeRef = useRef<number | undefined>(undefined);
useEffect(() => {
  // ⬇️ Prevent auto-rebuild during bulk imports
  if (suppressCompute.current) return;

  if (!selectedRouteId) return;

  const rt = routes.find(r => r.route_id === selectedRouteId)?.route_type;
  const prev = prevRouteTypeRef.current;

  if (prev !== undefined && prev !== rt) {
    // route_type changed → rebuild if we have stop_times
    const hasAny = (stopTimesByTrip.size > 0);
    if (hasAny) buildShapeAutoForSelectedRoute();
  }

  prevRouteTypeRef.current = rt;
}, [routes, selectedRouteId, stopTimesByTrip]);



  // App.tsx — inside App(), before return:
  const EMPTY_OBJ = useMemo(() => ({}), []);

  /** ---------- Render ---------- */
  const tooManyRoutes = routes.length > 1200; // tweak as needed

  const activeRoute =
    selectedRouteId ? routes.find(r => r.route_id === selectedRouteId) : null;

  const routeLabel = activeRoute
    ? (
        activeRoute.route_short_name?.trim() ||
        activeRoute.route_long_name?.trim() ||
        activeRoute.route_id
      )
    : "";
    return (
      
    <div className="container" style={{ padding: 16 }}>
      <style>{
        `.toolbar * { font-size: 13px !important; }
        .toolbar { margin: 0; }

        /* Unify toolbar font sizes */
        .card.section .card-body { font-size: 13px; }
        .card.section .card-body label,
        .card.section .card-body input,
        .card.section .card-body select,
        .card.section .card-body button { font-size: 13px !important; }
        .card.section .card-body h3,
        .card.section .card-body h1 { font-size: 15px; font-weight: 600; }

        /* Normalize toolbar text sizes */
        .toolbar .btn,
        .toolbar .file-btn,
        .toolbar label,
        .toolbar input,
        .toolbar select,
        .toolbar span {
          font-size: 13px;
          line-height: 1.25;
        }

        /* Keep headings slightly larger */
        .toolbar h1,
        .toolbar h3 {
          font-size: 15px;
          font-weight: 600;
        }

        .busy-fab {
          position: fixed; right: 16px; bottom: 16px;
          background: #111; color: #fff; border: none; border-radius: 999px;
          padding: 10px 14px; box-shadow: 0 8px 24px rgba(0,0,0,.18);
          display: inline-flex; align-items: center; gap: 8px; z-index: 9999;
          opacity: .92; cursor: default;
        }

        tr[draggable="true"] { user-select: none; }
        .spinner {
          width: 16px; height: 16px; border-radius: 50%;
          border: 2px solid rgba(255,255,255,.35); border-top-color: #fff;
          animation: spin 0.8s linear infinite;
        }
        @keyframes spin { to { transform: rotate(360deg); } }

        .map-shell.bw .leaflet-tile { filter: grayscale(1) contrast(1.05) brightness(1.0); }
        .route-halo-pulse {
          animation: haloPulse 1.8s ease-in-out infinite !important;
          filter: drop-shadow(0 0 4px rgba(255,255,255,0.9));
          stroke: #ffffff !important; stroke-linecap: round !important;
        }
        @keyframes haloPulse {
          0%{stroke-opacity:.65;stroke-width:10px;}
          50%{stroke-opacity:.2;stroke-width:16px;}
          100%{stroke-opacity:.65;stroke-width:10px;}
        }`}</style>


      {banner && (
        <div className={`banner ${banner.kind === "error" ? "banner-error" : banner.kind === "success" ? "banner-success" : "banner-info"}`}
             style={{ margin: "8px 0 12px", padding: "8px 12px", borderRadius: 12, boxShadow: "0 2px 8px rgba(0,0,0,0.08)" }}>
          {banner.text}
        </div>
      )}

      <datalist id="gtfs-stop-ids">
        {stops.map(s => <option key={s.stop_id} value={s.stop_id}>{s.stop_name}</option>)}
      </datalist>
      <datalist id="gtfs-stop-names">
        {stops.map(s => <option key={s.stop_id} value={s.stop_name}>{s.stop_id}</option>)}
      </datalist>

      {/* Toolbar */}
      <div className="card section" style={{ marginBottom: 12 }}>
        <div className="card-body toolbar" style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
          <h1 style={{ fontSize: '13px', color: '#df007c', display: 'block'}}>GTFS Builder</h1>

          <label className="file-btn">
            Import GTFS .zip
            <input
              type="file"
              accept=".zip"
              style={{ display: "none" }}
              onChange={(e) => {
                const f = e.target.files?.[0];
                if (f) importGTFSZip(f);
                (e.target as HTMLInputElement).value = "";
              }}
            />
          </label>

          

          <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <input type="checkbox" checked={showRoutes} onChange={e => setShowRoutes(e.target.checked)} />
            Show routes
          </label>

          <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <input
              type="checkbox"
              checked={isScopedView}
              onChange={e => setIsScopedView(e.target.checked)}
            />
            Scope tables/map to selected route
          </label>

          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <button className="btn" onClick={createNewRoute}>New route</button>
          </div>

          
          

          {selectedRouteIds.size > 0 && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                background: "#f6f7f9",
                padding: "6px 10px",
                borderRadius: 10
              }}
            >
              Selected: <b>{Array.from(selectedRouteIds).join(", ")}</b>

              <button
                className="btn"
                title="Clear selection"
                disabled={isBusy}
                onClick={() => {
                  if (isBusy) return;
                  setSelectedRouteIds(new Set());
                  setSelectedRouteId(null);
                }}
              >
                ×
              </button>

              <button
                className="btn btn-danger"
                disabled={isBusy}
                aria-busy={isBusy ? "true" : "false"}
                onClick={() =>
                  withBusy(`Deleting ${selectedRouteIds.size} route(s)…`, async () => {
                    if (!confirm(`Delete ${selectedRouteIds.size} route(s)?`)) return;
                    // Single-pass bulk delete (no per-route loop)
                    hardDeleteRoutesBulk(selectedRouteIds);
                  })
                }
              >
                Delete selected
              </button>
            </div>
          )}

          {selectedStopId && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, background: "#fff4f4", padding: "6px 10px", borderRadius: 10 }}>
              Selected stop: <b>{selectedStopId}</b>
              <button className="btn" onClick={() => setSelectedStopId(null)} title="Deselect stop">×</button>
              <button className="btn btn-danger" onClick={deleteSelectedStop} title="Delete stop">Delete stop</button>
            </div>
          )}

          <button className="btn" onClick={() => { setSelectedRouteId(null); setSelectedRouteIds(new Set()); setSelectedStopId(null); setActiveServiceIds(new Set()); setClearSignal(x => x + 1); }}>
            Deselect route
          </button>

          <label className="file-btn">
            Import project JSON
            <input type="file" accept="application/json" style={{ display: "none" }}
              onChange={e => { const f = e.target.files?.[0]; if (f) importProject(f); }} />
          </label>

          <button className="file-btn" onClick={exportProjectJSON}>Export project JSON</button>

          

          <label className="file-btn">
            Import custom stop rules
            <input
              type="file"
              accept="application/json"
              style={{ display: "none" }}
              onChange={(e) => {
                const f = e.target.files?.[0];
                if (f) importOverrides(f);
                // reset input so you can re-upload the same file later if needed
                (e.target as HTMLInputElement).value = "";
              }}
            />
          </label>
          
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap", background: "#f7f8fa", padding: "6px 10px", borderRadius: 10 }}>
            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input
                type="checkbox"
                checked={exportOnlySelectedRoutes}
                onChange={e => setExportOnlySelectedRoutes(e.target.checked)}
              />
              Export only selected routes
            </label>
          </div>
          <button className="btn btn-primary" onClick={onExportGTFS}>Export GTFS .zip</button>

          

          <button
            className="btn"
            onClick={() => {
              // grab ALL rules (not just selected route)
              const allRules = (project?.extras?.restrictions ?? {}) as Record<string, any>;

              // keep only non-"normal" (i.e., where you actually set pickup/dropoff/custom)
              const pruned: Record<string, any> = {};
              for (const [k, v] of Object.entries(allRules)) {
                if (!v) continue;
                const m = asMode((v as any)?.mode);
                if (m !== "normal") pruned[k] = v;
              }

              const blob = new Blob(
                [JSON.stringify({ version: 1, rules: pruned }, null, 2)],
                { type: "application/json" }
              );
              saveAs(blob, "overrides.json");
              setBanner({ kind: "success", text: "Exported all custom pickup/dropoff rules." });
              setTimeout(() => setBanner(null), 2000);
            }}
          >
            Export custom rules
          </button>

          

          <button className="btn btn-danger" onClick={resetAll}>Reset</button>


          <button
            className="btn"
            onClick={() => window.open("https://gtfs-validator.mobilitydata.org/", "_blank")}
            title="Opens MobilityData’s GTFS Validator in a new tab"
            style={{ display: "inline-flex", alignItems: "center", gap: 6 }}
          >
            Validate
            <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ opacity: 0.7 }}>
              <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
              <polyline points="15 3 21 3 21 9"></polyline>
              <line x1="10" y1="14" x2="21" y2="3"></line>
            </svg>
          </button>
        </div>
      </div>

      {/* Map */}
      <div className="card section">
        <div className="card-body">
          {(mapZoom < MIN_ADD_ZOOM) && (
            <span
              style={{
                padding: "6px 10px",
                borderRadius: 8,
                background: "#fff7ed",   // orange-100
                color: "#b45309",        // orange-700
                border: "1px solid #fed7aa",
                fontSize: 12,
                fontWeight: 600,
              }}
              title={`Zoom in to at least ${MIN_ADD_ZOOM} to add stops by clicking on the map and see the route paths`}
            >
              Zoom in to at least {MIN_ADD_ZOOM} to add stops by clicking on the map and see the route paths.
            </span>
          )}
          <div className={`map-shell ${selectedRouteId ? "bw" : ""}`} style={{ height: 400, width: "100%", borderRadius: 12, overflow: "hidden", position: "relative" }}>
            {isBusy && (
              <div
                style={{
                  position: "absolute", inset: 0, background: "rgba(255,255,255,0.6)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  zIndex: 1000, backdropFilter: "blur(1px)", pointerEvents: "auto"
                }}
              >
                <div style={{ padding: "10px 14px", borderRadius: 10, background: "#fff", boxShadow: "0 2px 10px rgba(0,0,0,.12)", display:"flex", gap:10, alignItems:"center" }}>
                  <span className="spinner" />
                  <span>{busyLabel || "Working…"}</span>
                </div>
              </div>
            )}
            {routes.length > 1200 && !selectedRouteId && selectedRouteIds.size === 0 && (
              <div style={{
                marginBottom: 6, padding: "6px 10px", borderRadius: 8,
                background: "#fff7ed", color: "#9a3412", border: "1px solid #fed7aa",
                fontSize: 12
              }}>
                Large feed detected. Select a route on the map or in <b>routes.txt</b> to render it.
              </div>
            )}
            <MapContainer
              center={[40.4168, -3.7038]}
              zoom={6}
              preferCanvas={true}
              renderer={L.canvas()}
              style={{ height: "100%", width: "100%" }}
            >
              <CaptureMap onReady={(m) => { mapRef.current = m; }} />
              {!selectedStopId && !selectedRouteId && (
                <AddStopOnMapClick
                  onAdd={(lat, lng) => addStopAt(lat, lng)}
                  onTooFar={() =>
                    setBanner({
                      kind: "info",
                      text: `Zoom in to at least ${MIN_ADD_ZOOM} to add stops and see the route paths.`,
                    })
                  }
                />
              )}

            {(selectedStopId || selectedRouteId) && (
              <MapClickMenuTrigger
                onShow={({ x, y, lat, lng }) => setMapClickMenu({ x, y, lat, lng })}
                onDeselect={() => {
                  setSelectedRouteId(null);
                  setSelectedRouteIds(new Set());
                  setMapClickMenu(null);
                }}
                hasRouteSelection={!!selectedRouteId || selectedRouteIds.size > 0}
              />
            )}

              {/* Track map zoom/bounds */}
              <MapStateTracker onChange={onMapState} />

              {/* Base map tiles */}
              <TileLayer
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                attribution="© OpenStreetMap contributors"
              />

              {/* Rendering panes for layers */}
              <Pane name="routeHalo"  style={{ zIndex: 680 }} />
              <Pane name="routeLines" style={{ zIndex: 690 }} />
              <Pane name="routeHits"  style={{ zIndex: 700 }} />
              <Pane name="stopsTop"   style={{ zIndex: 650 }} />

             


              {/* Stops as a single SVG GeoJSON layer (clickable, no re-render on pan/zoom) */}
              {showStops && mapZoom >= MIN_STOP_ZOOM && (
                <StopsLayer
                  stops={
                    // keep the same scoping behavior you had before
                    scopedKeep
                      ? stops.filter(s =>
                          scopedKeep.keepStopIds.has(s.stop_id) ||
                          s.stop_id === selectedStopId ||
                          s.stop_id === selectedStopIdFromRow
                        )
                      : stops
                  }
                  selectedStopId={selectedStopId}
                  onPick={(sid) => {
                    if (isBusy) return;
                    setMapClickMenu(null);
                    setSelectedStopId(sid);
                  }}
                />
              )}


              {/* Route polylines (from cache) */}
              {!tooManyRoutes &&
                visibleRouteIds
                  .filter((route_id) => {
                    if (!isScopedView) return true;
                    if (selectedRouteIds.size) return selectedRouteIds.has(route_id);
                    return selectedRouteId ? route_id === selectedRouteId : true;
                  })
                  .map((route_id) => {
                    const geom = getRouteGeom(route_id);
                    if (!geom) return null;
                    const coords = geom.coords;

                    const isSel = selectedRouteId === route_id || selectedRouteIds.has(route_id);
                    const hasSel = !!selectedRouteId || selectedRouteIds.size > 0;
                    const color = hasSel ? (isSel ? routeColor(route_id) : DIM_ROUTE_COLOR) : routeColor(route_id);
                    const weight = isSel ? 6 : 3;
                    const opacity = hasSel ? (isSel ? 0.95 : 0.6) : 0.95;

                    const onRouteClick = (e: any) => {
                      if (e?.originalEvent) L.DomEvent.stop(e.originalEvent);
                      if (isBusy) return;
                      startTransition(() => {
                        setSelectedRouteId(route_id);
                        setSelectedRouteIds(new Set([route_id]));
                      });
                    };


                    return (
                      <div key={route_id}>
                        {/* invisible wide hit line */}
                        <Polyline
                          positions={coords as any}
                          pane="routeHits"
                          smoothFactor={2}
                          pathOptions={{
                            color: "rgba(0,0,0,0.01)",   // or color: "#000", opacity: 0.01
                            opacity: 0.01,               // > 0 so Canvas will register hits
                            weight: Math.max(16, weight + 12),
                            interactive: true
                          }}
                          bubblingMouseEvents={false}
                          eventHandlers={{ click: onRouteClick }}
                        />

                        {/* halo when selected */}
                        {isSel && (
                          <Polyline
                            positions={coords as any}
                            pane="routeHalo"
                            smoothFactor={2}
                            className="route-halo-pulse"
                            pathOptions={{
                              color: "#ffffff",
                              weight: 10,
                              opacity: 0.9,
                              lineCap: "round",
                              interactive: false
                            }}
                          />
                        )}

                        {/* visible line */}
                        <Polyline
                          positions={coords as any}
                          pane="routeLines"
                          smoothFactor={2}
                          pathOptions={{ color, weight, opacity, interactive: true }}
                          bubblingMouseEvents={false}
                          eventHandlers={{ click: onRouteClick }}
                        />
                      </div>
                    );
                  })
              }
                
            </MapContainer>

            {mapClickMenu && (
              <div
                style={{
                  position: "absolute",
                  left: Math.max(8, Math.min(mapClickMenu.x, window.innerWidth - 180)),
                  top: Math.max(8, mapClickMenu.y),
                  zIndex: 2000,
                  background: "#fff",
                  border: "1px solid #e4e7eb",
                  borderRadius: 10,
                  boxShadow: "0 8px 24px rgba(0,0,0,.12)",
                  padding: 8,
                  display: "flex",
                  gap: 8,
                  alignItems: "center",
                  pointerEvents: "auto",
                }}
                onMouseDown={(e) => e.stopPropagation()}
                onClick={(e) => e.stopPropagation()}
              >
                {/* Always allow adding a stop from the menu (even if a route is selected) */}
                <button
                  className="btn"
                  onClick={() => {
                    if (mapZoom < MIN_ADD_ZOOM) {
                      setBanner({ kind: "info", text: `Zoom in to at least ${MIN_ADD_ZOOM} to add stops and see the route paths.` });
                      setTimeout(() => setBanner(null), 1500);
                      return;
                    }
                    addStopAt(mapClickMenu.lat, mapClickMenu.lng);
                    setMapClickMenu(null);
                  }}
                >
                  Add stop
                </button>

                {/* Move selected stop to here (only if a stop is selected) */}
                {selectedStopId && (
                  <button
                    className="btn"
                    onClick={() => {
                      relocateSelectedStop(mapClickMenu.lat, mapClickMenu.lng);
                      setMapClickMenu(null);
                    }}
                  >
                    Move here
                  </button>
                )}

                <button
                  className="btn"
                  onClick={() => {
                    if (selectedStopId) setSelectedStopId(null);
                    else if (selectedRouteId) {
                      setSelectedRouteId(null);
                      setSelectedRouteIds(new Set());
                    }
                    setMapClickMenu(null);
                  }}
                >
                  Deselect
                </button>

                <button className="btn" onClick={() => setMapClickMenu(null)} title="Close" style={{ padding: "4px 8px" }}>
                  ×
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      
      <div ref={routesTableRef}>
        <PaginatedEditableTable
          title="routes.txt"
          rows={routesScoped}
          onChange={setRoutes}
          visibleIndex={routesVisibleIdx}
          initialPageSize={10}
          onSearchTyping={() => {
            // Deselect immediately when the user types in the routes search box
            if (selectedRouteId || selectedRouteIds.size) {
              setSelectedRouteId(null);
              setSelectedRouteIds(new Set());
            }
          }}
          onRowClick={(row, e) => {
            if (isBusy) return;
            const rid = (row as RouteRow).route_id;
            const meta = (e.metaKey || e.ctrlKey);
            if (meta) {
              setSelectedRouteIds((prev: Set<string>) => {
                const next = new Set(prev);
                next.has(rid) ? next.delete(rid) : next.add(rid);
                if (next.size === 1) setSelectedRouteId(rid);
                return next;
              });
            } else {
              setSelectedRouteIds(new Set([rid]));
              setSelectedRouteId(rid);
            }
          }}
          selectedPredicate={(r) => {
            const rid = (r as RouteRow).route_id;
            return selectedRouteIds.has(rid) || rid === selectedRouteId;
          }}
          selectedIcon="✓"
          clearSignal={clearSignal}
          onIconClick={(r) => {
            const rid = (r as RouteRow).route_id;
            setSelectedRouteIds((prev: Set<string>) => { const n = new Set(prev); n.delete(rid); return n; });
            if (selectedRouteId === rid) setSelectedRouteId(null);
          }}
          selectOnCellFocus
          onDeleteRow={(r) => {
            const rid = (r as RouteRow).route_id;
            if (!rid) return;
            if (!confirm(`Delete route ${rid}?`)) return;
            hardDeleteRoutesBulk(new Set([rid]));
          }}
          enableMultiSelect
          primaryAction={{ label: "+ Add route", onClick: createNewRoute }}
        />
      </div>

      {/* Service chips */}
      {selectedRouteId && (
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center", marginTop: 6 }}>
          <span style={{ fontSize: 12, opacity: .7, marginRight: 4 }}>Services in route:</span>
          {Array.from(new Set((tripsByRoute.get(selectedRouteId) ?? []).map(t => t.service_id)))
            .sort()
            .map(sid => {
              const svc = services.find(s => s.service_id === sid);
              const active = activeServiceIds.has(sid);
              return (
                <ServiceChip
                  key={sid}
                  svc={sid}
                  active={active}
                  onToggle={() => {
                    setActiveServiceIds((prev: Set<string>) => {
                      const next = new Set(prev);
                      if (next.has(sid)) next.delete(sid); else next.add(sid);
                      return next;
                    });
                  }}
                  days={svc ? {
                    mo: svc.monday, tu: svc.tuesday, we: svc.wednesday, th: svc.thursday, fr: svc.friday, sa: svc.saturday, su: svc.sunday
                  } : undefined}
                  range={svc ? { start: svc.start_date || "", end: svc.end_date || "" } : undefined}
                />
              );
            })}
          <button
            className="btn"
            onClick={() => setActiveServiceIds(new Set())}
            title="Clear service filters"
            style={{ padding: "2px 8px", fontSize: 11, height: 22 }}
          >
            Clear
          </button>
        </div>
      )}

      {/* Patterns for selected route */}
      
      {selectedRouteId ? (
        <PatternMatrix
          stops={stops}
          services={services}
          trips={tripsByRoute.get(selectedRouteId) ?? []}
          stopTimes={stopTimes.filter(st =>
            (tripsByRoute.get(selectedRouteId) ?? []).some(t => t.trip_id === st.trip_id)
          )}
          selectedRouteId={selectedRouteId}
          initialRestrictions={(project?.extras?.restrictions as RestrictionsMap) ?? EMPTY_OBJ}
          initialStopDefaults={(project?.extras?.stopDefaults as StopDefaultsMap) ?? EMPTY_OBJ}

          // still keep bulk replace if PatternMatrix ever sends a full map
          onRestrictionsChange={handleRestrictionsChange}

          // NEW: targeted, section-scoped updates
          onApplyRuleToSection={applySectionRule}

          // keep this ONLY for the UI-level "defaults" screen;
          // Summary should call onApplyRuleToSection instead of this one.
          onStopDefaultsChange={(map) =>
            setProject((prev: any) => ({
              ...(prev ?? {}),
              extras: { ...(prev?.extras ?? {}), stopDefaults: map },
            }))
          }

          onEditTime={(trip_id, stop_id, newUiTime) => {
            setStopTimes((prev) =>
              prev.map((st) =>
                st.trip_id === trip_id && st.stop_id === stop_id
                  ? { ...st, departure_time: newUiTime, arrival_time: st.arrival_time ? st.arrival_time : newUiTime }
                  : st
              )
            );
          }}
        />
      ) : (
        <div className="card section" style={{ marginTop: 12 }}>
          <div className="card-body">
            <h3>Select a route to view all its departures together</h3>
            <p style={{ opacity: 0.7, marginTop: 6 }}>Click a polyline on the map or any row in <strong>routes.txt</strong>.</p>
          </div>
        </div>
      )}

      {/* OTHER GTFS TABLES (each row has a delete button now) */}
      <div style={{ marginTop: 12 }}>
        <div ref={tripsTableRef}>
          <PaginatedEditableTable
            title="trips.txt"
            rows={tripsScoped}
            onChange={setTrips}
            initialPageSize={10}
            onDeleteRow={(row:any) => {
              const tid = row.trip_id;
              if (!tid) return;
              if (!confirm(`Delete trip ${tid}?`)) return;
              setTrips(prev => prev.filter(t => t.trip_id !== tid));
              setStopTimes(prev => prev.filter(st => st.trip_id !== tid));
            }}
            onAddRow={addTripRow}
            addRowLabel="Add trip"
          />
        </div>
        <div ref={agenciesRef}>
          <PaginatedEditableTable
            title="agency.txt"
            rows={agencies}
            onChange={setAgencies}
            initialPageSize={5}
            onDeleteRow={(row) => setAgencies(prev => prev.filter(a => a !== row))}
            onAddRow={addAgencyRow}
            addRowLabel="Add agency"
          />
        </div>
        <div ref={stopsRef}>
          <PaginatedEditableTable
            title="stops.txt"
            rows={stopsScoped.map(s => ({
              stop_id: s.stop_id,
              stop_name: s.stop_name,
              stop_lat: s.stop_lat,
              stop_lon: s.stop_lon,
            }))}
            emptyText={selectedRouteId ? "No stops linked to this route yet" : "No rows."}
            onChange={(next) => {
              const uidById = new Map(stops.map(s => [s.stop_id, s.uid]));
              setStops(next.map((r: any) => ({
                uid: uidById.get(r.stop_id) || uuidv4(),
                stop_id: r.stop_id,
                stop_name: r.stop_name,
                stop_lat: Number(r.stop_lat),
                stop_lon: Number(r.stop_lon),
              })));
            }}
            initialPageSize={5}
            onRowClick={(row: any) => { if (row.stop_id) setSelectedStopId(row.stop_id); }}
            selectedPredicate={(r: any) => r.stop_id === selectedStopId}
            selectOnCellFocus
            onDeleteRow={(r:any) => {
              const sid = r.stop_id;
              if (!sid) return;
              if (!confirm(`Delete stop ${sid}?`)) return;
              setStops(prev => prev.filter(s => s.stop_id !== sid));
              setStopTimes(prev => prev.filter(st => st.stop_id !== sid));
            }}
            onAddRow={addStopRow}
            addRowLabel="Add stop"
          />
        </div>
        <div ref={calendarRef}>
          <PaginatedEditableTable
            title="calendar.txt"
            rows={servicesScoped}
            onChange={setServices}
            initialPageSize={5}
            onDeleteRow={(row:any) => {
              const sid = row.service_id;
              if (!sid) return;
              if (!confirm(`Delete service ${sid}? Trips using it will remain with dangling reference.`)) return;
              setServices(prev => prev.filter(s => s.service_id !== sid));
            }}
            onAddRow={addServiceRow}
            addRowLabel="Add service"
          />
        </div>
        <div ref={stopTimesRef}>
          {/* Draggable stop_times grouped by trip_id */}
          {tripIdsForStopTimes.length ? (
            <div className="card section" style={{ marginTop: 10 }}>
              <div className="card-body">
                <div
                  style={{
                    display: "flex",
                    alignItems: "baseline",
                    justifyContent: "space-between",
                    gap: 12,
                    flexWrap: "wrap",
                  }}
                >
                  <h3 style={{ margin: 0 }}>
                    stop_times.txt{" "}
                    {activeTripIdForTimes && (
                      <span style={{ opacity: 0.6, fontWeight: 400 }}>
                        — trip_id:&nbsp;<code>{activeTripIdForTimes}</code>{" "}
                        ({stopTimes.filter(st => st.trip_id === activeTripIdForTimes).length} rows)
                      </span>
                    )}
                  </h3>

                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 8,
                      flexWrap: "wrap",
                    }}
                  >
                    {/* Trip selector */}
                    <label style={{ fontSize: 12, opacity: 0.7 }}>Trip:</label>
                    <select
                      className="input"
                      value={activeTripIdForTimes ?? ""}
                      onChange={(e) => setActiveTripIdForTimes(e.target.value || null)}
                      style={{ minWidth: 220, padding: "6px 8px", borderRadius: 8, border: "1px solid #e3e3e3" }}
                    >
                      {tripIdsForStopTimes.map(tid => (
                        <option key={tid} value={tid}>{tid}</option>
                      ))}
                    </select>

                    {/* Add helpers on the same line */}
                    <button className="btn" onClick={addBlankStopTimeRow}>Add blank row</button>

                    <input
                      value={existingStopToAdd}
                      onChange={(e) => setExistingStopToAdd(e.target.value)}
                      placeholder="Choose a stop…"
                      list="gtfs-stop-ids"
                      className="input"
                      style={{ minWidth: 260 }}
                    />
                    <button
                      className="btn btn-primary"
                      onClick={() => confirmAddExistingStop(activeTripIdForTimes ?? undefined)}
                    >
                      Add
                    </button>

                    {/* Clear rows for this trip */}
                    <button
                      className="btn btn-danger"
                      disabled={!activeTripIdForTimes}
                      title="Remove all stop_times rows for this trip_id (trip remains in trips.txt)"
                      onClick={() => {
                        if (!activeTripIdForTimes) return;
                        const cnt = stopTimes.filter(st => st.trip_id === activeTripIdForTimes).length;
                        if (cnt === 0) return;
                        if (!confirm(`Delete ${cnt} stop_times row(s) for trip ${activeTripIdForTimes}?`)) return;
                        setStopTimes(prev => prev.filter(st => st.trip_id !== activeTripIdForTimes));
                      }}
                    >
                      Clear rows
                    </button>
                  </div>
                </div>

                {/* Table for just the active trip */}
                {activeTripIdForTimes ? (() => {
                  const trip_id = activeTripIdForTimes;
                  const rows = stopTimes
                    .filter(st => st.trip_id === trip_id)
                    .sort((a, b) => num(a.stop_sequence) - num(b.stop_sequence));

                  return (
                    <div className="overflow-auto" style={{ borderRadius: 12, border: "1px solid #eee", marginTop: 8 }}>
                      <table style={{ width: "100%", fontSize: 13, minWidth: 900 }}>
                        <thead>
                          <tr>
                            <th style={{ width: 28 }}></th>
                            <th>stop_sequence</th>
                            <th>stop_id</th>
                            <th>stop_name</th>
                            <th>arrival_time</th>
                            <th>departure_time</th>
                            <th>pickup_type</th>
                            <th>drop_off_type</th>
                            <th style={{ width: 30 }}></th>
                          </tr>
                        </thead>
                        <tbody>
                          {rows.length ? rows.map((r, idx) => {
                            const globalKey = `${r.trip_id}::${r.stop_id}::${r.stop_sequence}::${idx}`;
                            const stopName = stopIdToName(r.stop_id);
                            const bad = badTimeKeys.has(`${r.trip_id}::${r.stop_sequence}`);
                            const badInputStyle = bad ? { border: "1px solid #e11d48", background: "#fee2e2" } : {};

                            return (
                              <tr
                                key={globalKey}
                                onDragOver={(e) => { if (dragInfo?.trip_id === trip_id) e.preventDefault(); }}
                                onDrop={(e) => {
                                  e.preventDefault();
                                  if (dragInfo?.trip_id === trip_id) moveRowWithinTrip(trip_id, dragInfo.from, idx);
                                  setDragInfo(null);
                                }}
                                style={{
                                  cursor: "default", // row is no longer the drag source
                                  background:
                                    selectedStopTime &&
                                    selectedStopTime.trip_id === r.trip_id &&
                                    Number(selectedStopTime.stop_sequence) === Number(r.stop_sequence)
                                      ? "rgba(232, 242, 255, 0.7)"
                                      : "transparent",
                                  outline:
                                    selectedStopTime &&
                                    selectedStopTime.trip_id === r.trip_id &&
                                    Number(selectedStopTime.stop_sequence) === Number(r.stop_sequence)
                                      ? "2px solid #7db7ff"
                                      : "none",
                                  outlineOffset: -2,
                                }}
                                onClick={(e) => {
                                  const t = e.target as HTMLElement;
                                  if (t.closest("input, select, button, textarea")) return; // don’t steal focus from form controls
                                  setSelectedStopTime({ trip_id, stop_sequence: r.stop_sequence });
                                }}
                              >
                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4, textAlign: "center" }} title="Drag to reorder">
                                  <button
                                    type="button"
                                    draggable
                                    onDragStart={() => setDragInfo({ trip_id, from: idx })}
                                    onMouseDown={(e) => e.stopPropagation()} // don’t bubble to row
                                    style={{
                                      border: "none",
                                      background: "transparent",
                                      cursor: "grab",
                                      padding: 0,
                                      width: 24,
                                      height: 24,
                                      lineHeight: "24px",
                                      userSelect: "none",
                                    }}
                                    aria-label="Drag to reorder"
                                    title="Drag to reorder"
                                  >
                                    ↕
                                  </button>
                                </td>

                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                  <input value={r.stop_sequence} readOnly
                                    style={{ width: 80, border: "1px solid #e8e8e8", padding: "4px 6px", borderRadius: 8, background: "#f8f9fb" }} />
                                </td>

                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                  <input
                                    value={r.stop_id}
                                    list="gtfs-stop-ids"
                                    onChange={(e) => {
                                      const sidOrName = e.target.value;
                                      const sid = stopNameToId.get(sidOrName) ?? sidOrName;
                                      setStopTimes((prev) =>
                                        prev.map((st) =>
                                          st.trip_id === r.trip_id && st.stop_sequence === r.stop_sequence
                                            ? { ...st, stop_id: sid }
                                            : st
                                        )
                                      );
                                    }}
                                    style={{ width: 180, border: "1px solid #e8e8e8", padding: "4px 6px", borderRadius: 8 }}
                                  />
                                </td>

                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                  <input value={stopName} readOnly
                                    style={{ width: 200, border: "1px solid #e8e8e8", padding: "4px 6px", borderRadius: 8, background: "#f8f9fb" }} />
                                </td>

                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                  <input
                                    value={r.arrival_time ?? ""}
                                    onChange={(e) => {
                                      const v = e.target.value;
                                      setStopTimes((prev) =>
                                        prev.map((st) =>
                                          st.trip_id === r.trip_id && st.stop_sequence === r.stop_sequence
                                            ? { ...st, arrival_time: v }
                                            : st
                                        )
                                      );
                                    }}
                                    placeholder="HH:MM:SS"
                                    style={{ width: 130, padding: "4px 6px", borderRadius: 8, border: "1px solid #e8e8e8", ...badInputStyle }}
                                  />
                                </td>

                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                  <input
                                    value={r.departure_time ?? ""}
                                    onChange={(e) => {
                                      const v = e.target.value;
                                      setStopTimes((prev) =>
                                        prev.map((st) =>
                                          st.trip_id === r.trip_id && st.stop_sequence === r.stop_sequence
                                            ? { ...st, departure_time: v }
                                            : st
                                        )
                                      );
                                    }}
                                    placeholder="HH:MM:SS"
                                    style={{ width: 130, padding: "4px 6px", borderRadius: 8, border: "1px solid #e8e8e8", ...badInputStyle }}
                                  />
                                </td>

                                {/* pickup_type */}
                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                  <div onMouseDown={(e) => e.stopPropagation()}>
                                    <StopTypeSelect
                                      title="Pickup type"
                                      value={r.pickup_type}
                                      onChange={(next) =>
                                        setStopTimes((prev) =>
                                          prev.map((st) =>
                                            st.trip_id === r.trip_id && st.stop_sequence === r.stop_sequence
                                              ? { ...st, pickup_type: next }
                                              : st
                                          )
                                        )
                                      }
                                    />
                                  </div>
                                </td>

                                {/* drop_off_type */}
                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                  <div onMouseDown={(e) => e.stopPropagation()}>
                                    <StopTypeSelect
                                      title="Drop-off type"
                                      value={r.drop_off_type}
                                      onChange={(next) =>
                                  
                                        setStopTimes((prev) =>
                                          prev.map((st) =>
                                            st.trip_id === r.trip_id && st.stop_sequence === r.stop_sequence
                                              ? { ...st, drop_off_type: next }
                                              : st
                                          )
                                        )
                                      }
                                    />
                                  </div>
                                </td>

                                <td style={{ borderBottom: "1px solid #f3f3f3", padding: 0, textAlign: "center" }}>
                                  <button
                                    title="Delete row"
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      setStopTimes((prev) =>
                                        prev.filter(
                                          (st) => !(st.trip_id === r.trip_id && st.stop_sequence === r.stop_sequence)
                                        )
                                      );
                                    }}
                                    style={{ border: "none", background: "transparent", cursor: "pointer", width: 28, height: 28, lineHeight: "28px" }}
                                  >
                                    ×
                                  </button>
                                </td>
                              </tr>
                            );
                          }) : (
                            <tr>
                              <td colSpan={9} style={{ padding: 12, opacity: 0.6 }}>No rows.</td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  );
                })() : (
                  <div className="muted" style={{ marginTop: 6 }}>
                    Choose a trip to edit its stop_times.
                  </div>
                )}

                {selectedStopTime ? (
                  <>
                    <div className="muted" style={{ marginTop: 6 }}>
                      New rows will be inserted <b>after</b> trip{" "}
                      <code>{selectedStopTime.trip_id}</code>, sequence{" "}
                      <b>{selectedStopTime.stop_sequence}</b>.
                    </div>
                    <div className="muted" style={{ marginTop: 4, fontSize: 12, color: "#666" }}>
                      Custom stop types must be changed in the Summary block.
                    </div>
                  </>
                ) : (
                  <div className="muted" style={{ marginTop: 6 }}>
                    Tip: click a row to insert the next one <b>after</b> it.
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="card section" style={{ marginTop: 10 }}>
              <div className="card-body">
                <h3 style={{ marginTop: 0 }}>stop_times.txt</h3>
                <p className="muted" style={{ marginTop: 6 }}>
                  No trips yet. Create a route and a trip to start adding stop times.
                </p>
                <div style={{ display: "flex", gap: 8 }}>
                  <button className="btn" onClick={createNewRoute}>New route</button>
                  <button className="btn" onClick={addTripRow}>Add trip</button>
                </div>
              </div>
            </div>
          )}
        </div>
        <div ref={shapesRef}>
          <PaginatedEditableTable
            title="shapes.txt"
            headerExtras={
              <>
                <button
                  className="btn"
                  title="Build shape for the selected route (auto)"
                  onClick={() => withBusy("Building shape…", () => buildShapeAutoForSelectedRoute(true))}
                >
                  Build Shape (Auto)
                </button>

                <button
                  className="btn"
                  title="Remove all shape points for the selected route"
                  onClick={clearShapeForSelectedRoute}
                >
                  Clear shape
                </button>

                {selectedRouteId && (
                  <span className="muted" style={{ marginLeft: 8 }}>
                    Profile is auto-chosen from <code>route_type</code> (road / rail / water).
                    Changing <code>route_type</code> triggers a rebuild.
                  </span>
                )}
              </>
            }
            rows={shapePtsScoped.map(s => ({
              shape_id: s.shape_id,
              shape_pt_lat: s.lat,
              shape_pt_lon: s.lon,
              shape_pt_sequence: s.seq,
            }))}
            onChange={(next) => {
              setShapePts(next.map((r: any) => ({
                shape_id: r.shape_id,
                lat: Number(r.shape_pt_lat),
                lon: Number(r.shape_pt_lon),
                seq: Number(r.shape_pt_sequence),
              })));
            }}
            initialPageSize={10}
            onDeleteRow={(row: any) => {
              setShapePts(prev =>
                prev.filter(p => !(p.shape_id === row.shape_id && p.seq === row.shape_pt_sequence))
              );
            }}
            // onAddRow={addShapeRow}
            // addRowLabel="Add shape point"
          />
        </div>
      </div>

      
      {isBusy && (
        <button
          className="busy-fab"
          aria-busy="true"
          title={busyLabel ?? "Working…"}
          style={{
            position: "fixed",
            right: 16,
            bottom: 16,
            background: "#111",
            color: "#fff",
            border: "none",
            borderRadius: 999,
            padding: "10px 14px",
            boxShadow: "0 8px 24px rgba(0,0,0,.18)",
            display: "inline-flex",
            alignItems: "center",
            gap: 8,
            zIndex: 9999,
            opacity: 0.92,
            cursor: "default",
          }}
        >
          <span className="spinner" />
          <span>{busyLabel ?? "Working…"}</span>
        </button>
      )}
    </div>
  );
}