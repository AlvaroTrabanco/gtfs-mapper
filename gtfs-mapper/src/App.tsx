import { unstable_batchedUpdates } from "react-dom";
import { useEffect, useMemo, useState, useCallback, useRef } from "react";
import { MapContainer, TileLayer, Polyline, CircleMarker, useMapEvents, useMap, Pane, Marker } from "react-leaflet";
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

type Issue = { level: "error" | "warning"; file: string; row?: number; message: string };
type Banner = { kind: "success" | "error" | "info"; text: string } | null;

type StopDefaultsMap = Record<string, { 
  mode: "normal" | "pickup" | "dropoff" | "custom";
  dropoffOnlyFrom?: string[];
  pickupOnlyTo?: string[];
}>;


type StopRuleMode = "normal" | "pickup" | "dropoff" | "custom";
type ODRestriction = {
  mode: StopRuleMode;
  dropoffOnlyFrom?: string[];
  pickupOnlyTo?: string[];
};
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
const DEBUG = true;
const log = (...args: any[]) => { if (DEBUG) console.log("[GTFS]", ...args); };

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

/** ---------- Export GTFS (zip, slim + compressed) ---------- */

  
async function exportGTFSZip(payload: {
  agencies: Agency[]; stops: Stop[]; routes: RouteRow[]; services: Service[];
  trips: Trip[]; stopTimes: StopTime[]; shapePts: ShapePt[];
}, opts?: {
  onlyRoutes?: Set<string>;     // if provided, export only these route_ids (and their deps)
  pruneUnused?: boolean;        // default true
  roundCoords?: boolean;        // default true
  decimateShapes?: boolean;     // default true
  maxShapePts?: number;         // hard cap per shape after decimation (default 2000)
}) {
  const {
    onlyRoutes,
    pruneUnused = true,
    roundCoords = true,
    decimateShapes = true,
    maxShapePts = 2000,
  } = (opts ?? {});

  // 1) Start from payload; optionally filter to selected routes
  const baseRoutes = onlyRoutes
    ? payload.routes.filter(r => onlyRoutes.has(r.route_id))
    : payload.routes.slice();

  // Quick maps for lookup
  const tripsByRoute = new Map<string, Trip[]>();
  for (const t of payload.trips) {
    if (!tripsByRoute.has(t.route_id)) tripsByRoute.set(t.route_id, []);
    tripsByRoute.get(t.route_id)!.push(t);
  }

  const stopTimesByTrip = new Map<string, StopTime[]>();
  for (const st of payload.stopTimes) {
    if (!stopTimesByTrip.has(st.trip_id)) stopTimesByTrip.set(st.trip_id, []);
    stopTimesByTrip.get(st.trip_id)!.push(st);
  }

  

  // 2) Build reachability sets from the chosen routes
  const keepRouteIds = new Set(baseRoutes.map(r => r.route_id));
  const keepTripIds  = new Set<string>();
  const keepStopIds  = new Set<string>();
  const keepSvcIds   = new Set<string>();
  const keepShapeIds = new Set<string>();

  for (const r of baseRoutes) {
    const rTrips = tripsByRoute.get(r.route_id) ?? [];
    for (const t of rTrips) {
      keepTripIds.add(t.trip_id);
      if (t.service_id) keepSvcIds.add(t.service_id);
      if (t.shape_id) keepShapeIds.add(t.shape_id);

      const sts = stopTimesByTrip.get(t.trip_id) ?? [];
      for (const st of sts) {
        if (!((st.arrival_time?.trim()) || (st.departure_time?.trim()))) continue; // skip blank rows
        keepStopIds.add(st.stop_id);
      }
    }
  }

  // 3) Prune if requested
  const agenciesOut = payload.agencies.length ? payload.agencies.slice() : [{
    agency_id: "agency_1",
    agency_name: "Agency",
    agency_url: "https://example.com",
    agency_timezone: defaultTZ,
  }];

  const routesOut = baseRoutes;
  const tripsOut  = pruneUnused
    ? payload.trips.filter(t => keepTripIds.has(t.trip_id))
    : payload.trips.slice();

  const stopTimesOut = (pruneUnused
    ? payload.stopTimes.filter(st => keepTripIds.has(st.trip_id))
    : payload.stopTimes.slice()
  ).filter(st => !!(st.arrival_time?.trim() || st.departure_time?.trim())); // also strip fully-blank rows

  const stopsOut = pruneUnused
    ? payload.stops.filter(s => keepStopIds.has(s.stop_id))
    : payload.stops.slice();

  const servicesOut = pruneUnused
    ? payload.services.filter(s => keepSvcIds.has(s.service_id))
    : payload.services.slice();

  // 4) Shapes: filter + shrink
  let shapesOut = pruneUnused
    ? payload.shapePts.filter(p => keepShapeIds.has(p.shape_id))
    : payload.shapePts.slice();

  if (roundCoords) {
    const round5 = (x: number) => Math.round(x * 1e5) / 1e5; // ~1.1 m
    shapesOut = shapesOut.map(p => ({ ...p, lat: round5(p.lat), lon: round5(p.lon) }));
  }

  if (decimateShapes) {
    const groups = new Map<string, ShapePt[]>();
    for (const p of shapesOut) {
      if (!groups.has(p.shape_id)) groups.set(p.shape_id, []);
      groups.get(p.shape_id)!.push(p);
    }
    const slim: ShapePt[] = [];
    for (const [sid, arr] of groups) {
      const sorted = arr.slice().sort((a,b)=>a.seq-b.seq);
      if (sorted.length <= maxShapePts) {
        slim.push(...sorted);
      } else {
        const step = Math.ceil(sorted.length / maxShapePts);
        for (let i = 0; i < sorted.length; i += step) slim.push(sorted[i]);
        const last = sorted[sorted.length - 1];
        const packedLast = slim[slim.length - 1];
        if (!packedLast || packedLast.seq !== last.seq) slim.push(last);
      }
    }
    shapesOut = slim;
  }

  // 5) Build CSVs
  const zip = new JSZip();

  zip.file("agency.txt", csvify(agenciesOut, ["agency_id","agency_name","agency_url","agency_timezone"]));
  zip.file("stops.txt", csvify(
    stopsOut.map(s => ({ stop_id: s.stop_id, stop_name: s.stop_name, stop_lat: s.stop_lat, stop_lon: s.stop_lon })),
    ["stop_id","stop_name","stop_lat","stop_lon"]
  ));
  zip.file("routes.txt", csvify(routesOut, ["route_id","route_short_name","route_long_name","route_type","agency_id"]));
  zip.file("calendar.txt", csvify(servicesOut, ["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"]));
  zip.file("trips.txt", csvify(tripsOut, ["route_id","service_id","trip_id","trip_headsign","shape_id","direction_id"]));
  zip.file("stop_times.txt", csvify(stopTimesOut, ["trip_id","arrival_time","departure_time","stop_id","stop_sequence"]));

  if (shapesOut.length) {
    zip.file("shapes.txt", csvify(
      shapesOut.map(p => ({ shape_id: p.shape_id, shape_pt_lat: p.lat, shape_pt_lon: p.lon, shape_pt_sequence: p.seq })),
      ["shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"]
    ));
  }

  // 6) Compressed ZIP
  const blob = await zip.generateAsync({
    type: "blob",
    compression: "DEFLATE",
    compressionOptions: { level: 9 }
  });
  saveAs(blob, "gtfs.zip");
}

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
}: {
  title: string;
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

  const edit = (globalIndex: number, key: string, v: string) => {
    const next = rows.slice();
    next[globalIndex] = { ...next[globalIndex], [key]: v };
    onChange(next);
  };


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
              onChange={(e) => setQuery(e.target.value)}
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
              {pageIdx.length ? pageIdx.map((gi) => {
                const r = rows[gi];
                const isSelected = (selectedPredicate && r) ? !!selectedPredicate(r) : false;
                return (
                  <tr
                    key={gi}
                    onClick={(e) => onRowClick ? onRowClick(r, e) : undefined}
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
                            onClick={(e) => { e.stopPropagation(); onIconClick && onIconClick(r); }}
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
                    No rows.
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
}: {
  onShow: (info: { x: number; y: number; lat: number; lng: number }) => void;
}) {
  const map = useMap();
  useMapEvents({
    click(e) {
      // Only show the small menu when we are zoomed in enough to act on it
      if (map.getZoom() < MIN_ADD_ZOOM) return;

      const oe = (e as any).originalEvent as MouseEvent | undefined;
      if (oe) {
        if ((oe as any)._stopped || oe.defaultPrevented) return;
        const t = oe.target as HTMLElement | null;
        if (
          t &&
          (t.closest(".leaflet-marker-pane") ||
           t.closest(".leaflet-interactive") ||
           t.closest(".leaflet-popup") ||
           t.closest(".leaflet-tooltip"))
        ) {
          return;
        }
      }
      const p = map.latLngToContainerPoint(e.latlng);
      onShow({ x: p.x, y: p.y, lat: e.latlng.lat, lng: e.latlng.lng });
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



  const handleStopDefaultChange = useCallback((stop_id: string, nextRule: ODRestriction | null) => {
    setProject((prev: any) => {
      const curr: StopDefaultsMap = prev?.extras?.stopDefaults ?? {};
      const next: StopDefaultsMap = { ...curr };
      if (nextRule) next[stop_id] = nextRule;
      else delete next[stop_id];
      return {
        ...(prev ?? {}),
        extras: { ...(prev?.extras ?? {}), stopDefaults: next }
      };
    });
  }, []);

  const getStopDefault = useCallback((stop_id: string): ODRestriction | undefined => {
    return (project?.extras?.stopDefaults ?? {})[stop_id];
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

  /** UI */
  const [nextStopName, setNextStopName] = useState<string>("");
  const [showRoutes, setShowRoutes] = useState<boolean>(true);

  const [showStops, setShowStops] = useState<boolean>(true);

  // scope tables/map to the active route(s)
  const [isScopedView, setIsScopedView] = useState<boolean>(true);
  
  const [mapClickMenu, setMapClickMenu] = useState<{
    x: number; y: number; lat: number; lng: number;
  } | null>(null);

const addStopAt = useCallback((lat: number, lng: number) => {
  const base = nextStopName?.trim() || "Stop";
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
}, [nextStopName]);

const handleAddStopAtCenter = () => {
  if (!mapCenter) return;
  addStopAt(mapCenter.lat, mapCenter.lng);
};

  // --- Export options (UI) ---
  const [exportOnlySelectedRoutes, setExportOnlySelectedRoutes] = useState<boolean>(false);
  const [exportPruneUnused, setExportPruneUnused] = useState<boolean>(true);
  const [exportRoundCoords, setExportRoundCoords] = useState<boolean>(true);
  const [exportDecimateShapes, setExportDecimateShapes] = useState<boolean>(true);

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
    // don’t do anything during bulk compute suppression
    if (suppressCompute.current) return;

    const rid = resolveActiveRouteId();
    if (!rid) {
      // No route selected → keep the stop_times list small/empty
      if (stopTimes.length) setStopTimes([]);
      return;
    }

    const all = stopTimesAllRef.current;
    if (!all || !all.length) return;

    // Trips for this route
    const tids = new Set(trips.filter(t => t.route_id === rid).map(t => t.trip_id));
    if (!tids.size) {
      if (stopTimes.length) setStopTimes([]);
      return;
    }

    // Only rows for those trips
    const filtered = all.filter(st => tids.has(st.trip_id));
    setStopTimes(filtered);
  }, [selectedRouteId, selectedRouteIds, trips]);


  const [selectedStopId, setSelectedStopId] = useState<string | null>(null);

  const [selectedStopTime, setSelectedStopTime] =
    useState<{ trip_id: string; stop_sequence: number } | null>(null);

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

  // Stable callback so MapStateTracker's effect doesn't re-run forever
  // Stable callback so MapStateTracker's effect doesn't re-run forever
  const [mapCenter, setMapCenter] = useState<L.LatLng | null>(null);

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

  // All stop_times live here after import; we only render the subset for the selected route.
  const stopTimesAllRef = useRef<StopTime[] | null>(null);



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

    // If scoped view is on, filter the base stops by keepStopIds; else use all stops
    const base = scopedKeep
      ? stops.filter(s => scopedKeep.keepStopIds.has(s.stop_id))
      : stops;

    return base.filter(s => b.contains(L.latLng(s.stop_lat, s.stop_lon)));
  }, [showStops, mapBounds, mapZoom, stops, scopedKeep]);




  /** Filters / clearing */
  const [activeServiceIds, setActiveServiceIds] = useState<Set<string>>(new Set());
  const [clearSignal, setClearSignal] = useState(0);

  /** Validation & banner */
  const [validation, setValidation] = useState<{ errors: Issue[]; warnings: Issue[] }>({ errors: [], warnings: [] });
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
    const { targetTrip, afterSeq } = pickTargetTripAndAfterSeq();
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
  const base = nextStopName?.trim() || "Stop";
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

        const savedStopDefaults: StopDefaultsMap =
          obj.project?.extras?.stopDefaults ??
          obj.extras?.stopDefaults ??
          {};

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
  const exportProject = () => {
    const blob = new Blob([JSON.stringify({ agencies, stops, routes, services, trips, stopTimes, shapePts }, null, 2)], { type: "application/json" });
    saveAs(blob, "project.json");
  };
  const importProject = async (file: File) =>
    withBusy("Loading project…", async () => {
      try {
        const obj = JSON.parse(await file.text());

        suppressPersist.current = true;

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
            stopDefaults: (
              obj.project?.extras?.stopDefaults ??
              obj.extras?.stopDefaults ??
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
      // Accept both { rules: { "trip::stop": {...} } } and a raw map
      const rules: RestrictionsMap = (json?.rules ?? json) as RestrictionsMap;

      // Build the set of (trip_id::stop_id) pairs present in the loaded feed
      const presentPairs = new Set(stopTimes.map(st => `${st.trip_id}::${st.stop_id}`));

      // Start from current restrictions (so we merge, don’t blow away user edits)
      const current: RestrictionsMap = (project?.extras?.restrictions ?? {}) as RestrictionsMap;
      const next: RestrictionsMap = { ...current };

      let total = 0;
      let matched = 0;
      let skipped = 0;

      for (const [key, rawRule] of Object.entries(rules)) {
        total++;
        if (presentPairs.has(key)) {
          next[key] = normalizeRule(rawRule);
          matched++;
        } else {
          skipped++;
        }
      }

      setProject((prev: any) => ({
        ...(prev ?? {}),
        extras: { ...(prev?.extras ?? {}), restrictions: next },
      }));

      setBanner({
        kind: "success",
        text: `Overrides: applied ${matched}/${total}${skipped ? ` (${skipped} unmatched)` : ""}.`,
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
        // During huge imports, don't render thousands of markers
        setShowStops(false);

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

  const stopsById = useMemo(() => {
    const m = new Map<string, Stop>();
    for (const s of stops) m.set(s.stop_id, s);
    return m;
  }, [stops]);

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

  const tripsByRoute = useMemo(() => {
    const m = new Map<string, Trip[]>();
    for (const t of trips) {
      const arr = m.get(t.route_id) ?? [];
      arr.push(t);
      m.set(t.route_id, arr);
    }
    return m;
  }, [trips]);

  const routePolylines = useMemo(() => {
    // ⬇️ Skip when we’re importing or computing disabled
    if (suppressCompute.current) return new Map<string, [number, number][]>();

    if (isBusy || !mapBounds || mapZoom < MIN_ROUTE_ZOOM)
      return new Map<string, [number, number][]>();

    const out = new Map<string, [number, number][]>();
    const b = mapBounds;
    let routesDrawn = 0;

    // Helper: keep at most MAX_ROUTE_POINTS by picking every k-th point
    const decimate = (coords: [number, number][]) => {
      const n = coords.length;
      if (n <= MAX_ROUTE_POINTS) return coords;
      const step = Math.ceil(n / MAX_ROUTE_POINTS);
      const slim: [number, number][] = [];
      for (let i = 0; i < n; i += step) slim.push(coords[i]);
      // ensure last point is included
      if (slim[slim.length - 1] !== coords[n - 1]) slim.push(coords[n - 1]);
      return slim;
    };

    for (const r of routes) {
      const rTrips = tripsByRoute.get(r.route_id) ?? [];

      // Pick the longest available SHAPE
      let coords: [number, number][] | null = null;
      for (const t of rTrips) {
        const sid = t.shape_id;
        if (!sid) continue;
        const pts = shapesById.get(sid);
        if (!pts || pts.length < 2) continue;
        const c = pts.map(p => [p.lat, p.lon] as [number, number]);
        if (!coords || c.length > coords.length) coords = c;
      }

      // If there’s still no geometry, do not render this route.
      if (!coords || coords.length < 2) {
        if (DEBUG && selectedRouteId === r.route_id) {
          log("routePolylines: NO coords for selected route", r.route_id);
        }
        continue;
      }

      // Quick bounds check: keep the route only if any segment lies in (or near) the current view.
      // We’ll do a cheap test by checking whether at least one vertex is inside bounds.
      const anyInside = (() => {
        for (let i=0;i<coords.length;i++) {
          const [lat, lon] = coords[i];
          if (b.contains(L.latLng(lat, lon))) return true;
          if (i > 0) {
            const [lat0, lon0] = coords[i-1];
            const segBounds = L.latLngBounds(L.latLng(lat0, lon0), L.latLng(lat, lon));
            if (segBounds.intersects(b)) return true;
          }
        }
        return false;
      })();
      if (!anyInside) continue;

      out.set(r.route_id, decimate(coords));
      routesDrawn++;
    }
    if (DEBUG) log("routePolylines recomputed", {
      routesTotal: routes.length,
      routesDrawn,
      shapesKnown: shapesById.size
    });
    return out;
    // NOTE we now depend on mapBounds/mapZoom to cull by view
  }, [isBusy, mapBounds, mapZoom, routes, tripsByRoute, shapesById, stopTimesByTrip, stopsById]);

  /** ---------- Selection & ordering ---------- */
  // Visual: selected route first
  const routesVisibleIdx = useMemo(() => {
    // IMPORTANT: base must match what you pass as rows to the table
    const base = (isScopedView && scopedKeep) ? routesScoped : routes;

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

  /** ---------- Validation ---------- */
  function validateFeed(ctx: {
    agencies: Agency[]; stops: Stop[]; routes: RouteRow[]; services: Service[];
    trips: Trip[]; stopTimes: StopTime[]; shapePts: ShapePt[];
  }) {
    const errors: Issue[] = [];
    const warnings: Issue[] = [];

    const req = <T, K extends keyof T>(rows: T[], key: K, file: string) => {
      rows.forEach((r, i) => { const v = (r as any)[key]; if (v === undefined || v === null || v === "") errors.push({ level: "error", file, row: i + 1, message: `Missing ${String(key)}` }); });
    };
    req(ctx.agencies, "agency_id", "agency.txt");
    req(ctx.stops, "stop_id", "stops.txt");
    req(ctx.routes, "route_id", "routes.txt");
    req(ctx.services, "service_id", "calendar.txt");
    req(ctx.trips, "trip_id", "trips.txt");
    req(ctx.stopTimes, "trip_id", "stop_times.txt");

    const dup = <T, K extends keyof T>(rows: T[], key: K, file: string) => {
      const seen = new Set<any>();
      rows.forEach((r, i) => { const k = (r as any)[key]; if (seen.has(k)) errors.push({ level: "error", file, row: i + 1, message: `Duplicate ${String(key)}: ${k}` }); seen.add(k); });
    };
    dup(ctx.stops, "stop_id", "stops.txt");
    dup(ctx.routes, "route_id", "routes.txt");
    dup(ctx.trips, "trip_id", "trips.txt");
    dup(ctx.services, "service_id", "calendar.txt");

    return { errors, warnings };
  }
  const runValidation = () => {
    const res = validateFeed({ agencies, stops, routes, services, trips, stopTimes, shapePts });
    setValidation(res);
    setBanner(res.errors.length ? { kind: "error", text: `Validation found ${res.errors.length} errors and ${res.warnings.length} warnings.` }
                                : { kind: "success", text: res.warnings.length ? `Validation OK with ${res.warnings.length} warnings.` : "Validation OK." });
    setTimeout(() => setBanner(null), 3200);
    return res;
  };
  async function ensureAllStopTimesForExport() {
    const all = stopTimesAllRef.current;
    if (!all) return; // nothing lazy-loaded
    if (stopTimes.length === all.length) return; // already hydrated
    setStopTimes(all.slice());
    await tick(); // allow React to flush setStopTimes before we read from maps
  }
  const onExportGTFS = async () =>
    withBusy("Preparing GTFS…", async () => {
      const { errors } = runValidation();
      if (errors.length) { alert("Fix validation errors before exporting."); return; }

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

        // You already have this helper in your file
        const rowsByTripForExport = groupStopTimesByTrip(rowsSource);

        await exportGtfsCompiled(
          onlyRoutes,
          {
            roundCoords: exportRoundCoords,
            decimateShapes: exportDecimateShapes,
            maxShapePts: 2000,
          },
          rowsByTripForExport   // ⬅️ pass the FULL grouped map
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
    restrictions: Record<
      string,
      { mode: "normal" | "pickup" | "dropoff" | "custom"; dropoffOnlyFrom?: string[]; pickupOnlyTo?: string[] }
    >,
    rowsByTrip: Map<string, StopTime[]>
  ) {
    const outTrips: Trip[] = [];
    const outStopTimes: StopTime[] = [];

    for (const t of trips) {
      const rows = (rowsByTrip.get(t.trip_id) ?? []).slice();
      if (!rows.length) continue;

      const rulesByIdx = new Map<number, { mode: "normal" | "pickup" | "dropoff" | "custom"; dropoffOnlyFrom?: string[]; pickupOnlyTo?: string[] }>();
      rows.forEach((st, i) => {
        const key = `${t.trip_id}::${st.stop_id}`;
        const r = (restrictions as any)[key];
        if (r && r.mode) rulesByIdx.set(i, r);
      });

      const hasCustom = Array.from(rulesByIdx.values()).some(r => r.mode === "custom");

      if (!hasCustom) {
        outTrips.push({ ...t });
        for (let i = 0; i < rows.length; i++) {
          const st = rows[i];
          const r = rulesByIdx.get(i);
          let pickup_type = 0, drop_off_type = 0;
          if (r?.mode === "pickup")  drop_off_type = 1;
          if (r?.mode === "dropoff") pickup_type  = 1;

          outStopTimes.push({
            trip_id: t.trip_id,
            stop_id: st.stop_id,
            stop_sequence: 0,
            arrival_time: toHHMMSS(st.arrival_time),
            departure_time: toHHMMSS(st.departure_time),
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
      outTrips.push({ ...t, trip_id: upId });
      for (let i = 0; i <= lastC; i++) {
        const st = rows[i];
        const r = rulesByIdx.get(i);
        let pickup_type = 0, drop_off_type = 0;
        if (r?.mode === "pickup")  drop_off_type = 1;
        else if (r?.mode === "dropoff") pickup_type = 1;
        else if (r?.mode === "custom") { pickup_type = 1; drop_off_type = 0; }
        outStopTimes.push({
          trip_id: upId, stop_id: st.stop_id, stop_sequence: 0,
          arrival_time: toHHMMSS(st.arrival_time), departure_time: toHHMMSS(st.departure_time),
          pickup_type, drop_off_type
        });
      }

      const downId = `${t.trip_id}__segB`;
      outTrips.push({ ...t, trip_id: downId });
      for (let i = firstC; i < rows.length; i++) {
        const st = rows[i];
        const r = rulesByIdx.get(i);
        let pickup_type = 0, drop_off_type = 0;
        if (r?.mode === "pickup")  drop_off_type = 1;
        else if (r?.mode === "dropoff") pickup_type = 1;
        else if (r?.mode === "custom") { pickup_type = 0; drop_off_type = 1; }
        outStopTimes.push({
          trip_id: downId, stop_id: st.stop_id, stop_sequence: 0,
          arrival_time: toHHMMSS(st.arrival_time), departure_time: toHHMMSS(st.departure_time),
          pickup_type, drop_off_type
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

/** Clear shape points for the selected route’s shape_id */
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
  return (
    <div className="container" style={{ padding: 16 }}>
      <style>{`.busy-fab {
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
        .route-halo-pulse { animation: haloPulse 1.8s ease-in-out infinite !important; filter: drop-shadow(0 0 4px rgba(255,255,255,0.9)); stroke: #ffffff !important; stroke-linecap: round !important; }
        @keyframes haloPulse { 0%{stroke-opacity:.65;stroke-width:10px;} 50%{stroke-opacity:.2;stroke-width:16px;} 100%{stroke-opacity:.65;stroke-width:10px;} }
      `}</style>

      <h1 style={{ fontSize: '15px'}}>GTFS Builder by Álvaro Trabanco for Rome2Rio</h1>

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
        <div className="card-body" style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
          

        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <button
            type="button"
            className="btn"
            disabled={mapZoom < MIN_ADD_ZOOM || !mapCenter}
            onClick={handleAddStopAtCenter}
          >
            Add stop (map center)
          </button>
          {(mapZoom < MIN_ADD_ZOOM || !mapCenter) && (
            <span style={{ fontSize: 11, opacity: 0.7 }}>zoom in to Add Stops</span>
          )}
        </div>

          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <label>Next stop name:</label>
            <input className="input" value={nextStopName} onChange={e => setNextStopName(e.target.value)} placeholder="optional" />
          </div>

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

          {/* Route & trips quick actions */}
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <button className="btn" onClick={createNewRoute}>New route</button>

            <button
              className="btn"
              disabled={!selectedRouteId}
              title={selectedRouteId ? "Duplicate selected route" : "Select a route first"}
              onClick={duplicateSelectedRoute}
            >
              Duplicate route
            </button>

            <button
              className="btn"
              disabled={!selectedRouteId}
              title={selectedRouteId ? "Add a trip for the selected route" : "Select a route first"}
              onClick={createTripForSelectedRoute}
            >
              New trip (for selected)
            </button>

            
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
            Clear filters & selection
          </button>

          <button className="btn" onClick={exportProject}>Export project JSON</button>

          <label className="file-btn">
            Import project JSON
            <input type="file" accept="application/json" style={{ display: "none" }}
              onChange={e => { const f = e.target.files?.[0]; if (f) importProject(f); }} />
          </label>

          <label className="file-btn">
            Import overrides.json
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

          <button className="btn btn-primary" onClick={onExportGTFS}>Export GTFS .zip</button>

                    {/* Export options */}
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap", background: "#f7f8fa", padding: "6px 10px", borderRadius: 10 }}>
            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input type="checkbox" checked={exportOnlySelectedRoutes} onChange={e=>setExportOnlySelectedRoutes(e.target.checked)} />
              Only selected route(s)
            </label>
            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input type="checkbox" checked={exportPruneUnused} onChange={e=>setExportPruneUnused(e.target.checked)} />
              Prune unused rows
            </label>
            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input type="checkbox" checked={exportRoundCoords} onChange={e=>setExportRoundCoords(e.target.checked)} />
              Round shape coords
            </label>
            <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input type="checkbox" checked={exportDecimateShapes} onChange={e=>setExportDecimateShapes(e.target.checked)} />
              Decimate long shapes
            </label>
          </div>

          <button
            className="btn"
            onClick={() => {
              // grab ALL rules (not just selected route)
              const allRules = (project?.extras?.restrictions ?? {}) as Record<string, any>;

              // keep only non-"normal" (i.e., where you actually set pickup/dropoff/custom)
              const pruned: Record<string, any> = {};
              for (const [k, v] of Object.entries(allRules)) {
                if (!v) continue;
                if (v.mode && v.mode !== "normal") pruned[k] = v;
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

          <button
            className="btn"
            onClick={() =>
              withBusy("Validating…", async () => {
                const res = runValidation();
                setValidation(res);
                setBanner(
                  res.errors.length
                    ? { kind: "error", text: `Validation found ${res.errors.length} errors and ${res.warnings.length} warnings.` }
                    : { kind: "success", text: res.warnings.length ? `Validation OK with ${res.warnings.length} warnings.` : "Validation OK." }
                );
                setTimeout(() => setBanner(null), 3200);
              })
            }
          >
            Validate
          </button>

          <button className="btn btn-danger" onClick={resetAll}>Reset</button>
        </div>
      </div>

      {/* Map */}
      <div className="card section">
        <div className="card-body">
          <div className={`map-shell ${selectedRouteId ? "bw" : ""}`} style={{ height: 520, width: "100%", borderRadius: 12, overflow: "hidden", position: "relative" }}>
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
            <MapContainer
              center={[40.4168, -3.7038]}
              zoom={6}
              preferCanvas={true}
              style={{ height: "100%", width: "100%" }}
            >
              {!selectedStopId && !selectedRouteId && (
                <AddStopOnMapClick
                  onAdd={(lat, lng) => addStopAt(lat, lng)}
                  onTooFar={() =>
                    setBanner({
                      kind: "info",
                      text: `Zoom in to at least ${MIN_ADD_ZOOM} to add stops.`,
                    })
                  }
                />
              )}

              {(selectedStopId || selectedRouteId) && (
                <MapClickMenuTrigger
                  onShow={({ x, y, lat, lng }) => setMapClickMenu({ x, y, lat, lng })}
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
              <Pane name="routeHalo" style={{ zIndex: 390 }} />
              <Pane name="routeLines" style={{ zIndex: 400 }} />
              <Pane name="stopsTop" style={{ zIndex: 650 }} />

             


              {/* Normal stops — not draggable while in Add mode */}
              {visibleStops.map((s) => (
                <CircleMarker
                  key={s.uid}
                  center={[s.stop_lat, s.stop_lon]}
                  pane="stopsTop"
                  radius={selectedRouteId ? 7 : 6} // you can also make them slightly bigger here
                  color={selectedStopId === s.stop_id ? "#e11d48" : "#111"}
                  weight={1.5}
                  fillColor="#fff"
                  fillOpacity={1}
                  eventHandlers={{
                    click: (e) => {
                      if (isBusy) return;
                      if ((e as any).originalEvent) L.DomEvent.stop((e as any).originalEvent);
                      setMapClickMenu(null);        // ← close popup when switching stops
                      setSelectedStopId(s.stop_id);
                    },
                  }}
                />
              ))}


              {/* Route polylines */}
              {!tooManyRoutes &&
                Array.from(routePolylines.entries())
                  .filter(([route_id]) => {
                    if (!isScopedView) return true;
                    if (selectedRouteIds.size) return selectedRouteIds.has(route_id);
                    return selectedRouteId ? route_id === selectedRouteId : true;
                  })
                  .map(([route_id, coords]) => {
                  const isSel =
                    selectedRouteId === route_id || selectedRouteIds.has(route_id);
                  const hasSel = !!selectedRouteId || selectedRouteIds.size > 0;
                  const color = hasSel
                    ? isSel
                      ? routeColor(route_id)
                      : DIM_ROUTE_COLOR
                    : routeColor(route_id);
                  const weight = isSel ? 6 : 3;
                  const opacity = hasSel ? (isSel ? 0.95 : 0.6) : 0.95;

                  return (
                    <div key={route_id}>
                      {isSel && (
                        <Polyline
                          positions={coords as any}
                          pane="routeHalo"
                          className="route-halo-pulse"
                          pathOptions={{
                            color: "#ffffff",
                            weight: 10,
                            opacity: 0.9,
                            lineCap: "round",
                          }}
                          eventHandlers={{
                            click: () => {
                              if (isBusy) return;
                              setSelectedRouteId(route_id);
                            },
                          }}
                        />
                      )}
                      <Polyline
                        positions={coords as any}
                        pane="routeLines"
                        pathOptions={{ color, weight, opacity }}
                        eventHandlers={{
                          click: () => {
                            if (isBusy) return;
                            setSelectedRouteId(route_id);
                          },
                        }}
                      />
                    </div>
                  );
                })}
                
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
                // prevent clicks from bubbling to Leaflet
                onMouseDown={(e) => e.stopPropagation()}
                onClick={(e) => e.stopPropagation()}
              >
                {/* Only show 'Move here' when a stop is selected */}
                {selectedStopId && (
                    <button
                      className="btn"
                      onClick={() => {
                        setSelectedStopId(null);                    // deselect current
                        addStopAt(mapClickMenu.lat, mapClickMenu.lng); // add & (inside addStopAt) select new one
                        setMapClickMenu(null);
                      }}
                    >
                      Add stop
                    </button>
                  )}

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
          trips={(tripsByRoute.get(selectedRouteId) ?? [])}
          stopTimes={stopTimes.filter(st =>
            (tripsByRoute.get(selectedRouteId) ?? []).some(t => t.trip_id === st.trip_id)
          )}
          selectedRouteId={routes.some(r => r.route_id === selectedRouteId) ? selectedRouteId : null}
          initialRestrictions={(project?.extras?.restrictions as RestrictionsMap) ?? EMPTY_OBJ}
          initialStopDefaults={(project?.extras?.stopDefaults as StopDefaultsMap) ?? EMPTY_OBJ}
          onRestrictionsChange={handleRestrictionsChange}
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
          {tripIdsForStopTimes.length ? (() => {
            // limit how many trip sections render at once
            const tripIdsPage = tripIdsForStopTimes.slice(0, MAX_TRIP_GROUPS_RENDERED);

            return (
              <>
                {tripIdsPage.map((trip_id) => {
                  const rows = stopTimes
                    .filter((st) => st.trip_id === trip_id)
                    .sort((a, b) => num(a.stop_sequence) - num(b.stop_sequence));

                  return (
                    <div key={trip_id} className="card section" style={{ marginTop: 10 }}>
                      <div className="card-body">
                        <div
                          style={{
                            display: "flex",
                            alignItems: "baseline",
                            justifyContent: "space-between",
                            gap: 12,
                          }}
                        >
                          <h3 style={{ margin: 0 }}>
                            stop_times.txt{" "}
                            <span style={{ opacity: 0.6, fontWeight: 400 }}>
                              — trip_id: <code>{trip_id}</code> ({rows.length} rows)
                            </span>
                          </h3>

                          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                            <button className="btn" onClick={addBlankStopTimeRow}>
                              Add blank row
                            </button>

                            <input
                              value={existingStopToAdd}
                              onChange={(e) => setExistingStopToAdd(e.target.value)}
                              placeholder="Choose a stop…"
                              list="gtfs-stop-ids"
                              className="input"
                              style={{ minWidth: 260 }}
                            />
                            <button className="btn btn-primary" onClick={() => confirmAddExistingStop(trip_id)}>
                              Add
                            </button>
                          </div>
                        </div>

                        <div
                          className="overflow-auto"
                          style={{ borderRadius: 12, border: "1px solid #eee", marginTop: 8 }}
                        >
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
                              {rows.map((r, idx) => {
                                const globalKey = `${r.trip_id}::${r.stop_id}::${r.stop_sequence}::${idx}`;
                                const stopName = stopIdToName(r.stop_id);
                                const bad = badTimeKeys.has(`${r.trip_id}::${r.stop_sequence}`);
                                const badInputStyle = bad
                                  ? { border: "1px solid #e11d48", background: "#fee2e2" }
                                  : {};

                                return (
                                  <tr
                                    key={globalKey}
                                    draggable
                                    onDragStart={() => setDragInfo({ trip_id, from: idx })}
                                    onDragOver={(e) => {
                                      if (dragInfo?.trip_id === trip_id) e.preventDefault();
                                    }}
                                    onDrop={(e) => {
                                      e.preventDefault();
                                      if (dragInfo?.trip_id === trip_id)
                                        moveRowWithinTrip(trip_id, dragInfo.from, idx);
                                      setDragInfo(null);
                                    }}
                                    style={{
                                      cursor: "grab",
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
                                    onClick={() =>
                                      setSelectedStopTime({
                                        trip_id,
                                        stop_sequence: r.stop_sequence,
                                      })
                                    }
                                  >
                                    {/* drag handle */}
                                    <td
                                      style={{
                                        borderBottom: "1px solid #f3f3f3",
                                        padding: 4,
                                        textAlign: "center",
                                      }}
                                      title="Drag to reorder"
                                    >
                                      ↕
                                    </td>

                                    {/* stop_sequence */}
                                    <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                      <input
                                        value={r.stop_sequence}
                                        readOnly
                                        style={{
                                          width: 80,
                                          border: "1px solid #e8e8e8",
                                          padding: "4px 6px",
                                          borderRadius: 8,
                                          background: "#f8f9fb",
                                        }}
                                      />
                                    </td>

                                    {/* stop_id */}
                                    <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                      <input
                                        value={r.stop_id}
                                        list="gtfs-stop-ids"
                                        onChange={(e) => {
                                          const sidOrName = e.target.value;
                                          const sid = stopNameToId.get(sidOrName) ?? sidOrName;
                                          setStopTimes((prev) =>
                                            prev.map((st) =>
                                              st.trip_id === r.trip_id &&
                                              st.stop_sequence === r.stop_sequence
                                                ? { ...st, stop_id: sid }
                                                : st
                                            )
                                          );
                                        }}
                                        style={{
                                          width: 180,
                                          border: "1px solid #e8e8e8",
                                          padding: "4px 6px",
                                          borderRadius: 8,
                                        }}
                                      />
                                    </td>

                                    {/* stop_name (mirror) */}
                                    <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                      <input
                                        value={stopName}
                                        readOnly
                                        style={{
                                          width: 200,
                                          border: "1px solid #e8e8e8",
                                          padding: "4px 6px",
                                          borderRadius: 8,
                                          background: "#f8f9fb",
                                        }}
                                      />
                                    </td>

                                    {/* arrival_time */}
                                    <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                      <input
                                        value={r.arrival_time ?? ""}
                                        onChange={(e) => {
                                          const v = e.target.value;
                                          setStopTimes((prev) =>
                                            prev.map((st) =>
                                              st.trip_id === r.trip_id &&
                                              st.stop_sequence === r.stop_sequence
                                                ? { ...st, arrival_time: v }
                                                : st
                                            )
                                          );
                                        }}
                                        placeholder="HH:MM:SS"
                                        style={{
                                          width: 130,
                                          padding: "4px 6px",
                                          borderRadius: 8,
                                          border: "1px solid #e8e8e8",
                                          ...badInputStyle,
                                        }}
                                      />
                                    </td>

                                    {/* departure_time */}
                                    <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                      <input
                                        value={r.departure_time ?? ""}
                                        onChange={(e) => {
                                          const v = e.target.value;
                                          setStopTimes((prev) =>
                                            prev.map((st) =>
                                              st.trip_id === r.trip_id &&
                                              st.stop_sequence === r.stop_sequence
                                                ? { ...st, departure_time: v }
                                                : st
                                            )
                                          );
                                        }}
                                        placeholder="HH:MM:SS"
                                        style={{
                                          width: 130,
                                          padding: "4px 6px",
                                          borderRadius: 8,
                                          border: "1px solid #e8e8e8",
                                          ...badInputStyle,
                                        }}
                                      />
                                    </td>

                                    {/* pickup_type */}
                                    <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                      <input
                                        value={r.pickup_type ?? 0}
                                        onChange={(e) => {
                                          const v =
                                            e.target.value === "" ? undefined : Number(e.target.value);
                                          setStopTimes((prev) =>
                                            prev.map((st) =>
                                              st.trip_id === r.trip_id &&
                                              st.stop_sequence === r.stop_sequence
                                                ? { ...st, pickup_type: v }
                                                : st
                                            )
                                          );
                                        }}
                                        style={{
                                          width: 90,
                                          border: "1px solid #e8e8e8",
                                          padding: "4px 6px",
                                          borderRadius: 8,
                                        }}
                                      />
                                    </td>

                                    {/* drop_off_type */}
                                    <td style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                                      <input
                                        value={r.drop_off_type ?? 0}
                                        onChange={(e) => {
                                          const v =
                                            e.target.value === "" ? undefined : Number(e.target.value);
                                          setStopTimes((prev) =>
                                            prev.map((st) =>
                                              st.trip_id === r.trip_id &&
                                              st.stop_sequence === r.stop_sequence
                                                ? { ...st, drop_off_type: v }
                                                : st
                                            )
                                          );
                                        }}
                                        style={{
                                          width: 110,
                                          border: "1px solid #e8e8e8",
                                          padding: "4px 6px",
                                          borderRadius: 8,
                                        }}
                                      />
                                    </td>

                                    {/* delete */}
                                    <td
                                      style={{
                                        borderBottom: "1px solid #f3f3f3",
                                        padding: 0,
                                        textAlign: "center",
                                      }}
                                    >
                                      <button
                                        title="Delete row"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          setStopTimes((prev) =>
                                            prev.filter(
                                              (st) =>
                                                !(
                                                  st.trip_id === r.trip_id &&
                                                  st.stop_sequence === r.stop_sequence
                                                )
                                            )
                                          );
                                        }}
                                        style={{
                                          border: "none",
                                          background: "transparent",
                                          cursor: "pointer",
                                          width: 28,
                                          height: 28,
                                          lineHeight: "28px",
                                        }}
                                      >
                                        ×
                                      </button>
                                    </td>
                                  </tr>
                                );
                              })}
                              {!rows.length && (
                                <tr>
                                  <td colSpan={9} style={{ padding: 12, opacity: 0.6 }}>
                                    No rows.
                                  </td>
                                </tr>
                              )}
                            </tbody>
                          </table>
                        </div>

                        {selectedStopTime ? (
                          <div className="muted" style={{ marginTop: 6 }}>
                            New rows will be inserted <b>after</b> trip{" "}
                            <code>{selectedStopTime.trip_id}</code>, sequence{" "}
                            <b>{selectedStopTime.stop_sequence}</b>.
                          </div>
                        ) : (
                          <div className="muted" style={{ marginTop: 6 }}>
                            Tip: click a row to insert the next one <b>after</b> it.
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}

                {tripIdsForStopTimes.length > MAX_TRIP_GROUPS_RENDERED && (
                  <div className="muted" style={{ marginTop: 6 }}>
                    Showing first {MAX_TRIP_GROUPS_RENDERED} trips for this route. Narrow by service chips to see fewer.
                  </div>
                )}
              </>
            );
          })() : (
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

      {/* Validation summary */}
      <div className="card section" style={{ marginTop: 12 }}>
        <div className="card-body">
          <h3>Validation</h3>
          <div className="muted" style={{ marginTop: 10 }}>
            <strong>Errors:</strong> {validation.errors.length} &nbsp; | &nbsp;
            <strong>Warnings:</strong> {validation.warnings.length}
          </div>
          {validation.errors.length > 0 && (
            <>
              <h4>Errors</h4>
              <ul>{validation.errors.map((e, i) => <li key={`e${i}`}><code>{e.file}{e.row ? `:${e.row}` : ""}</code> — {e.message}</li>)}</ul>
            </>
          )}
          {validation.warnings.length > 0 && (
            <>
              <h4>Warnings</h4>
              <ul>{validation.warnings.map((w, i) => <li key={`w${i}`}><code>{w.file}{w.row ? `:${w.row}` : ""}</code> — {w.message}</li>)}</ul>
            </>
          )}
          <p className="muted" style={{ marginTop: 8 }}>
            Export skips any <code>stop_times</code> row where both arrival and departure are blank.
          </p>
        </div>
      </div>
      {isBusy && (
        <button className="busy-fab" aria-busy="true" title={busyLabel ?? "Working…"}>
          <span className="spinner" />
          <span>{busyLabel ?? "Working…"}</span>
        </button>
      )}
    </div>
  );
}