import { unstable_batchedUpdates } from "react-dom";
import { useEffect, useMemo, useState, useCallback, useRef } from "react";
import { MapContainer, TileLayer, Polyline, CircleMarker, useMapEvents, useMap, Pane } from "react-leaflet";
import L from "leaflet";
import JSZip from "jszip";
import { saveAs } from "file-saver";
import * as Papa from "papaparse";
import { v4 as uuidv4 } from "uuid";
import PatternMatrix from "./PatternMatrix";




/** ---------- Misc ---------- */
const defaultTZ: string = String(Intl.DateTimeFormat().resolvedOptions().timeZone || "Europe/Madrid");
const MIN_ADD_ZOOM = 15;
const MIN_STOP_ZOOM = 11;
const MIN_ROUTE_ZOOM = 6;      // ← NEW: don’t draw route lines when zoomed way out
const MAX_ROUTE_POINTS = 2000; // ← NEW: cap points per polyline drawn to the screen
const DIM_ROUTE_COLOR = "#2b2b2b";


/** ---------- Types ---------- */
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
type Agency = { agency_id: string; agency_name: string; agency_url: string; agency_timezone: string };

type Issue = { level: "error" | "warning"; file: string; row?: number; message: string };
type Banner = { kind: "success" | "error" | "info"; text: string } | null;


type StopRuleMode = "normal" | "pickup" | "dropoff" | "custom";
type ODRestriction = {
  mode: StopRuleMode;
  dropoffOnlyFrom?: string[];
  pickupOnlyTo?: string[];
};
type RestrictionsMap = Record<string, ODRestriction>;

type StopDefaultsMap = Record<string, ODRestriction>;



/** ---------- Helpers ---------- */
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
  if (!rows || !rows.length) return "";
  const headers = headerOrder?.length ? headerOrder : Object.keys(rows[0]);
  const out = [headers.join(",")];
  for (const r of rows) out.push(headers.map(h => String(r[h] ?? "")).join(","));
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

/** ---------- Export GTFS (zip) ---------- */
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
function AddStopOnClick({ onAdd, onTooFar }: { onAdd: (latlng: { lat: number; lng: number }) => void; onTooFar: () => void }) {
  const map = useMap();
  useMapEvents({
    click(e) {
      const z = map.getZoom();
      if (z >= MIN_ADD_ZOOM) onAdd(e.latlng);
      else onTooFar();
    }
  });
  return null;
}
function DrawShapeOnClick({ onPoint, onFinish }: { onPoint: (latlng: { lat: number; lng: number }) => void; onFinish: () => void }) {
  const map = useMap();
  useEffect(() => {
    const onClick = (e: any) => onPoint(e.latlng);
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape" || e.key === "Enter") onFinish(); };
    map.on("click", onClick); window.addEventListener("keydown", onKey);
    return () => { map.off("click", onClick); window.removeEventListener("keydown", onKey); };
  }, [map, onPoint, onFinish]);
  return null;
}

function MapStateTracker({ onChange }: { onChange: (z: number, b: L.LatLngBounds) => void }) {
  const map = useMap();

  useEffect(() => {
    const emit = () => onChange(map.getZoom(), map.getBounds());
    emit(); // initial
    map.on("moveend", emit);
    map.on("zoomend", emit);
    return () => {
      map.off("moveend", emit);
      map.off("zoomend", emit);
    };
  }, [map, onChange]); // onChange is memoized (useCallback), so this won't loop

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
function PaginatedEditableTable<T extends Record<string, any>>({
  title, rows, onChange,
  visibleIndex,
  initialPageSize = 5,
  onRowClick,                   // (row, event) => void
  selectedPredicate,
  selectedIcon = "✓",
  clearSignal = 0,
  onIconClick,
  selectOnCellFocus = false,
  onDeleteRow,                  // NEW: show a rightmost (×) per row
  enableMultiSelect = false,    // NEW: allow Cmd/Ctrl additive selection
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
}) {
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
            {title}{" "}
            <span style={{ opacity: .6, fontWeight: 400 }}>
              ({rows.length} rows{filteredIdx.length !== rows.length ? ` | filtered: ${filteredIdx.length}` : ""})
            </span>
          </h3>
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
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
                const isSelected = selectedPredicate ? selectedPredicate(r) : false;
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
                    {cols.map(c => (
                      <td key={c} style={{ borderBottom: "1px solid #f3f3f3", padding: 4 }}>
                        <input
                          value={(r as any)[c] ?? ""}
                          onFocus={selectOnCellFocus && onRowClick ? (e) => onRowClick(r, e as any) : undefined}
                          onChange={e => edit(gi, c, e.target.value)}
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
                <tr><td colSpan={(selectedPredicate ? 1 : 0) + Math.max(1, cols.length) + (onDeleteRow ? 1 : 0)} style={{ padding: 12, opacity: .6 }}>No rows.</td></tr>
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

/** ---------- App ---------- */
export default function App() {
  const [project, setProject] = useState<any>({ extras: { restrictions: {}, stopDefaults: {} as StopDefaultsMap } });

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
  const [drawMode, setDrawMode] = useState(false);
  const [nextStopName, setNextStopName] = useState<string>("");
  const [showRoutes, setShowRoutes] = useState<boolean>(true);

  // --- Export options (UI) ---
  const [exportOnlySelectedRoutes, setExportOnlySelectedRoutes] = useState<boolean>(false);
  const [exportPruneUnused, setExportPruneUnused] = useState<boolean>(true);
  const [exportRoundCoords, setExportRoundCoords] = useState<boolean>(true);
  const [exportDecimateShapes, setExportDecimateShapes] = useState<boolean>(true);

  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null);
  const [selectedRouteIds, setSelectedRouteIds] = useState<Set<string>>(new Set()); // NEW multi-select
  const [selectedStopId, setSelectedStopId] = useState<string | null>(null);


  // Map state for culling heavy layers
  const [mapZoom, setMapZoom] = useState(6);
  const [mapBounds, setMapBounds] = useState<L.LatLngBounds | null>(null);

  // Stable callback so MapStateTracker's effect doesn't re-run forever
  const onMapState = useCallback((z: number, b: L.LatLngBounds) => {
    setMapZoom(prev => (prev === z ? prev : z));
    setMapBounds(prev => (prev && prev.equals(b) ? prev : b));
  }, []);

  // Only draw stops that are in view and when zoomed in enough
  const visibleStops = useMemo(() => {
    if (!mapBounds || mapZoom < MIN_STOP_ZOOM) return [];
    const b = mapBounds;
    return stops.filter(s => b.contains(L.latLng(s.stop_lat, s.stop_lon)));
  }, [stops, mapBounds, mapZoom]);




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

  useEffect(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const obj = JSON.parse(raw);

        setAgencies(obj.agencies ?? []);
        setStops((obj.stops ?? []).map((s: any) => ({ uid: s.uid || uuidv4(), ...s })));
        setRoutes(obj.routes ?? []);
        setServices(obj.services ?? []);
        setTrips(obj.trips ?? []);
        setStopTimes(obj.stopTimes ?? []);
        setShapePts(obj.shapePts ?? []);

        // ⬇️ restore OD rules (supports old/new shapes of the saved object)
        const savedRules =
          obj.project?.extras?.restrictions ??
          obj.extras?.restrictions ??
          obj.restrictions ??
          {};
        const savedStopDefaults: StopDefaultsMap =
          obj.project?.extras?.stopDefaults ??
          obj.extras?.stopDefaults ??
          {};
        setProject((prev: any) => ({
          ...(prev ?? {}),
          extras: {
            ...(prev?.extras ?? {}),
            restrictions: savedRules,
            stopDefaults: savedStopDefaults,
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
  }, [hydrated, agencies, stops, routes, services, trips, stopTimes, shapePts, project?.extras?.restrictions]);

  /** Colors */
  function hashCode(str: string) { let h = 0; for (let i=0;i<str.length;i++) h = ((h<<5)-h) + str.charCodeAt(i) | 0; return h; }
  const PALETTE = ["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf"];
  const routeColor = (routeId: string) => PALETTE[Math.abs(hashCode(routeId)) % PALETTE.length];

  /** Add stop by clicking map */
  const addStopFromMap = (latlng: { lat: number; lng: number }) => {
    const base = nextStopName?.trim() || "Stop";
    const numStr = (stops.length + 1).toString().padStart(3, "0");
    setStops([...stops, { uid: uuidv4(), stop_id: `S_${numStr}`, stop_name: `${base} ${numStr}`, stop_lat: latlng.lat, stop_lon: latlng.lng }]);
  };

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
          extras: { ...(prev?.extras ?? {}), restrictions: savedRules },
        }));

        setBanner({ kind: "success", text: "Project imported." });
        setTimeout(() => setBanner(null), 2200);

        queueMicrotask(() => { suppressPersist.current = false; });
      } catch {
        setBanner({ kind: "error", text: "Invalid project JSON." });
        setTimeout(() => setBanner(null), 3200);
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
        suppressPersist.current = true;

        const zip = await JSZip.loadAsync(file);
        const tables: Record<string, string> = {};
        for (const entry of Object.values(zip.files)) {
          const f = entry as any;
          if (f.dir) continue;
          if (!f.name?.toLowerCase().endsWith(".txt")) continue;
          tables[f.name.replace(/\.txt$/i, "")] = await f.async("string");
        }
        const parse = (text: string) =>
          (Papa.parse(text, { header: true, dynamicTyping: true, skipEmptyLines: true }).data as any[]) || [];

        if (tables["agency"]) {
          setAgencies(parse(tables["agency"]).map((r: any) => ({
            agency_id: String(r.agency_id ?? ""), agency_name: String(r.agency_name ?? ""),
            agency_url: String(r.agency_url ?? ""), agency_timezone: String(r.agency_timezone ?? defaultTZ)
          })));
        }
        if (tables["stops"]) {
          setStops(parse(tables["stops"]).map((r: any) => ({
            uid: uuidv4(),
            stop_id: String(r.stop_id ?? ""), stop_name: String(r.stop_name ?? ""),
            stop_lat: Number(r.stop_lat ?? r.stop_latitude ?? r.lat ?? 0),
            stop_lon: Number(r.stop_lon ?? r.stop_longitude ?? r.lon ?? 0),
          })));
        }
        if (tables["routes"]) {
          setRoutes(parse(tables["routes"]).map((r: any) => ({
            route_id: String(r.route_id ?? ""), route_short_name: String(r.route_short_name ?? ""),
            route_long_name: String(r.route_long_name ?? ""), route_type: Number(r.route_type ?? 3),
            agency_id: String(r.agency_id ?? ""),
          })));
        }
        if (tables["calendar"]) {
          setServices(parse(tables["calendar"]).map((r: any) => ({
            service_id: String(r.service_id ?? ""),
            monday: Number(r.monday ?? 0), tuesday: Number(r.tuesday ?? 0), wednesday: Number(r.wednesday ?? 0),
            thursday: Number(r.thursday ?? 0), friday: Number(r.friday ?? 0), saturday: Number(r.saturday ?? 0), sunday: Number(r.sunday ?? 0),
            start_date: String(r.start_date ?? ""), end_date: String(r.end_date ?? ""),
          })));
        }
        if (tables["trips"]) {
          setTrips(parse(tables["trips"]).map((r: any) => ({
            route_id: String(r.route_id ?? ""),
            service_id: String(r.service_id ?? ""),
            trip_id: String(r.trip_id ?? ""),
            trip_headsign: r.trip_headsign != null ? String(r.trip_headsign) : undefined,
            shape_id: r.shape_id != null ? String(r.shape_id) : undefined,
            direction_id: r.direction_id != null && r.direction_id !== "" ? Number(r.direction_id) : undefined,
          })));
        }
        if (tables["stop_times"]) {
          setStopTimes(parse(tables["stop_times"]).map((r: any) => ({
            trip_id: String(r.trip_id ?? ""),
            arrival_time: String(r.arrival_time ?? ""),
            departure_time: String(r.departure_time ?? ""),
            stop_id: String(r.stop_id ?? ""),
            stop_sequence: Number(r.stop_sequence ?? 0),
            pickup_type: r.pickup_type != null && r.pickup_type !== "" ? Number(r.pickup_type) : undefined,
            drop_off_type: r.drop_off_type != null && r.drop_off_type !== "" ? Number(r.drop_off_type) : undefined,
          })));
        }
        if (tables["shapes"]) {
          setShapePts(parse(tables["shapes"]).map((r: any) => ({
            shape_id: String(r.shape_id ?? ""),
            lat: Number(r.shape_pt_lat ?? r.lat ?? 0), lon: Number(r.shape_pt_lon ?? r.lon ?? 0),
            seq: Number(r.shape_pt_sequence ?? r.seq ?? 0),
          })));
        }

        setBanner({ kind: "success", text: "GTFS zip imported." });
        setTimeout(() => setBanner(null), 2200);

        queueMicrotask(() => { suppressPersist.current = false; });
      } catch (e) {
        console.error(e);
        setBanner({ kind: "error", text: "Failed to import GTFS zip." });
        setTimeout(() => setBanner(null), 3200);
      }
    });

  /** ---------- Derived maps for map drawing ---------- */
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
    // Don’t compute during busy work or when zoomed far out / no bounds yet
    if (isBusy || !mapBounds || mapZoom < MIN_ROUTE_ZOOM) return new Map<string, [number, number][]>();

    const out = new Map<string, [number, number][]>();
    const b = mapBounds;

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
        if (!t.shape_id) continue;
        const pts = shapesById.get(t.shape_id);
        if (!pts || pts.length < 2) continue;
        const c = pts.map(p => [p.lat, p.lon] as [number, number]);
        if (!coords || c.length > coords.length) coords = c;
      }

      // Fallback: derive from the first trip’s stop_times if no shape available
      if (!coords) {
        for (const t of rTrips) {
          const sts = stopTimesByTrip.get(t.trip_id);
          if (!sts || sts.length < 2) continue;
          const c: [number, number][] = [];
          for (const st of sts) {
            const s = stopsById.get(st.stop_id);
            if (s) c.push([s.stop_lat, s.stop_lon]);
          }
          if (c.length >= 2) { coords = c; break; }
        }
      }

      if (!coords || coords.length < 2) continue;

      // Quick bounds check: keep the route only if any segment lies in (or near) the current view.
      // We’ll do a cheap test by checking whether at least one vertex is inside bounds.
      const anyInside = coords.some(([lat, lon]) => b.contains(L.latLng(lat, lon)));
      if (!anyInside) continue;

      out.set(r.route_id, decimate(coords));
    }

    return out;
    // NOTE we now depend on mapBounds/mapZoom to cull by view
  }, [isBusy, mapBounds, mapZoom, routes, tripsByRoute, shapesById, stopTimesByTrip, stopsById]);

  /** ---------- Selection & ordering ---------- */
  // Visual: selected route first
  const routesVisibleIdx = useMemo(() => {
    const idxs = routes.map((_, i) => i);
    if (!selectedRouteId) return idxs;
    const selIdx = routes.findIndex(r => r.route_id === selectedRouteId);
    if (selIdx < 0) return idxs;
    return [selIdx, ...idxs.filter(i => i !== selIdx)];
  }, [routes, selectedRouteId]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") { setSelectedRouteId(null); setSelectedStopId(null); setSelectedRouteIds(new Set()); } };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

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

  const onExportGTFS = async () =>
  withBusy("Preparing GTFS…", async () => {
    const { errors } = runValidation();
    if (errors.length) { alert("Fix validation errors before exporting."); return; }
    await exportGtfsCompiled(); // ← always export the compiled OD version
  });

  const resetAll = () => {
    if (!confirm("Reset project?")) return;
    setAgencies([]); setStops([]); setRoutes([]); setServices([]); setTrips([]); setStopTimes([]); setShapePts([]);
    localStorage.removeItem(STORAGE_KEY);
    setSelectedRouteId(null);
    setSelectedRouteIds(new Set());
    setSelectedStopId(null);
    setActiveServiceIds(new Set());
    setClearSignal((x: number) => x + 1)
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

  /** Delete selected stop (from stops + all stop_times) */
  const deleteSelectedStop = () => {
    if (!selectedStopId) return;
    if (!confirm(`Delete stop ${selectedStopId}? This removes it from stops and all stop_times.`)) return;
    const sid = selectedStopId;
    setStops(prev => prev.filter(s => s.stop_id !== sid));
    setStopTimes(prev => prev.filter(st => st.stop_id !== sid));
    setSelectedStopId(null);
  };

  /** ---------- OD compiler + compiled export ---------- */
  function compileTripsWithOD(
    restrictions: Record<
      string,
      { mode: "normal" | "pickup" | "dropoff" | "custom"; dropoffOnlyFrom?: string[]; pickupOnlyTo?: string[] }
    >
  ) {
    const outTrips: Trip[] = [];
    const outStopTimes: StopTime[] = [];

    for (const t of trips) {
      const rows = (stopTimesByTrip.get(t.trip_id) ?? []).slice();
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
        for (const st of rows) {
          const r = rulesByIdx.get(rows.indexOf(st));
          let pickup_type = 0, drop_off_type = 0;
          if (r?.mode === "pickup")  drop_off_type = 1;
          if (r?.mode === "dropoff") pickup_type  = 1;

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
          });
        }
        continue;
      }

      // Two-segment compilation around custom stops
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
        const arr = toHHMMSS(st.arrival_time);
        const dep = toHHMMSS(st.departure_time);
        if (!arr && !dep) continue;
        outStopTimes.push({
          trip_id: upId, stop_id: st.stop_id, stop_sequence: 0,
          arrival_time: arr, departure_time: dep, pickup_type, drop_off_type
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
        const arr = toHHMMSS(st.arrival_time);
        const dep = toHHMMSS(st.departure_time);
        if (!arr && !dep) continue;
        outStopTimes.push({
          trip_id: downId, stop_id: st.stop_id, stop_sequence: 0,
          arrival_time: arr, departure_time: dep, pickup_type, drop_off_type
        });
      }
    }

    const grouped = new Map<string, StopTime[]>();
    for (const st of outStopTimes) {
      (grouped.get(st.trip_id) ?? grouped.set(st.trip_id, []).get(st.trip_id)!).push(st);
    }
    const finalStopTimes: StopTime[] = [];
    for (const [, arr] of grouped) {
      arr.forEach((st, i) => (st.stop_sequence = i + 1));
      finalStopTimes.push(...arr);
    }

    return { trips: outTrips, stop_times: finalStopTimes };
  }

  async function exportGtfsCompiled() {
    const zip = new JSZip();

    const agenciesOut = agencies.length ? agencies : [{
      agency_id: "agency_1",
      agency_name: "Agency",
      agency_url: "https://example.com",
      agency_timezone: defaultTZ,
    }];
    zip.file("agency.txt", csvify(agenciesOut, ["agency_id","agency_name","agency_url","agency_timezone"]));

    zip.file("stops.txt", csvify(
      stops.map(s => ({ stop_id: s.stop_id, stop_name: s.stop_name, stop_lat: s.stop_lat, stop_lon: s.stop_lon })),
      ["stop_id","stop_name","stop_lat","stop_lon"]
    ));

    zip.file("routes.txt", csvify(
      routes.map(r => ({
        route_id: r.route_id,
        route_short_name: r.route_short_name,
        route_long_name: r.route_long_name,
        route_type: r.route_type,
        agency_id: r.agency_id || agenciesOut[0].agency_id,
      })),
      ["route_id","route_short_name","route_long_name","route_type","agency_id"]
    ));

    zip.file("calendar.txt", csvify(
      services,
      ["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"]
    ));

    if (shapePts.length) {
      zip.file("shapes.txt", csvify(
        shapePts.map(p => ({ shape_id: p.shape_id, shape_pt_lat: p.lat, shape_pt_lon: p.lon, shape_pt_sequence: p.seq })),
        ["shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"]
      ));
    }

    const restrictions = (project?.extras?.restrictions ?? {}) as Record<string, any>;
    const { trips: outTrips, stop_times: outStopTimes } = compileTripsWithOD(restrictions);

    zip.file("trips.txt", csvify(
      outTrips.map(tr => ({
        route_id: tr.route_id,
        service_id: tr.service_id,
        trip_id: tr.trip_id,
        trip_headsign: tr.trip_headsign ?? "",
        shape_id: tr.shape_id ?? "",
        direction_id: tr.direction_id ?? "",
      })),
      ["route_id","service_id","trip_id","trip_headsign","shape_id","direction_id"]
    ));

    zip.file("stop_times.txt", csvify(
      outStopTimes.map(st => ({
        trip_id: st.trip_id,
        arrival_time: toHHMMSS(st.arrival_time),
        departure_time: toHHMMSS(st.departure_time),
        stop_id: st.stop_id,
        stop_sequence: st.stop_sequence,
        pickup_type: st.pickup_type ?? 0,
        drop_off_type: st.drop_off_type ?? 0,
      })),
      ["trip_id","arrival_time","departure_time","stop_id","stop_sequence","pickup_type","drop_off_type"]
    ));

    const blob = await zip.generateAsync({ type: "blob" });
    saveAs(blob, "gtfs_compiled.zip");
  }

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

      {/* Toolbar */}
      <div className="card section" style={{ marginBottom: 12 }}>
        <div className="card-body" style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <label>Next stop name:</label>
            <input className="input" value={nextStopName} onChange={e => setNextStopName(e.target.value)} placeholder="optional" />
          </div>

          <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <input type="checkbox" checked={showRoutes} onChange={e => setShowRoutes(e.target.checked)} />
            Show routes
          </label>

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
          <div className={`map-shell ${selectedRouteId ? "bw" : ""} ${drawMode ? "is-drawing" : ""}`} style={{ height: 520, width: "100%", borderRadius: 12, overflow: "hidden", position: "relative" }}>
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
            <MapContainer center={[40.4168, -3.7038]} zoom={6} preferCanvas={true} style={{ height: "100%", width: "100%" }}>
              <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" attribution="© OpenStreetMap contributors" />
              <Pane name="routeHalo" style={{ zIndex: 390 }} />
              <Pane name="routeLines" style={{ zIndex: 400 }} />
              <Pane name="stopsTop"  style={{ zIndex: 650 }} />
              <MapStateTracker onChange={onMapState} />

              {!drawMode && <AddStopOnClick onAdd={addStopFromMap} onTooFar={() => setBanner({ kind: "info", text: `Zoom in to at least ${MIN_ADD_ZOOM} to add stops.` })} />}
              {drawMode && <DrawShapeOnClick onPoint={() => {}} onFinish={() => setDrawMode(false)} />}

              {visibleStops.map(s => (
                <CircleMarker
                  key={s.uid}
                  center={[s.stop_lat, s.stop_lon]}
                  pane="stopsTop"
                  radius={selectedRouteId ? 3.5 : 3}
                  color={selectedStopId === s.stop_id ? "#e11d48" : "#111"}
                  weight={1.5}
                  fillColor="#fff"
                  fillOpacity={1}
                  eventHandlers={{ click: () => { if (isBusy) return; setSelectedStopId(s.stop_id); } }}
                />
              ))}

              {!tooManyRoutes && Array.from(routePolylines.entries()).map(([route_id, coords]) => {
                const isSel = selectedRouteId === route_id || selectedRouteIds.has(route_id);
                const hasSel = !!selectedRouteId || selectedRouteIds.size > 0;
                const color   = hasSel ? (isSel ? routeColor(route_id) : DIM_ROUTE_COLOR) : routeColor(route_id);
                const weight  = isSel ? 6 : 3;
                const opacity = hasSel ? (isSel ? 0.95 : 0.6) : 0.95;

                return (
                  <div key={route_id}>
                    {isSel && (
                      <Polyline
                        positions={coords as any}
                        pane="routeHalo"
                        className="route-halo-pulse"
                        pathOptions={{ color: "#ffffff", weight: 10, opacity: 0.9, lineCap: "round" }}
                        eventHandlers={{ click: () => { if (isBusy) return; setSelectedRouteId(route_id); } }}
                      />
                    )}
                    <Polyline
                      positions={coords as any}
                      pane="routeLines"
                      pathOptions={{ color, weight, opacity }}
                      eventHandlers={{ click: () => { if (isBusy) return; setSelectedRouteId(route_id); } }}
                    />
                  </div>
                );
              })}
            </MapContainer>
          </div>
        </div>
      </div>

      {/* routes.txt — multi-select + delete button + tiny full-row selection */}
      <PaginatedEditableTable
        title="routes.txt"
        rows={routes}
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
      />

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
          initialRestrictions={project?.extras?.restrictions ?? {}}
          onRestrictionsChange={handleRestrictionsChange}
          initialStopDefaults={project?.extras?.stopDefaults ?? {}}
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
        <PaginatedEditableTable
          title="agency.txt"
          rows={agencies}
          onChange={setAgencies}
          initialPageSize={5}
          onDeleteRow={(row) => setAgencies(prev => prev.filter(a => a !== row))}
        />
        <PaginatedEditableTable
          title="stops.txt"
          rows={stops.map(s => ({ stop_id: s.stop_id, stop_name: s.stop_name, stop_lat: s.stop_lat, stop_lon: s.stop_lon }))}
          onChange={(next) => {
            const uidById = new Map(stops.map(s => [s.stop_id, s.uid]));
            setStops(next.map((r: any) => ({
              uid: uidById.get(r.stop_id) || uuidv4(),
              stop_id: r.stop_id, stop_name: r.stop_name, stop_lat: Number(r.stop_lat), stop_lon: Number(r.stop_lon)
            })));
          }}
          initialPageSize={5}
          onDeleteRow={(r:any) => {
            const sid = r.stop_id;
            if (!sid) return;
            if (!confirm(`Delete stop ${sid}?`)) return;
            setStops(prev => prev.filter(s => s.stop_id !== sid));
            setStopTimes(prev => prev.filter(st => st.stop_id !== sid));
          }}
        />
        <PaginatedEditableTable
          title="calendar.txt"
          rows={services}
          onChange={setServices}
          initialPageSize={5}
          onDeleteRow={(row:any) => {
            const sid = row.service_id;
            if (!sid) return;
            if (!confirm(`Delete service ${sid}? Trips using it will remain with dangling reference.`)) return;
            setServices(prev => prev.filter(s => s.service_id !== sid));
          }}
        />
        <PaginatedEditableTable
          title="trips.txt"
          rows={trips}
          onChange={setTrips}
          initialPageSize={10}
          onDeleteRow={(row:any) => {
            const tid = row.trip_id;
            if (!tid) return;
            if (!confirm(`Delete trip ${tid}?`)) return;
            setTrips(prev => prev.filter(t => t.trip_id !== tid));
            setStopTimes(prev => prev.filter(st => st.trip_id !== tid));
          }}
        />
        <PaginatedEditableTable
          title="stop_times.txt"
          rows={stopTimes}
          onChange={setStopTimes}
          initialPageSize={10}
          onDeleteRow={(row:any) => {
            // delete this specific row in stop_times
            setStopTimes(prev => prev.filter(st =>
              !(st.trip_id === row.trip_id && st.stop_id === row.stop_id && String(st.stop_sequence) === String(row.stop_sequence))
            ));
          }}
        />
        <PaginatedEditableTable
          title="shapes.txt"
          rows={shapePts.map(s => ({ shape_id: s.shape_id, shape_pt_lat: s.lat, shape_pt_lon: s.lon, shape_pt_sequence: s.seq }))}
          onChange={(next) => {
            setShapePts(next.map((r: any) => ({ shape_id: r.shape_id, lat: Number(r.shape_pt_lat), lon: Number(r.shape_pt_lon), seq: Number(r.shape_pt_sequence) })));
          }}
          initialPageSize={10}
          onDeleteRow={(row:any) => {
            setShapePts(prev => prev.filter(p => !(p.shape_id === row.shape_id && p.seq === row.shape_pt_sequence)));
          }}
        />
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