import React, { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";


// Reusable "Select all" with indeterminate state
function SelectAllCheckbox({
  label,
  totalCount,
  selectedCount,
  onToggle,
}: {
  label: string;
  totalCount: number;
  selectedCount: number;
  onToggle: (checked: boolean) => void;
}) {
  const ref = React.useRef<HTMLInputElement>(null);
  const all = totalCount > 0 && selectedCount === totalCount;
  const none = selectedCount === 0;
  const some = !none && !all;

  React.useEffect(() => {
    if (ref.current) ref.current.indeterminate = some;
  }, [some]);

  return (
    <label style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
      <input
        ref={ref}
        type="checkbox"
        checked={all}
        onChange={(e) => onToggle(e.target.checked)}
      />
      <span style={{ fontSize: 12, opacity: 0.8 }}>{label}</span>
      <span style={{ fontSize: 12, opacity: 0.55 }}>
        ({selectedCount}/{totalCount})
      </span>
    </label>
  );
}

/* ---------------- Small helper ---------------- */
function uniq<T>(arr: T[]) { return Array.from(new Set(arr)); }

/* ---------------- Types (kept local; align with your app types) ---------------- */
type Service = {
  service_id: string;
  monday?: number; tuesday?: number; wednesday?: number; thursday?: number; friday?: number; saturday?: number; sunday?: number;
  start_date?: string; end_date?: string;
};

type Stop = {
  stop_id: string;
  stop_name: string;
  stop_lat: number;
  stop_lon: number;
  uid?: string;
};

type Trip = {
  trip_id: string;
  route_id: string;
  service_id: string;
  shape_id?: string;
  direction_id?: number;
  trip_headsign?: string | null;
};

type StopTime = {
  trip_id: string;
  stop_id: string;
  stop_sequence: number;
  arrival_time: string;
  departure_time: string;
  pickup_type?: number;
  drop_off_type?: number;
};

/* ---------------- OD restriction model ---------------- */
type StopRuleMode = "normal" | "pickup" | "dropoff" | "custom";
type ODRestriction = {
  mode: StopRuleMode;
  dropoffOnlyFrom?: string[];  // origins where alighting here is allowed
  pickupOnlyTo?: string[];     // destinations where boarding here is allowed
};
type RestrictionsMap = Record<string, ODRestriction>;

/** (legacy/unused) Per-stop defaults granularity; keeping for clarity */
type StopDefaults = { dwell?: number; pickup?: number; dropoff?: number };

/** ✅ NEW: the missing type */
type StopDefaultsMap = Record<string, ODRestriction>;

const keyTS = (trip_id: string, stop_id: string) => `${trip_id}::${stop_id}`;

/* ---------------- Props ---------------- */
type PatternMatrixProps = {
  stops: Stop[];
  services: Service[];
  trips: Trip[];
  stopTimes: StopTime[];
  selectedRouteId?: string | null;

  /** Optional: delete selected trips (parent mutation). If omitted, we hide them locally. */
  onDeleteTrips?: (tripIds: string[]) => void;

  /** Optional: bulk shift times for selected trips (parent mutation). 
   * offsetSeconds can be negative. 
   */
  onShiftTripTimes?: (tripIds: string[], offsetSeconds: number) => void;

  initialRestrictions?: RestrictionsMap;
  onRestrictionsChange?: (next: RestrictionsMap) => void;

  /** per-stop defaults */
  initialStopDefaults?: StopDefaultsMap;
  onStopDefaultsChange?: (next: StopDefaultsMap) => void;

  // Optional: use your app’s time writeback; if omitted, we keep a local shadow so UI still edits.
  onEditTime?: (trip_id: string, stop_id: string, newUiTime: string) => void;

  onApplyRuleToSection?: (args: {
    tripIds: string[];
    stopId: string;
    rule: ODRestriction | null;
  }) => void;

};

/* ---------------- Helpers ---------------- */
function sameSet(a: string[] = [], b: string[] = []) {
  if (a.length !== b.length) return false;
  const A = new Set(a), B = new Set(b);
  for (const x of A) if (!B.has(x)) return false;
  return true;
}

function hhmmOnly(s: string) {
  if (!s) return "";
  const m = s.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (!m) return s;
  return `${m[1].padStart(2, "0")}:${m[2]}`;
}

/** 
 * Parse offsets like:
 *  "+01:00", "-00:30", "1:0", "1:00" (HH:MM[:SS])
 *  "+1.0", "-1.00" (dot form ONLY if the fractional part is 0 or 00 → whole hours)
 * Returns seconds (may be negative). Null on invalid.
 */
/**
 * Parse offsets like:
 *  "+01:00", "-00:30", "1:0", "1:00", "01:30" (colon form)
 *  "1.30", "1.15", "1.0", "1.00" (dot treated as time separator for minutes; optional seconds supported)
 *  "+1", "-2" (plain hours)
 * Rules:
 *  - With dot, the part(s) after the first dot are minutes (and optional seconds): 1.5 → 01:05, 1.30 → 01:30
 *  - Minutes/seconds must be 0–59.
 * Returns seconds (may be negative). Null on invalid.
 */
function parseOffsetToSeconds(s: string): number | null {
  if (!s) return null;
  const trimmed = s.trim();
  const m = trimmed.match(/^([+-])?\s*(\d+)(?:([:.])(\d{1,2})(?:(?:\3)(\d{1,2}))?)?$/);
  // groups: 1=sign, 2=hours, 3=sep(: or .), 4=minutes, 5=seconds
  if (!m) return null;

  const sign = m[1] === "-" ? -1 : 1;
  const H = parseInt(m[2], 10);
  const sep = m[3];         // ":" or "." or undefined
  let M = 0;
  let S = 0;

  if (sep) {
    M = parseInt(m[4], 10);
    if (isNaN(M) || M > 59) return null;

    if (m[5] != null) {
      S = parseInt(m[5], 10);
      if (isNaN(S) || S > 59) return null;
    }
  }
  // if no separator, it’s just hours
  return sign * (H * 3600 + M * 60 + S);
}

/** Shift a GTFS-style HH:MM[:SS] string by offsetSeconds. Supports hours >= 24 (kept as is). */
function shiftTime(hms: string, offsetSeconds: number): string {
  if (!hms) return hms;
  const m = hms.match(/^(\d{1,})(?::([0-5]\d))(?:\:([0-5]\d))?$/);
  if (!m) return hms;
  const H = parseInt(m[1], 10);
  const M = parseInt(m[2], 10);
  const S = m[3] ? parseInt(m[3], 10) : 0;
  let total = H * 3600 + M * 60 + S + offsetSeconds;
  // Allow negative to clamp at 0 if you prefer; here we keep floor at 0 to avoid "-01:00".
  if (total < 0) total = 0;
  const newH = Math.floor(total / 3600);
  const rem = total % 3600;
  const newM = Math.floor(rem / 60);
  const newS = rem % 60;
  // Keep seconds only if they existed originally
  const base = `${String(newH)}:${String(newM).padStart(2, "0")}`;
  return m[3] ? `${base}:${String(newS).padStart(2, "0")}` : base;
}

function daysString(svc?: Service) {
  if (!svc) return "";
  const f = [svc.monday, svc.tuesday, svc.wednesday, svc.thursday, svc.friday, svc.saturday, svc.sunday].map(Boolean);
  const chars = ["M", "T", "W", "T", "F", "S", "S"];
  return chars.map((c, i) => (f[i] ? c : "·")).join("");
}
function ymdDashed(x?: string) {
  return x && x.length === 8 ? `${x.slice(0, 4)}-${x.slice(4, 6)}-${x.slice(6, 8)}` : "";
}
function useClickAway<T extends HTMLElement>(onAway: () => void) {
  const ref = useRef<T | null>(null);
  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (!ref.current) return;
      if (!ref.current.contains(e.target as Node)) onAway();
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [onAway]);
  return ref;
}



function summarizeRowRuleForStop(args: {
  tripsWithStop: Trip[];
  stopId: string;
  groupTripStops: Map<string, string[]>;
  restrictions: RestrictionsMap;
}) {
  const { tripsWithStop, stopId, restrictions } = args;

  type Preset = { mode: StopRuleMode; dropoffOnlyFrom?: string[]; pickupOnlyTo?: string[] };
  const presets: Preset[] = tripsWithStop.map((t) => {
    const k = keyTS(t.trip_id, stopId);
    const r = restrictions[k];
    if (!r) return { mode: "normal" };
    if (r.mode === "pickup" || r.mode === "dropoff" || r.mode === "normal") return { mode: r.mode };
    return {
      mode: "custom",
      dropoffOnlyFrom: r.dropoffOnlyFrom ?? [],
      pickupOnlyTo: r.pickupOnlyTo ?? [],
    };
  });

  if (presets.length === 0) return null;

  // All same mode?
  const first = presets[0];
  const allSameMode = presets.every(p => p.mode === first.mode);
  if (!allSameMode) return null;

  if (first.mode === "normal" || first.mode === "pickup" || first.mode === "dropoff") {
    return first; // uniform simple mode
  }

  // custom: require identical sets to show a single row preset
  const allSameCustom =
    presets.every(p => p.mode === "custom") &&
    presets.every(p => sameSet(p.dropoffOnlyFrom, first.dropoffOnlyFrom)) &&
    presets.every(p => sameSet(p.pickupOnlyTo, first.pickupOnlyTo));

  return allSameCustom ? first : null;
}
/* ---------------- Tiny chip ---------------- */
function Chip({ children }: { children: React.ReactNode }) {
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 6px",
        borderRadius: 12,
        border: "1px solid #ddd",
        background: "#f7f7f8",
        fontSize: 11,
        marginRight: 6,
      }}
    >
      {children}
    </span>
  );
}

/* ---------------- Popover rendered in a portal (no clipping) ---------------- */
/** Note: anchorRef is wide (Element) to avoid TS variance headaches with HTMLButtonElement, etc. */
function PortalPopover({
  open,
  anchorEl,
  onClose,
  offset = { x: 0, y: 6 },
  children,
}: {
  open: boolean;
  anchorEl: HTMLElement | null;
  onClose: () => void;
  offset?: { x: number; y: number };
  children: React.ReactNode;
}) {
  const [pos, setPos] = useState({ top: 0, left: 0, width: 0 });

  useEffect(() => {
    if (!open || !anchorEl) return;
    const update = () => {
      const r = anchorEl.getBoundingClientRect();
      setPos({ top: r.bottom + offset.y, left: r.left + offset.x, width: r.width });
    };
    update();
    const onScroll = () => update();
    const onResize = () => update();
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };

    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", onResize);
    window.addEventListener("keydown", onKey);

    return () => {
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", onResize);
      window.removeEventListener("keydown", onKey);
    };
  }, [open, anchorEl, offset.x, offset.y, onClose]);

  if (!open || !anchorEl) return null;

  return createPortal(
    <div style={{ position: "fixed", top: pos.top, left: pos.left, zIndex: 200000, background: '#fff', padding: '10px' }}>
      {children}
    </div>,
    document.body
  );
}

/* ---------------- Stop rule editors ---------------- */
function StopRuleEditor({
  open, onClose, mode, setMode,
  upstreamStops, downstreamStops,
  dropoffOnlyFrom, pickupOnlyTo,
  onChangeDropoffOnlyFrom, onChangePickupOnlyTo
}: {
  open: boolean;
  onClose: () => void;
  mode: StopRuleMode;
  setMode: (m: StopRuleMode) => void;
  upstreamStops: Stop[];
  downstreamStops: Stop[];
  dropoffOnlyFrom?: string[];
  pickupOnlyTo?: string[];
  onChangeDropoffOnlyFrom: (ids: string[]) => void;
  onChangePickupOnlyTo: (ids: string[]) => void;
}) {
  const ref = useClickAway<HTMLDivElement>(onClose);
  if (!open) return null;
  return (
    <div style={{ border: "1px solid #df007d"}}  ref={ref} role="dialog" aria-label="Bulk stop rule editor">
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
        <span style={{ fontSize: 12, color: "#555" }}>Stop rule:</span>
        <select
          value={mode}
          onChange={(e) => setMode(e.target.value as StopRuleMode)}
          style={{ fontSize: 12 }}
        >
          <option value="normal">Stop</option>
          <option value="pickup">Pickup only</option>
          <option value="dropoff">Dropoff only</option>
          <option value="custom">Custom…</option>
        </select>
      </div>

      {mode === "pickup" && <div style={{ fontSize: 12, color: "#555" }}>Pickup-only (no alighting here).</div>}
      {mode === "dropoff" && <div style={{ fontSize: 12, color: "#555" }}>Dropoff-only (no boarding here).</div>}

      {mode === "custom" && (
        <div style={{ display: "grid", gap: 12 }}>
          {/* DROP-OFF BLOCK */}
          <div>
            <div style={{ fontSize: 12, marginBottom: 6 }}>
              <b>Dropoff allowed when boarded at…</b>
            </div>

            <div style={{ marginBottom: 6 }}>
              <SelectAllCheckbox
                label="Select all origins"
                totalCount={upstreamStops.length}
                selectedCount={dropoffOnlyFrom?.length ?? 0}
                onToggle={(checked) => {
                  const nextIds = checked ? upstreamStops.map((s) => s.stop_id) : [];
                  onChangeDropoffOnlyFrom(nextIds);
                }}
              />
            </div>

            <div style={{ maxHeight: 160, overflow: "auto", border: "1px solid #eee", borderRadius: 6, padding: 6 }}>
              {upstreamStops.length === 0 ? (
                <div style={{ fontSize: 12, color: "#777" }}>No upstream stops.</div>
              ) : (
                upstreamStops.map((s) => {
                  const checked = !!dropoffOnlyFrom?.includes(s.stop_id);
                  return (
                    <label key={s.stop_id} style={{ display: "flex", gap: 8, fontSize: 12, padding: "3px 0" }}>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={(e) => {
                          const next = new Set(dropoffOnlyFrom ?? []);
                          e.target.checked ? next.add(s.stop_id) : next.delete(s.stop_id);
                          onChangeDropoffOnlyFrom(Array.from(next));
                        }}
                      />
                      <span>{s.stop_name}</span>
                    </label>
                  );
                })
              )}
            </div>

            <div style={{ marginTop: 6 }}>
              {(dropoffOnlyFrom ?? []).map((id) => {
                const it = upstreamStops.find((x) => x.stop_id === id);
                return it ? <Chip key={id}>{it.stop_name}</Chip> : null;
              })}
            </div>
          </div>

          {/* PICKUP BLOCK */}
          <div>
            <div style={{ fontSize: 12, marginBottom: 6 }}>
              <b>Pickup allowed only to…</b>
            </div>

            <div style={{ marginBottom: 6 }}>
              <SelectAllCheckbox
                label="Select all destinations"
                totalCount={downstreamStops.length}
                selectedCount={pickupOnlyTo?.length ?? 0}
                onToggle={(checked) => {
                  const nextIds = checked ? downstreamStops.map((s) => s.stop_id) : [];
                  onChangePickupOnlyTo(nextIds);
                }}
              />
            </div>

            <div style={{ maxHeight: 160, overflow: "auto", border: "1px solid #eee", borderRadius: 6, padding: 6 }}>
              {downstreamStops.length === 0 ? (
                <div style={{ fontSize: 12, color: "#777" }}>No downstream stops.</div>
              ) : (
                downstreamStops.map((s) => {
                  const checked = !!pickupOnlyTo?.includes(s.stop_id);
                  return (
                    <label key={s.stop_id} style={{ display: "flex", gap: 8, fontSize: 12, padding: "3px 0" }}>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={(e) => {
                          const next = new Set(pickupOnlyTo ?? []);
                          e.target.checked ? next.add(s.stop_id) : next.delete(s.stop_id);
                          onChangePickupOnlyTo(Array.from(next));
                        }}
                      />
                      <span>{s.stop_name}</span>
                    </label>
                  );
                })
              )}
            </div>

            <div style={{ marginTop: 6 }}>
              {(pickupOnlyTo ?? []).map((id) => {
                const it = downstreamStops.find((x) => x.stop_id === id);
                return it ? <Chip key={id}>{it.stop_name}</Chip> : null;
              })}
            </div>
          </div>
        </div>
      )}

      <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 12 }}>
        <button onClick={onClose} style={{ fontSize: 12, padding: "6px 10px" }}>Close</button>
      </div>
    </div>
  );
}

/* ------- Bulk editor: apply rule to ALL trips at this stop (row) ------- */
function StopBulkRuleEditor({
  open, onClose, mode, setMode,
  upstreamPool, downstreamPool,
  bulkDropFrom, bulkPickTo,
  setBulkDropFrom, setBulkPickTo,
  onApply
}: {
  open: boolean;
  onClose: () => void;
  mode: StopRuleMode;
  setMode: (m: StopRuleMode) => void;
  upstreamPool: Stop[];
  downstreamPool: Stop[];
  bulkDropFrom: string[];
  bulkPickTo: string[];
  setBulkDropFrom: (ids: string[]) => void;
  setBulkPickTo: (ids: string[]) => void;
  onApply: () => void;
}) {
  const ref = useClickAway<HTMLDivElement>(onClose);
  if (!open) return null;
  return (
    <div style={{ border: "1px solid #df007d", borderRadius: 6, padding: "10px", boxShadow: "rgba(0,0,0,0.7) 3px 5px 20px"}}  ref={ref} role="dialog" aria-label="Bulk stop rule editor">
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
        <span style={{ fontSize: 12, color: "#555" }}>Stop rule (bulk):</span>
        <select value={mode} onChange={(e) => setMode(e.target.value as StopRuleMode)} style={{ fontSize: 12 }}>
          <option value="normal">Stop</option>
          <option value="pickup">Pickup only</option>
          <option value="dropoff">Dropoff only</option>
          <option value="custom">Custom…</option>
        </select>
      </div>

      {mode === "custom" && (
        <div style={{ display: "grid", gap: 12 }}>
          {/* DROP-OFF BLOCK */}
          <div>
            <div style={{ fontSize: 12, marginBottom: 6 }}><b>Dropoff allowed when boarded at…</b></div>

            <div style={{ marginBottom: 6 }}>
              <SelectAllCheckbox
                label="Select all origins"
                totalCount={upstreamPool.length}
                selectedCount={bulkDropFrom.length}
                onToggle={(checked) => {
                  setBulkDropFrom(checked ? upstreamPool.map(s => s.stop_id) : []);
                }}
              />
            </div>

            <div style={{ maxHeight: 160, overflow: "auto", border: "1px solid #eee", borderRadius: 6, padding: 6 }}>
              {upstreamPool.length === 0 ? (
                <div style={{ fontSize: 12, color: "#777" }}>No upstream stops.</div>
              ) : upstreamPool.map((s) => {
                const checked = !!bulkDropFrom.includes(s.stop_id);
                return (
                  <label key={s.stop_id} style={{ display: "flex", gap: 8, fontSize: 12, padding: "3px 0" }}>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={(e) => {
                        const next = new Set(bulkDropFrom);
                        e.target.checked ? next.add(s.stop_id) : next.delete(s.stop_id);
                        setBulkDropFrom(Array.from(next));
                      }}
                    />
                    <span>{s.stop_name}</span>
                  </label>
                );
              })}
            </div>
          </div>

          {/* PICKUP BLOCK */}
          <div>
            <div style={{ fontSize: 12, marginBottom: 6 }}><b>Pickup allowed only to…</b></div>

            <div style={{ marginBottom: 6 }}>
              <SelectAllCheckbox
                label="Select all destinations"
                totalCount={downstreamPool.length}
                selectedCount={bulkPickTo.length}
                onToggle={(checked) => {
                  setBulkPickTo(checked ? downstreamPool.map(s => s.stop_id) : []);
                }}
              />
            </div>

            <div style={{ maxHeight: 160, overflow: "auto", border: "1px solid #eee", borderRadius: 6, padding: 6 }}>
              {downstreamPool.length === 0 ? (
                <div style={{ fontSize: 12, color: "#777" }}>No downstream stops.</div>
              ) : downstreamPool.map((s) => {
                const checked = !!bulkPickTo.includes(s.stop_id);
                return (
                  <label key={s.stop_id} style={{ display: "flex", gap: 8, fontSize: 12, padding: "3px 0" }}>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={(e) => {
                        const next = new Set(bulkPickTo);
                        e.target.checked ? next.add(s.stop_id) : next.delete(s.stop_id);
                        setBulkPickTo(Array.from(next));
                      }}
                    />
                    <span>{s.stop_name}</span>
                  </label>
                );
              })}
            </div>
          </div>
        </div>
      )}

      <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 12 }}>
        <button onClick={onClose} style={{ fontSize: 12, padding: "6px 10px" }}>Cancel</button>
        <button onClick={onApply} style={{ fontSize: 12, padding: "6px 10px" }}>Apply to these trips</button>
      </div>
    </div>
  );
}

/* ---------------- Main component ---------------- */
export default function PatternMatrix({
  stops, services, trips, stopTimes, selectedRouteId,
  onDeleteTrips,
  onShiftTripTimes,
  initialRestrictions, onRestrictionsChange,
  initialStopDefaults, onStopDefaultsChange,
  onEditTime,
}: PatternMatrixProps) {
    // --- Selection & bulk-shift state (must be before tripsFiltered) ---
  const [selectedTripIds, setSelectedTripIds] = useState<Set<string>>(new Set());
  const [hiddenTripIds, setHiddenTripIds] = useState<Set<string>>(new Set());
  const [shiftInput, setShiftInput] = useState<string>("");

  const isTripSelected = (id: string) => selectedTripIds.has(id);
  const toggleTripSelected = (id: string, checked: boolean) => {
    setSelectedTripIds(prev => {
      const next = new Set(prev);
      if (checked) next.add(id); else next.delete(id);
      return next;
    });
  };

  /* ---------- Base indexing ---------- */
  const tripsFiltered = useMemo(
    () => {
      const base = selectedRouteId ? trips.filter(t => t.route_id === selectedRouteId) : trips;
      return base.filter(t => !hiddenTripIds.has(t.trip_id));
    },
    [trips, selectedRouteId, hiddenTripIds]
  );

  // one anchor for the cell editor
  const [cellAnchorEl, setCellAnchorEl] = useState<HTMLElement | null>(null);
  // one anchor for the bulk (row) editor
  const [bulkAnchorEl, setBulkAnchorEl] = useState<HTMLElement | null>(null);

  const stopById = useMemo(() => {
    const m = new Map<string, Stop>();
    for (const s of stops) m.set(s.stop_id, s);
    return m;
  }, [stops]);

  const serviceById = useMemo(() => {
    const m = new Map<string, Service>();
    for (const s of services) m.set(s.service_id, s);
    return m;
  }, [services]);

  const tripIdSet = useMemo(() => new Set(tripsFiltered.map(t => t.trip_id)), [tripsFiltered]);

  // Map trip -> ordered stop_ids
  const tripStops = useMemo(() => {
    const grouped = new Map<string, StopTime[]>();
    for (const st of stopTimes) {
      if (!tripIdSet.has(st.trip_id)) continue;
      (grouped.get(st.trip_id) ?? grouped.set(st.trip_id, []).get(st.trip_id)!).push(st);
    }
    const byTrip = new Map<string, string[]>();
    for (const [tid, arr] of grouped.entries()) {
      arr.sort((a, b) => a.stop_sequence - b.stop_sequence);
      byTrip.set(tid, arr.map(x => x.stop_id));
    }
    return byTrip;
  }, [stopTimes, tripIdSet]);

  // First departure time per trip (for sorting)
  const firstDep = useMemo(() => {
    const m = new Map<string, string>();
    const grouped = new Map<string, StopTime[]>();
    for (const st of stopTimes) {
      if (!tripIdSet.has(st.trip_id)) continue;
      (grouped.get(st.trip_id) ?? grouped.set(st.trip_id, []).get(st.trip_id)!).push(st);
    }
    for (const [tid, arr] of grouped.entries()) {
      arr.sort((a, b) => a.stop_sequence - b.stop_sequence);
      m.set(tid, arr[0]?.departure_time ?? "");
    }
    return m;
  }, [stopTimes, tripIdSet]);

  // Sort trips: by service_id, then by first departure (earlier first)
  const orderedTrips = useMemo(() => {
    const copy = [...tripsFiltered];
    copy.sort((a, b) => {
      if (a.service_id !== b.service_id) return a.service_id.localeCompare(b.service_id);
      const fa = firstDep.get(a.trip_id) ?? "";
      const fb = firstDep.get(b.trip_id) ?? "";
      if (fa === "" && fb === "") return a.trip_id.localeCompare(b.trip_id);
      if (fa === "") return 1;
      if (fb === "") return -1;
      return fa.localeCompare(fb);
    });
    return copy;
  }, [tripsFiltered, firstDep]);

  /* ---------- Group trips by exact ordered stop sequence ---------- */
  type Group = { key: string; trips: Trip[]; seq: string[] };
  const groups: Group[] = useMemo(() => {
    const map = new Map<string, Group>();
    for (const t of orderedTrips) {
      const seq = tripStops.get(t.trip_id) ?? [];
      const key = seq.join(">");
      if (!map.has(key)) map.set(key, { key, trips: [], seq });
      map.get(key)!.trips.push(t);
    }
    return Array.from(map.values());
  }, [orderedTrips, tripStops]);

  /* ---------- Shared state ---------- */
  const [localTimes, setLocalTimes] = useState<Record<string, string>>({});
  // --- Local-undo support (one-step undo for Summary actions) ---
  // --- Local Undo / Redo (two-step stack) ---
type UndoSnapshot = {
  localTimes: Record<string, string>;
  hiddenTripIds: string[];
  selectedTripIds: string[];
};

// Back/forward stacks
const [undoStack, setUndoStack] = useState<UndoSnapshot[]>([]);
const [redoStack, setRedoStack] = useState<UndoSnapshot[]>([]);

// helper to capture a snapshot before mutating
function pushUndo() {
  setUndoStack(prev => [
    ...prev.slice(-9), // cap to 10
    {
      localTimes: { ...localTimes },
      hiddenTripIds: Array.from(hiddenTripIds),
      selectedTripIds: Array.from(selectedTripIds),
    },
  ]);
  setRedoStack([]); // clear redo whenever new action occurs
}
  const getUiTime = (trip_id: string, stop_id: string, fallback: string) =>
    localTimes[keyTS(trip_id, stop_id)] ?? fallback;

// (optional but good) initialize from props with a function initializer
const [restrictions, setRestrictions] = useState<RestrictionsMap>(() => initialRestrictions ?? {});
const [stopDefaults, setStopDefaults] = useState<StopDefaultsMap>(() => initialStopDefaults ?? {});

// prevent feedback loop when parent re-renders on every set
const sentRestrictionsRef = useRef<string>("");
useEffect(() => {
  const snapshot = JSON.stringify(restrictions);
  if (snapshot !== sentRestrictionsRef.current) {
    onRestrictionsChange?.(restrictions);
    sentRestrictionsRef.current = snapshot;
  }
}, [restrictions, onRestrictionsChange]);

const sentDefaultsRef = useRef<string>("");
useEffect(() => {
  const snapshot = JSON.stringify(stopDefaults);
  if (snapshot !== sentDefaultsRef.current) {
    onStopDefaultsChange?.(stopDefaults);
    sentDefaultsRef.current = snapshot;
  }
}, [stopDefaults, onStopDefaultsChange]);

  // --- Bulk actions (delete / shift) ---
  const deleteSelectedTrips = () => {
    const ids = Array.from(selectedTripIds);
    if (ids.length === 0) return;

    // Snapshot current local state BEFORE changing anything (for undo)
    pushUndo();

    if (onDeleteTrips) {
      // NOTE: This triggers parent-level mutation; local undo only restores local state.
      // For full app-wide undo, prefer wiring this through App's history system.
      onDeleteTrips(ids);
    } else {
      // Local hide fallback
      setHiddenTripIds(prev => {
        const next = new Set(prev);
        ids.forEach(id => next.add(id));
        return next;
      });
    }

    // Clear selection (post-op)
    setSelectedTripIds(new Set());
  };

  const shiftSelectedTrips = () => {
    const ids = Array.from(selectedTripIds);
    if (ids.length === 0) return;

    const secs = parseOffsetToSeconds(shiftInput);
    if (secs === null) {
      alert("Invalid offset. Use formats like +01:00, -00:30, or 01:15.");
      return;
    }

    // Snapshot current local state BEFORE changing anything (for undo)
    pushUndo();

    if (onShiftTripTimes) {
      // NOTE: Parent-level mutation; local undo won’t revert parent state.
      onShiftTripTimes(ids, secs);
    } else {
      // Local shift fallback: update localTimes overlay for every stop_time in these trips
      const idSet = new Set(ids);
      const next: Record<string, string> = {};
      for (const st of stopTimes) {
        if (!idSet.has(st.trip_id)) continue;
        const base = st.departure_time || st.arrival_time || "";
        if (!base) continue;
        const shifted = shiftTime(base, secs);
        next[keyTS(st.trip_id, st.stop_id)] = hhmmOnly(shifted);
      }
      setLocalTimes(prev => ({ ...prev, ...next }));
    }
  };

// -------- Undo / Redo handlers --------
function handleUndo() {
  setUndoStack(prev => (
    (() => {
      if (!prev.length) return prev;
      const last = prev[prev.length - 1];
      setRedoStack(r => [
        ...r.slice(-9),
        {
          localTimes: { ...localTimes },
          hiddenTripIds: Array.from(hiddenTripIds),
          selectedTripIds: Array.from(selectedTripIds),
        },
      ]);
      setLocalTimes(last.localTimes);
      setHiddenTripIds(new Set(last.hiddenTripIds));
      setSelectedTripIds(new Set(last.selectedTripIds));
      return prev.slice(0, -1);
    })()
  ));
}

function handleRedo() {
  setRedoStack(prev => (
    (() => {
      if (!prev.length) return prev;
      const last = prev[prev.length - 1];
      setUndoStack(u => [
        ...u.slice(-9),
        {
          localTimes: { ...localTimes },
          hiddenTripIds: Array.from(hiddenTripIds),
          selectedTripIds: Array.from(selectedTripIds),
        },
      ]);
      setLocalTimes(last.localTimes);
      setHiddenTripIds(new Set(last.hiddenTripIds));
      setSelectedTripIds(new Set(last.selectedTripIds));
      return prev.slice(0, -1);
    })()
  ));
}

// --- Keyboard shortcuts ---
useEffect(() => {
  const onKey = (e: KeyboardEvent) => {
    const tag = (e.target as HTMLElement | null)?.tagName?.toLowerCase();
    const isTyping =
      tag === "input" || tag === "textarea" ||
      (e.target as HTMLElement | null)?.getAttribute("contenteditable") === "true";

    if (isTyping) return;

    const z = e.key.toLowerCase() === "z";
    if ((e.metaKey || e.ctrlKey) && z) {
      e.preventDefault();
      if (e.shiftKey) handleRedo(); // Cmd/Ctrl + Shift + Z
      else handleUndo();            // Cmd/Ctrl + Z
    }
  };
  window.addEventListener("keydown", onKey);
  return () => window.removeEventListener("keydown", onKey);
}, [undoStack, redoStack]);



  const [openCellKey, setOpenCellKey] = useState<string | null>(null);
  const [openBulkKey, setOpenBulkKey] = useState<string | null>(null);
  const [bulkMode, setBulkMode] = useState<StopRuleMode>("normal");
  const [bulkDropFrom, setBulkDropFrom] = useState<string[]>([]);
  const [bulkPickTo, setBulkPickTo] = useState<string[]>([]);

  const headerFor = (t: Trip) => {
    const svc = serviceById.get(t.service_id);
    const days = daysString(svc);
    const range = svc ? `${ymdDashed(svc.start_date)}/${ymdDashed(svc.end_date)}` : "";
    return (
      <div style={{ lineHeight: 1.15, whiteSpace: "normal", fontSize: 11 }}>
        <div style={{ fontWeight: 700 }}>{t.trip_id}</div>
        <div>{t.service_id}</div>
        <div style={{ opacity: 0.7 }}>{days || range ? `(${[days, range].filter(Boolean).join(" ")})` : ""}</div>
      </div>
    );
  };

  // Apply bulk rule to all trips at a given stop (row) within *current group*
  const makeApplyBulkToStop = (groupTrips: Trip[], groupTripStops: Map<string, string[]>) => (stop_id: string) => {
    setRestrictions((prev) => {
      const next: RestrictionsMap = { ...prev };

      if (bulkMode === "normal") {
        for (const t of groupTrips) delete next[keyTS(t.trip_id, stop_id)];
        return next;
      }

      if (bulkMode === "pickup" || bulkMode === "dropoff") {
        for (const t of groupTrips) next[keyTS(t.trip_id, stop_id)] = { mode: bulkMode };
        return next;
      }

      // custom (clamp per-trip)
      for (const t of groupTrips) {
        const seq = groupTripStops.get(t.trip_id) ?? [];
        const idx = seq.indexOf(stop_id);
        const upstreamSet = new Set(idx > 0 ? seq.slice(0, idx) : []);
        const downstreamSet = new Set(idx >= 0 ? seq.slice(idx + 1) : []);
        const effDropFrom = bulkDropFrom.filter(id => upstreamSet.has(id));
        const effPickTo   = bulkPickTo.filter(id => downstreamSet.has(id));
        next[keyTS(t.trip_id, stop_id)] = { mode: "custom", dropoffOnlyFrom: effDropFrom, pickupOnlyTo: effPickTo };
      }
      return next;
    });
    setOpenBulkKey(null);
  };

  /* ---------- Render one group block ---------- */
  const renderGroup = (g: Group, gi: number) => {
    // groupTripStops: Trip->sequence (already computed, but scoped)
    const groupTripStops = new Map<string, string[]>();
    for (const t of g.trips) groupTripStops.set(t.trip_id, tripStops.get(t.trip_id) ?? []);

    // union of stop_ids with baseline = first trip in this group
    const computeOrderedStopIds = () => {
      const base = groupTripStops.get(g.trips[0]?.trip_id ?? "") ?? [];
      const set = new Set(base);
      for (let i = 1; i < g.trips.length; i++) {
        for (const sid of (groupTripStops.get(g.trips[i].trip_id) ?? [])) set.add(sid);
      }
      return Array.from(set);
    };
    const orderedStopIds = computeOrderedStopIds();

    const groupTitle = g.seq.length
      ? g.seq.map(id => stopById.get(id)?.stop_name || id).join(" → ")
      : "No stop_times";

    const applyBulkToStop = makeApplyBulkToStop(g.trips, groupTripStops);

    return (
      <div
        key={`grp_${gi}`}
        className="card section"
        style={{
          marginTop: gi === 0 ? 12 : 16,
          borderColor: "#df007d",
          borderWidth: 2,
          borderStyle: "solid",
          borderRadius: 12
        }}
      >
        <div className="card-body" style={{ overflow: "auto", position: "relative" }}>
          <div style={{ marginBottom: 6, display: "flex", alignItems: "baseline", gap: 8 }}>
            <h3 style={{ margin: 0 }}>Summary of selected route</h3>
            <div style={{ fontSize: 12, opacity: .75, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
              {groupTitle}
            </div>
            <div style={{ marginLeft: "auto", fontSize: 12, opacity: .7 }}>
              Trips in this pattern: {g.trips.length}
            </div>
          </div>

                    {/* Group actions: select all, delete, shift */}
          <div style={{ display: "flex", gap: 12, alignItems: "center", margin: "8px 0" }}>
            <SelectAllCheckbox
              label="Select all trips in this pattern"
              totalCount={g.trips.length}
              selectedCount={g.trips.filter(t => selectedTripIds.has(t.trip_id)).length}
              onToggle={(checked) => {
                setSelectedTripIds(prev => {
                  const next = new Set(prev);
                  if (checked) g.trips.forEach(t => next.add(t.trip_id));
                  else g.trips.forEach(t => next.delete(t.trip_id));
                  return next;
                });
              }}
            />

            <button
              onClick={handleUndo}
              disabled={undoStack.length === 0}
              style={{
                fontSize: 12,
                padding: "6px 10px",
                border: "1px solid #ddd",
                background: "#fff",
                borderRadius: 6,
                cursor: undoStack.length === 0 ? "not-allowed" : "pointer"
              }}
              title="Undo last shift/delete in Summary"
            >
              Undo
            </button>

            <button
              onClick={handleRedo}
              disabled={redoStack.length === 0}
              style={{
                fontSize: 12,
                padding: "6px 10px",
                border: "1px solid #ddd",
                background: "#fff",
                borderRadius: 6,
                cursor: redoStack.length === 0 ? "not-allowed" : "pointer"
              }}
              title="Redo last shift/delete in Summary (or Cmd/Ctrl+Shift+Z)"
            >
              Redo
            </button>

            <button
              onClick={deleteSelectedTrips}
              disabled={selectedTripIds.size === 0}
              style={{ fontSize: 12, padding: "6px 10px", border: "1px solid #ddd", background: "#fff", borderRadius: 6, cursor: selectedTripIds.size ? "pointer" : "not-allowed" }}
              title="Delete selected trips (columns)"
            >
              Delete selected trips
            </button>

            <div style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
              <span style={{ fontSize: 12, color: "#555" }}>Shift service</span>
              <input
                value={shiftInput}
                onChange={(e) => setShiftInput(e.target.value)}
                placeholder="+01:00 / -00:30 / 1.30 / 1.15"
                style={{ width: 100, fontVariantNumeric: "tabular-nums" }}
              />
              <button
                onClick={shiftSelectedTrips}
                disabled={selectedTripIds.size === 0 || !shiftInput.trim()}
                style={{ fontSize: 12, padding: "6px 10px", border: "1px solid #ddd", background: "#fff", borderRadius: 6, cursor: (selectedTripIds.size && shiftInput.trim()) ? "pointer" : "not-allowed" }}
                title="Apply shift to selected trips"
              >
                OK
              </button>
            </div>
          </div>

          <table style={{ borderCollapse: "collapse", width: "100%" }}>
            <thead>
              <tr>
                <th style={{ width: 260, textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Stop</th>
                {g.trips.map((t) => (
                  <th key={t.trip_id} style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee", minWidth: 180 }}>
                    <div style={{ display: "flex", alignItems: "flex-start", gap: 8 }}>
                      <input
                        type="checkbox"
                        checked={isTripSelected(t.trip_id)}
                        onChange={(e) => toggleTripSelected(t.trip_id, e.target.checked)}
                        title="Select this trip"
                        style={{ marginTop: 2 }}
                      />
                      {headerFor(t)}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>

            <tbody>
              {orderedStopIds.map((sid) => {
                const s = stopById.get(sid);
                const rowKey = `${gi}::${sid}`;
                const isBulkOpen = openBulkKey === rowKey;


                
                // Build UNION pools across group trips that include this stop
                const tripsWithThisStop = g.trips.filter(t => (groupTripStops.get(t.trip_id) ?? []).includes(sid));
                
                // safe unions even if empty
                const upstreamUnion = new Set<string>();
                const downstreamUnion = new Set<string>();

                for (const t of tripsWithThisStop) {
                  const seq = tripStops.get(t.trip_id) ?? [];
                  const idx = seq.indexOf(sid);
                  const up = idx > 0 ? seq.slice(0, idx) : [];
                  const down = idx >= 0 ? seq.slice(idx + 1) : [];
                  up.forEach(id => upstreamUnion.add(id));
                  down.forEach(id => downstreamUnion.add(id));
                }

                const upstreamPool = Array.from(upstreamUnion).map(id => stopById.get(id)).filter((x): x is Stop => Boolean(x));
                const downstreamPool = Array.from(downstreamUnion).map(id => stopById.get(id)).filter((x): x is Stop => Boolean(x));
                const rowPreset = summarizeRowRuleForStop({
                  tripsWithStop: tripsWithThisStop,
                  stopId: sid,
                  groupTripStops,
                  restrictions,
                });
                return (
                  <tr key={sid}>
                    <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", position: "sticky", left: 0, background: "#fff", zIndex: 2 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, position: "relative" }}>
                        <span>{s?.stop_name ?? sid}</span>

                        {/* 3A) Row badge: put this EXACTLY here, between the name and the gear */}
                        {rowPreset && (
                          <span
                            title={
                              rowPreset.mode === "custom"
                                ? "Custom OD rules applied in this section"
                                : rowPreset.mode === "pickup"
                                ? "Pickup-only in this section"
                                : rowPreset.mode === "dropoff"
                                ? "Dropoff-only in this section"
                                : "Stop"
                            }
                            style={{
                              fontSize: 10,
                              padding: "1px 6px",
                              border: "1px solid #ddd",
                              borderRadius: 10,
                              background: "#f7f7f8",
                            }}
                          >
                            {rowPreset.mode === "pickup"
                              ? "⭱ pickup"
                              : rowPreset.mode === "dropoff"
                              ? "⭳ dropoff"
                              : rowPreset.mode === "custom"
                              ? "⇄ custom"
                              : "⤵︎ stop"}
                          </span>
                        )}

                        <button
                          title="Edit rule for this row (trips in this section)"
                          onClick={(e) => {
                            setBulkAnchorEl(e.currentTarget as HTMLElement);
                            const rowKeyInner = `${gi}::${sid}`;
                            setOpenBulkKey((cur: string | null) => (cur === rowKeyInner ? null : rowKeyInner));

                            /* Preload the bulk editor FROM rowPreset (imported overrides)
                              instead of stopDefaults */
                            if (!rowPreset || rowPreset.mode === "normal") {
                              setBulkMode("normal");
                              setBulkDropFrom([]);
                              setBulkPickTo([]);
                            } else if (rowPreset.mode === "pickup" || rowPreset.mode === "dropoff") {
                              setBulkMode(rowPreset.mode);
                              setBulkDropFrom([]);
                              setBulkPickTo([]);
                            } else {
                              setBulkMode("custom");
                              setBulkDropFrom(rowPreset.dropoffOnlyFrom ?? []);
                              setBulkPickTo(rowPreset.pickupOnlyTo ?? []);
                            }
                          }}
                          style={{ border: "1px solid #ddd", background: "#fff", borderRadius: 6, padding: "2px 6px", fontSize: 11, cursor: "pointer" }}
                        >
                          ⚙︎
                        </button>

                        <PortalPopover
                          open={isBulkOpen}
                          anchorEl={isBulkOpen ? bulkAnchorEl : null}
                          onClose={() => {
                            setOpenBulkKey(null);
                            setBulkAnchorEl(null);
                          }}
                        >
                          <div>
                            <StopBulkRuleEditor
                              open={true}
                              onClose={() => {
                                setOpenBulkKey(null);
                                setBulkAnchorEl(null);
                              }}
                              mode={bulkMode}
                              setMode={setBulkMode}
                              upstreamPool={upstreamPool ?? []}
                              downstreamPool={downstreamPool ?? []}
                              bulkDropFrom={bulkDropFrom}
                              bulkPickTo={bulkPickTo}
                              setBulkDropFrom={setBulkDropFrom}
                              setBulkPickTo={setBulkPickTo}
                              onApply={() => applyBulkToStop(sid)}
                            />
                          </div>
                        </PortalPopover>
                      </div>
                    </td>

                    {g.trips.map((t) => {
                      const st = (stopTimes.find(x => x.trip_id === t.trip_id && x.stop_id === sid) ?? null);
                      const base = hhmmOnly(st?.departure_time || st?.arrival_time || "");
                      const ui = hhmmOnly(getUiTime(t.trip_id, sid, base));
                      const k = keyTS(t.trip_id, sid);
                      const rule = restrictions[k]?.mode ?? "normal";
                      const cellKey = `${gi}::${t.trip_id}::${sid}`;
                      const isOpen = openCellKey === cellKey;

                      // upstream/downstream lists for this trip (within group)
                      const seqs = groupTripStops.get(t.trip_id) ?? [];
                      const idx = seqs.indexOf(sid);
                      const upstreamIds = idx > 0 ? seqs.slice(0, idx) : [];
                      const downstreamIds = idx >= 0 ? seqs.slice(idx + 1) : [];
                      const upstreamStops = upstreamIds.map((x) => stopById.get(x)!).filter(Boolean);
                      const downstreamStops = downstreamIds.map((x) => stopById.get(x)!).filter(Boolean);

                      const dropFrom = restrictions[k]?.dropoffOnlyFrom ?? [];
                      const pickTo = restrictions[k]?.pickupOnlyTo ?? [];

                      return (
                        <td
                          key={t.trip_id}
                          style={{
                            padding: 8,
                            borderBottom: "1px solid #f2f2f2",
                            position: "relative",
                            background: selectedTripIds.has(t.trip_id) ? "#fafcff" : "#fff"
                          }}
                        >
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <input
                              value={ui}
                              placeholder="--:--"
                              maxLength={5}
                              onChange={(e) => {
                                const val = e.target.value.replace(/[^\d:]/g, "");
                                if (onEditTime) onEditTime(t.trip_id, sid, val);
                                else setLocalTimes((prev) => ({ ...prev, [k]: val }));
                              }}
                              style={{ width: 64, fontVariantNumeric: "tabular-nums" }}
                            />
                            <button
                              title="Stop rule"
                              onClick={(e) => {
                                setCellAnchorEl(e.currentTarget as HTMLElement);
                                const cellKeyInner = `${gi}::${t.trip_id}::${sid}`;
                                setOpenCellKey((cur: string | null) => (cur === cellKeyInner ? null : cellKeyInner));
                              }}
                              style={{ border: "1px solid #ddd", background: "#fff", borderRadius: 6, padding: "2px 6px", fontSize: 11, cursor: "pointer" }}>
                              {rule === "pickup" ? "⭱" : rule === "dropoff" ? "⭳" : rule === "custom" ? "⇄" : "⤵︎"}
                            </button>

                            <PortalPopover
                              open={isOpen}
                              anchorEl={isOpen ? cellAnchorEl : null}
                              onClose={() => { setOpenCellKey(null); setCellAnchorEl(null); }}
                            >
                              <div>
                                <StopRuleEditor
                                  open={true}
                                  onClose={() => { setOpenCellKey(null); setCellAnchorEl(null); }}
                                  mode={rule}
                                  setMode={(m) => {
                                    setRestrictions(prev => {
                                      const next = { ...prev };
                                      if (m === "normal") delete next[k];
                                      else if (m === "pickup") next[k] = { mode: "pickup" };
                                      else if (m === "dropoff") next[k] = { mode: "dropoff" };
                                      else {
                                        const existing = next[k] ?? { mode: "custom", dropoffOnlyFrom: [], pickupOnlyTo: [] };
                                        next[k] = {
                                          mode: "custom",
                                          dropoffOnlyFrom: existing.dropoffOnlyFrom ?? [],
                                          pickupOnlyTo: existing.pickupOnlyTo ?? []
                                        };
                                      }
                                      return next;
                                    });
                                  }}
                                  upstreamStops={upstreamStops}
                                  downstreamStops={downstreamStops}
                                  dropoffOnlyFrom={dropFrom}
                                  pickupOnlyTo={pickTo}
                                  onChangeDropoffOnlyFrom={(ids) =>
                                    setRestrictions(prev => ({
                                      ...prev,
                                      [k]: { mode: "custom", dropoffOnlyFrom: ids, pickupOnlyTo: prev[k]?.pickupOnlyTo ?? [] }
                                    }))
                                  }
                                  onChangePickupOnlyTo={(ids) =>
                                    setRestrictions(prev => ({
                                      ...prev,
                                      [k]: { mode: "custom", dropoffOnlyFrom: prev[k]?.dropoffOnlyFrom ?? [], pickupOnlyTo: ids }
                                    }))
                                  }
                                />
                              </div>
                            </PortalPopover>
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>

          <div style={{ paddingTop: 8, fontSize: 12, color: "#666", display: "flex", gap: 12 }}>
            <span>⤵︎ Stop</span>
            <span>⭱ Pickup-only</span>
            <span>⭳ Dropoff-only</span>
            <span>⇄ Custom (OD rules)</span>
            <span>⚙︎ Row (bulk) editor</span>
          </div>
        </div>
      </div>
    );
  };

  /* ---------- All groups ---------- */
  return (
    <>
      {groups.map((g, gi) => renderGroup(g, gi))}
      {groups.length === 0 && (
        <div className="card section" style={{ marginTop: 12 }}>
          <div className="card-body">
            <p style={{ opacity: .7 }}>No trips with stop_times for this route.</p>
          </div>
        </div>
      )}
    </>
  );
}