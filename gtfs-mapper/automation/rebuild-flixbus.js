// gtfs-mapper/automation/rebuild-flixbus.js
// Minimal placeholder: writes report.json and a small valid zip to OUT_DIR.
// You can replace the internals later with the real build.

import { execSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const OUT_DIR = process.env.OUT_DIR || "site";
const OUT_ZIP = process.env.OUT_ZIP || "flixbus_eu_compiled.zip";

fs.mkdirSync(OUT_DIR, { recursive: true });

// 1) Write a simple report
const report = {
  name: "Flixbus GTFS (compiled)",
  generatedAt: new Date().toISOString(),
  feeds: [],       // fill with real feeds later
  notes: "Placeholder build — replace with real compilation logic.",
};
fs.writeFileSync(path.join(OUT_DIR, "report.json"), JSON.stringify(report, null, 2));

// 2) Create a tiny valid zip containing a README (uses system `zip`)
const tmpDir = fs.mkdtempSync(path.join(process.cwd(), "tmp-flixbus-"));
const readme = path.join(tmpDir, "README.txt");
fs.writeFileSync(readme, "Placeholder Flixbus GTFS bundle. Replace with real output.");

const outZipPath = path.join(OUT_DIR, OUT_ZIP);
// -j = junk paths, -q = quiet, overwrites if exists
execSync(`zip -jq "${outZipPath}" "${readme}"`, { stdio: "inherit" });

// Cleanup temp files (best-effort)
try { fs.unlinkSync(readme); fs.rmdirSync(tmpDir); } catch {}
console.log(`Wrote ${path.relative(process.cwd(), outZipPath)} and report.json`);