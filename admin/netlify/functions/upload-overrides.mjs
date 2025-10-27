// ESM Netlify function (Node 18+)
// Env vars on Netlify admin site: ADMIN_KEY, GH_TOKEN, REPO_OWNER, REPO_NAME, TARGET_BRANCH
import { Octokit } from "@octokit/rest";

function normPath(p) {
  const clean = String(p || "").replace(/\\/g, "/").replace(/^\/+/, "");
  if (clean.includes("..")) throw new Error("Invalid path");
  return clean;
}

export const handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method Not Allowed" };
    }

    const body = JSON.parse(event.body || "{}");
    const {
      key,
      op,                // "readFile" | "writeFile" | "deleteFile" | "moveFile" | "upsertOverridesSlug" | "deleteSlugFromOverrides" | "renameSlugInOverrides"
      path,
      content,           // string for writeFile
      src, dest,         // for moveFile
      slug,              // for overrides ops
      newSlug,           // for renameSlugInOverrides
      overridesJson,     // string (raw JSON) when default write
      commitMessage
    } = body;

    if (!key || key !== process.env.ADMIN_KEY) return { statusCode: 401, body: "Unauthorized" };

    const owner  = process.env.REPO_OWNER;
    const repo   = process.env.REPO_NAME;
    const branch = process.env.TARGET_BRANCH || "main";
    const token  = process.env.GH_TOKEN;
    if (!owner || !repo || !token) {
      return { statusCode: 500, body: "Server misconfigured: missing REPO_OWNER, REPO_NAME or GH_TOKEN" };
    }

    const octokit = new Octokit({ auth: token });

    async function getFile(pth) {
      const p = normPath(pth);
      const res = await octokit.repos.getContent({ owner, repo, path: p, ref: branch });
      if (Array.isArray(res.data)) throw new Error("Path refers to a directory");
      const { sha, content: b64, encoding } = res.data;
      const buff = Buffer.from(b64, encoding === "base64" ? "base64" : "utf8");
      return { sha, text: buff.toString("utf8") };
    }

    async function putFile(pth, text, msg) {
      const p = normPath(pth);
      let sha;
      try {
        const res = await octokit.repos.getContent({ owner, repo, path: p, ref: branch });
        if (!Array.isArray(res.data)) sha = res.data.sha;
      } catch (e) {
        if (e.status !== 404) throw e;
      }
      const contentB64 = Buffer.from(String(text ?? ""), "utf8").toString("base64");
      const { data } = await octokit.repos.createOrUpdateFileContents({
        owner, repo, path: p, message: msg, content: contentB64, branch, ...(sha ? { sha } : {})
      });
      return { ok: true, path: p, branch, commit: data.commit?.sha, html_url: data.content?.html_url || null };
    }

    async function deleteFile(pth, msg) {
      const p = normPath(pth);
      const res = await octokit.repos.getContent({ owner, repo, path: p, ref: branch }).catch(e => {
        if (e.status === 404) return null; throw e;
      });
      if (!res) return { ok: true, deleted: false, path: p };
      const sha = Array.isArray(res.data) ? null : res.data.sha;
      if (!sha) return { ok: true, deleted: false, path: p };
      const { data } = await octokit.repos.deleteFile({ owner, repo, path: p, message: msg, branch, sha });
      return { ok: true, deleted: true, path: p, commit: data.commit?.sha };
    }

    // --- API ops ---
    if (op === "readFile") {
      if (!path) return { statusCode: 400, body: "Bad Request: path required for readFile" };
      const { text } = await getFile(path);
      return { statusCode: 200, body: JSON.stringify({ ok: true, content: text }) };
    }

    if (op === "writeFile") {
      if (!path || typeof content !== "string") return { statusCode: 400, body: "Bad Request: path+content required for writeFile" };
      const msg = commitMessage || `chore: update ${normPath(path)}`;
      const result = await putFile(path, content, msg);
      return { statusCode: 200, body: JSON.stringify(result) };
    }

    if (op === "deleteFile") {
      if (!path) return { statusCode: 400, body: "Bad Request: path required for deleteFile" };
      const msg = commitMessage || `chore: delete ${normPath(path)}`;
      const result = await deleteFile(path, msg);
      return { statusCode: 200, body: JSON.stringify(result) };
    }

    if (op === "moveFile") {
      if (!src || !dest) return { statusCode: 400, body: "Bad Request: src & dest required for moveFile" };
      const from = normPath(src), to = normPath(dest);
      let text = "";
      try { text = (await getFile(from)).text; } catch (e) {
        if (e.status === 404) return { statusCode: 404, body: JSON.stringify({ ok:false, error:"Source not found" }) };
        throw e;
      }
      const msg = commitMessage || `chore: move ${from} -> ${to}`;
      await putFile(to, text, msg);
      await deleteFile(from, msg);
      return { statusCode: 200, body: JSON.stringify({ ok:true, src: from, dest: to }) };
    }

    // upsert one slug in legacy gtfs-mapper/automation/overrides.json
    if (op === "upsertOverridesSlug") {
      if (!slug) return { statusCode: 400, body: "Bad Request: slug required" };
      if (typeof content !== "string") return { statusCode: 400, body: "Bad Request: content (stringified JSON) required" };
      const p = "gtfs-mapper/automation/overrides.json";
      let obj = { overrides: {} };
      try {
        const txt = (await getFile(p)).text;
        const parsed = JSON.parse(txt);
        obj = parsed && typeof parsed === "object" ? parsed : obj;
        if (!obj.overrides || typeof obj.overrides !== "object") obj.overrides = {};
      } catch (e) {
        if (e.status !== 404) throw e; // create new if missing
      }
      obj.overrides[slug] = JSON.parse(content);
      const msg = commitMessage || `chore: upsert overrides for ${slug}`;
      const res = await putFile(p, JSON.stringify(obj, null, 2), msg);
      return { statusCode: 200, body: JSON.stringify(res) };
    }

    if (op === "deleteSlugFromOverrides") {
      if (!slug) return { statusCode: 400, body: "Bad Request: slug required" };
      const p = "gtfs-mapper/automation/overrides.json";
      let obj;
      try {
        const txt = (await getFile(p)).text;
        obj = JSON.parse(txt);
      } catch (e) {
        if (e.status === 404) return { statusCode: 200, body: JSON.stringify({ ok:true, deleted:false }) };
        throw e;
      }
      if (obj && obj.overrides && obj.overrides[slug]) {
        delete obj.overrides[slug];
        const msg = commitMessage || `chore: delete ${slug} from overrides.json`;
        const res = await putFile(p, JSON.stringify(obj, null, 2), msg);
        return { statusCode: 200, body: JSON.stringify(res) };
      }
      return { statusCode: 200, body: JSON.stringify({ ok:true, deleted:false }) };
    }

    if (op === "renameSlugInOverrides") {
      if (!slug || !newSlug) return { statusCode: 400, body: "Bad Request: slug & newSlug required" };
      const p = "gtfs-mapper/automation/overrides.json";
      let obj;
      try {
        const txt = (await getFile(p)).text;
        obj = JSON.parse(txt);
      } catch (e) {
        if (e.status === 404) return { statusCode: 200, body: JSON.stringify({ ok:true, changed:false }) };
        throw e;
      }
      if (!obj || !obj.overrides || !obj.overrides[slug]) {
        return { statusCode: 200, body: JSON.stringify({ ok:true, changed:false }) };
      }
      obj.overrides[newSlug] = obj.overrides[slug];
      delete obj.overrides[slug];
      const msg = commitMessage || `chore: rename overrides slug ${slug} -> ${newSlug}`;
      const res = await putFile(p, JSON.stringify(obj, null, 2), msg);
      return { statusCode: 200, body: JSON.stringify(res) };
    }

    // Default: write a per-file overrides JSON
    if (typeof overridesJson !== "string") return { statusCode: 400, body: "Bad Request: overridesJson (string) required" };
    const targetPath = normPath(path || "gtfs-mapper/automation/overrides.json");
    const filename = targetPath.split("/").pop() || "overrides.json";
    const msg = commitMessage || `chore: update ${filename} via admin uploader`;
    const result = await putFile(targetPath, overridesJson, msg);
    return { statusCode: 200, body: JSON.stringify(result) };
  } catch (err) {
    const code = err.status && Number.isInteger(err.status) ? err.status : 500;
    return { statusCode: code, body: `Error: ${err.message}` };
  }
};