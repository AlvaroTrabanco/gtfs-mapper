// ESM Netlify function (Node 18+)
// Env vars on Netlify admin site:
// ADMIN_KEY, GH_TOKEN, REPO_OWNER, REPO_NAME, TARGET_BRANCH

import { Octokit } from "@octokit/rest";

// Basic path guard: avoid absolute paths and parent traversal
function normalizeRepoPath(p) {
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
      key,                 // admin password
      op,                  // "readFile" | "writeFile" | undefined (default: write overrides)
      path,                // repo path to read/write (required for readFile/writeFile; optional for overrides)
      overridesJson,       // string (raw JSON) when writing overrides
      content,             // string content for writeFile
      commitMessage        // optional message
    } = body;

    // simple auth gate
    if (!key || key !== process.env.ADMIN_KEY) {
      return { statusCode: 401, body: "Unauthorized" };
    }

    const owner  = process.env.REPO_OWNER;
    const repo   = process.env.REPO_NAME;
    const branch = process.env.TARGET_BRANCH || "main";
    const token  = process.env.GH_TOKEN;

    if (!owner || !repo || !token) {
      return {
        statusCode: 500,
        body: "Server misconfigured: missing REPO_OWNER, REPO_NAME or GH_TOKEN",
      };
    }

    const octokit = new Octokit({ auth: token });

    // Helpers
    async function getFile(pathInRepo) {
      const p = normalizeRepoPath(pathInRepo);
      const res = await octokit.repos.getContent({ owner, repo, path: p, ref: branch });
      if (Array.isArray(res.data)) throw new Error("Path refers to a directory");
      const { sha, content: b64, encoding } = res.data;
      const buff = Buffer.from(b64, encoding === "base64" ? "base64" : "utf8");
      return { sha, text: buff.toString("utf8") };
    }

    async function putFile(pathInRepo, text, message) {
      const p = normalizeRepoPath(pathInRepo);
      // Lookup existing SHA (if any)
      let existingSha;
      try {
        const res = await octokit.repos.getContent({ owner, repo, path: p, ref: branch });
        if (!Array.isArray(res.data)) existingSha = res.data.sha;
      } catch (e) {
        if (e.status !== 404) throw e; // 404 means new file is fine
      }
      const contentB64 = Buffer.from(String(text ?? ""), "utf8").toString("base64");
      const { data } = await octokit.repos.createOrUpdateFileContents({
        owner,
        repo,
        path: p,
        message,
        content: contentB64,
        branch,
        ...(existingSha ? { sha: existingSha } : {}),
      });
      return {
        ok: true,
        path: p,
        branch,
        commit: data.commit?.sha,
        html_url: data.content?.html_url || null,
      };
    }

    // Ops
    if (op === "readFile") {
      if (!path) return { statusCode: 400, body: "Bad Request: path required for readFile" };
      const { text } = await getFile(path);
      return { statusCode: 200, body: JSON.stringify({ ok: true, content: text }) };
    }

    if (op === "writeFile") {
      if (!path) return { statusCode: 400, body: "Bad Request: path required for writeFile" };
      if (typeof content !== "string") {
        return { statusCode: 400, body: "Bad Request: content (string) required for writeFile" };
      }
      const msg = commitMessage || `chore: update ${normalizeRepoPath(path)}`;
      const result = await putFile(path, content, msg);
      return { statusCode: 200, body: JSON.stringify(result) };
    }

    // Default: write overrides
    // Accept a custom target path; default to the legacy location if none provided.
    if (typeof overridesJson !== "string") {
      return { statusCode: 400, body: "Bad Request: overridesJson (string) required" };
    }

    const targetPath = normalizeRepoPath(
      path || "gtfs-mapper/automation/overrides.json"
    );

    // Friendly default commit message that reflects the specific file
    const filename = targetPath.split("/").pop() || "overrides.json";
    const message =
      commitMessage || `chore: update ${filename} via admin uploader`;

    const result = await putFile(targetPath, overridesJson, message);
    return { statusCode: 200, body: JSON.stringify(result) };

  } catch (err) {
    const code = err.status && Number.isInteger(err.status) ? err.status : 500;
    return { statusCode: code, body: `Error: ${err.message}` };
  }
};