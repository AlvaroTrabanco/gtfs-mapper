// ESM Netlify function (Node 18+)
// Needs env vars on the Netlify *admin site*:
// ADMIN_KEY, GH_TOKEN, REPO_OWNER, REPO_NAME, TARGET_BRANCH, OVERRIDES_PATH

import { Octokit } from "@octokit/rest";

export const handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method Not Allowed" };
    }

    const {
      key,                 // admin password
      overridesJson,       // string (raw JSON)
      commitMessage        // optional
    } = JSON.parse(event.body || "{}");

    // simple auth gate
    if (!key || key !== process.env.ADMIN_KEY) {
      return { statusCode: 401, body: "Unauthorized" };
    }

    if (!overridesJson || typeof overridesJson !== "string") {
      return { statusCode: 400, body: "Bad Request: overridesJson (string) required" };
    }

    const owner  = process.env.REPO_OWNER;
    const repo   = process.env.REPO_NAME;
    const path   = process.env.OVERRIDES_PATH || "automation/overrides.json";
    const branch = process.env.TARGET_BRANCH || "main";
    const token  = process.env.GH_TOKEN;

    if (!owner || !repo || !token) {
      return { statusCode: 500, body: "Server misconfigured: missing repo or token envs" };
    }

    const octokit = new Octokit({ auth: token });

    // Look up existing file SHA (if exists)
    let existingSha = undefined;
    try {
      const res = await octokit.repos.getContent({ owner, repo, path, ref: branch });
      if (Array.isArray(res.data)) {
        // path is a directory — unexpected
        return { statusCode: 409, body: "Conflict: path refers to a directory" };
      }
      existingSha = res.data.sha;
    } catch (e) {
      // 404 means it doesn't exist yet — that's fine
      if (e.status !== 404) {
        return { statusCode: e.status || 500, body: `GitHub error: ${e.message}` };
      }
    }

    const contentB64 = Buffer.from(overridesJson, "utf8").toString("base64");
    const message = commitMessage || `chore: update overrides.json via admin uploader`;

    const { data } = await octokit.repos.createOrUpdateFileContents({
      owner,
      repo,
      path,
      message,
      content: contentB64,
      branch,
      sha: existingSha, // include only if exists to update; omit to create
    });

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        path,
        branch,
        commit: data.commit?.sha,
        html_url: data.content?.html_url || null,
      }),
    };
  } catch (err) {
    return { statusCode: 500, body: `Error: ${err.message}` };
  }
};