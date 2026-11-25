export async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (method !== "GET") {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body || {});
  }
  const r = await fetch(path, opts);
  const text = await r.text();
  if (!r.ok) {
    const err = new Error(text.trim() || `HTTP ${r.status}`);
    err.status = r.status;
    throw err;
  }
  // Some action endpoints answer with an empty or plain-text body.
  try { return text ? JSON.parse(text) : {}; } catch { return {}; }
}

// Storage can be blocked (private window, cleared site data); the page must work without it.
export const safeStore = {
  get(k, d) { try { const v = localStorage.getItem(k); return v === null ? d : v; } catch { return d; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* per-viewer convenience only */ } },
};
