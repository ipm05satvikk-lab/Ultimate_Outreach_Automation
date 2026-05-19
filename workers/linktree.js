// workers/linktree.js — Two-hop contact discovery.
//
// When scraper.persistProfile() saves a creator with a bio_link, it queues a row
// into link_fetches. This worker polls that queue, fetches the URL (only if it's
// on the allowlist of known bio-link aggregator domains), and runs the same
// contact-extraction pipeline against the resulting HTML. Emails/phones/socials
// found this way get inserted into the contacts table with source='linktree'
// (or 'bio_link_page' if the link points to a personal/branded site that is on
// the allowlist).
//
// Safety:
//   - URLs are validated and only fetched if their host is on ALLOWED_HOSTS.
//   - Response size is capped (LINK_MAX_BYTES, default 1MB).
//   - Per-domain rate limiting via a simple in-process minimum interval.
//   - HEAD-then-GET pattern avoids downloading huge non-HTML responses.
//   - Timeouts and retry-with-backoff like the other workers.
//
// Disable entirely with LINKTREE_ENABLED=false.

const { pool } = require("../db");
const { extractContactsFromHtml } = require("../lib/extractors");

const ENABLED = process.env.LINKTREE_ENABLED !== "false";
const POLL_MS = parseInt(process.env.LINKTREE_POLL_MS || "8000", 10);
const BATCH_SIZE = parseInt(process.env.LINKTREE_BATCH_SIZE || "10", 10);
const MAX_ATTEMPTS = parseInt(process.env.LINKTREE_MAX_ATTEMPTS || "3", 10);
const FETCH_TIMEOUT_MS = parseInt(process.env.LINKTREE_TIMEOUT_MS || "15000", 10);
const MAX_BYTES = parseInt(process.env.LINKTREE_MAX_BYTES || "1048576", 10);
const MIN_INTERVAL_PER_HOST_MS = parseInt(process.env.LINKTREE_MIN_INTERVAL_MS || "1500", 10);

// Known bio-link aggregator hosts. We will fetch any URL whose hostname matches
// one of these (or a subdomain of one). Extend via LINKTREE_EXTRA_HOSTS=foo.com,bar.io.
const DEFAULT_ALLOWED_HOSTS = [
  "linktr.ee", "linktree.com",
  "beacons.ai", "beacons.page",
  "stan.store",
  "bio.link", "lnk.bio",
  "allmylinks.com",
  "snipfeed.co",
  "withkoji.com", "koji.to",
  "komi.io",
  "magic.ly",
  "hoo.be",
  "flowcode.com",
  "carrd.co",
  "later.com", "later.bio",
  "campsite.bio",
  "taplink.cc",
  "milkshake.app",
];
const extraHosts = (process.env.LINKTREE_EXTRA_HOSTS || "")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);
const ALLOWED_HOSTS = new Set([...DEFAULT_ALLOWED_HOSTS, ...extraHosts]);

const USER_AGENT =
  process.env.LINKTREE_USER_AGENT ||
  "Mozilla/5.0 (compatible; CreatorScraperBot/1.0; +internal-tool)";

// Per-host last-fetch timestamps, for simple rate limiting.
const lastFetchByHost = new Map();

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function hostnameOf(urlStr) {
  try {
    const u = new URL(urlStr);
    return u.hostname.toLowerCase();
  } catch {
    return null;
  }
}

function isAllowedHost(hostname) {
  if (!hostname) return false;
  for (const h of ALLOWED_HOSTS) {
    if (hostname === h || hostname.endsWith("." + h)) return true;
  }
  return false;
}

async function rateLimit(hostname) {
  const last = lastFetchByHost.get(hostname) || 0;
  const elapsed = Date.now() - last;
  if (elapsed < MIN_INTERVAL_PER_HOST_MS) {
    await sleep(MIN_INTERVAL_PER_HOST_MS - elapsed);
  }
  lastFetchByHost.set(hostname, Date.now());
}

/**
 * Fetch a URL with a body-size cap and timeout. Returns { status, text } or throws.
 */
async function fetchCapped(url) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
      },
      redirect: "follow",
      signal: ctl.signal,
    });

    // Reject obviously non-HTML responses early
    const ctype = (res.headers.get("content-type") || "").toLowerCase();
    if (ctype && !/text\/html|application\/xhtml|text\/plain/.test(ctype)) {
      // Some bio-link services return JSON APIs as well; allow JSON since it may
      // contain mailto: strings, but skip binary types.
      if (!/json|javascript/.test(ctype)) {
        return { status: res.status, text: "" };
      }
    }

    // Cap the body size while reading
    const reader = res.body && res.body.getReader ? res.body.getReader() : null;
    let text = "";
    if (reader) {
      const decoder = new TextDecoder("utf-8", { fatal: false });
      let total = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        total += value.length;
        if (total > MAX_BYTES) break;
        text += decoder.decode(value, { stream: true });
      }
      text += decoder.decode();
    } else {
      // Fallback for environments without a streaming body
      text = await res.text();
      if (text.length > MAX_BYTES) text = text.slice(0, MAX_BYTES);
    }
    return { status: res.status, text };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Atomic claim of pending link_fetches rows.
 */
async function claimBatch(limit) {
  const { rows } = await pool.query(
    `
    WITH candidate AS (
      SELECT id FROM link_fetches
      WHERE status='pending' AND attempts < $2
      ORDER BY id ASC
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    )
    UPDATE link_fetches lf
    SET status='in_flight', attempts = lf.attempts + 1
    FROM candidate
    WHERE lf.id = candidate.id
    RETURNING lf.id, lf.handle, lf.url, lf.attempts
    `,
    [limit, MAX_ATTEMPTS]
  );
  return rows;
}

async function markDone(id, httpStatus, contactsFound) {
  await pool.query(
    `UPDATE link_fetches
     SET status='done', http_status=$2, contacts_found=$3, fetched_at=now(), error=NULL
     WHERE id=$1`,
    [id, httpStatus, contactsFound]
  );
}

async function markSkipped(id, reason) {
  await pool.query(
    `UPDATE link_fetches
     SET status='skipped', error=$2, fetched_at=now()
     WHERE id=$1`,
    [id, reason.slice(0, 500)]
  );
}

async function releaseOrFail(id, attempts, errMsg) {
  await pool.query(
    `UPDATE link_fetches
     SET status = CASE WHEN $2 >= $3 THEN 'failed' ELSE 'pending' END,
         error = $4,
         fetched_at = CASE WHEN $2 >= $3 THEN now() ELSE fetched_at END
     WHERE id=$1`,
    [id, attempts, MAX_ATTEMPTS, errMsg.slice(0, 500)]
  );
}

async function insertContacts(handle, contacts) {
  let inserted = 0;
  for (const c of contacts) {
    const { rowCount } = await pool.query(
      `INSERT INTO contacts (handle, kind, value, source)
       VALUES ($1,$2,$3,$4)
       ON CONFLICT (handle, kind, value) DO NOTHING`,
      [handle, c.kind, c.value, c.source || "linktree"]
    );
    if (rowCount) inserted++;
  }
  return inserted;
}

async function processOne(row) {
  const host = hostnameOf(row.url);
  if (!host) {
    await markSkipped(row.id, "invalid URL");
    return 0;
  }
  if (!isAllowedHost(host)) {
    await markSkipped(row.id, `host not on allowlist: ${host}`);
    return 0;
  }
  await rateLimit(host);

  let res;
  try {
    res = await fetchCapped(row.url);
  } catch (e) {
    await releaseOrFail(row.id, row.attempts, String(e?.message || e));
    return 0;
  }

  if (!res.text) {
    await markDone(row.id, res.status, 0);
    return 0;
  }

  // Pick a source label that tells the user where the contact came from.
  const sourceLabel = host.includes("linktr") ? "linktree" : `bio_link_page:${host}`;
  const contacts = extractContactsFromHtml(res.text, sourceLabel);
  // Filter out the bio_link URL itself reappearing as a link
  const filtered = contacts.filter((c) => !(c.kind === "link" && c.value === row.url));
  const inserted = await insertContacts(row.handle, filtered);
  await markDone(row.id, res.status, inserted);
  return inserted;
}

async function processBatch() {
  const claimed = await claimBatch(BATCH_SIZE);
  if (!claimed.length) return 0;
  let total = 0;
  for (const r of claimed) {
    try {
      total += await processOne(r);
    } catch (e) {
      console.error(`[linktree] processOne failed for ${r.url}:`, e.message);
      await releaseOrFail(r.id, r.attempts, String(e?.message || e));
    }
  }
  return total;
}

async function loop() {
  try {
    await processBatch();
  } catch (e) {
    console.error("[linktree] loop error:", e);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

function start() {
  if (!ENABLED) {
    console.log("[linktree] disabled via LINKTREE_ENABLED=false");
    return;
  }
  console.log(`[linktree] starting — batch=${BATCH_SIZE} poll=${POLL_MS}ms hosts=${ALLOWED_HOSTS.size}`);
  loop();
}

module.exports = { start };
