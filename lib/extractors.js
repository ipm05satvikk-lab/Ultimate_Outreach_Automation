// lib/extractors.js — pull emails, phones, links, social handles from bio text.
//
// Tradeoffs:
//   - Emails are extracted with a permissive but well-known pattern.
//   - Phones are restricted to clearly phone-shaped patterns (international +CC,
//     parenthesized US area code, or three groups separated by ./space/dash).
//     The older "any 10 digits" approach produced too many false positives in
//     bios that mentioned follower counts, dates, or order numbers.
//   - URLs cover http(s) and a curated list of bio-link aggregator domains.

const EMAIL_RE = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;

// Phones, four variants. Each requires a phone-shaped pattern:
//   - +CC international with optional separators
//   - US parens around the area code
//   - Three-group separated (415-555-1234 / 415.555.1234 / 415 555 1234)
//   - Bare 10-13 digit run (catches unformatted Indian / international numbers).
//     Lower bound 10 is safe because no realistic follower count, view count, or
//     year reaches 10 digits. Upper bound 13 avoids matching long platform IDs
//     (TikTok user IDs are typically 19 digits, Discord/Snowflake IDs 18-19).
const PHONE_RE = new RegExp(
  [
    "(?:\\+\\d{1,3}[ .\\-]?\\d{2,4}[ .\\-]?\\d{2,4}[ .\\-]?\\d{2,5})",
    "(?:\\(\\d{3}\\)[ .\\-]?\\d{3}[ .\\-]?\\d{4})",
    "(?:(?<![\\d.])\\d{3}[ .\\-]\\d{3}[ .\\-]\\d{4}(?![\\d.]))",
    "(?:(?<![\\d.])\\d{10,13}(?![\\d.]))",
  ].join("|"),
  "g"
);

// URLs: full http(s) URLs, or paths on known bio-link aggregator domains.
const URL_RE = /\b((?:https?:\/\/|www\.)[^\s,;<>"')\]]+|(?:linktr\.ee|stan\.store|beacons\.ai|bio\.link|allmylinks\.com|snipfeed\.co|withkoji\.com|komi\.io|magic\.ly|hoo\.be|flowcode\.com|carrd\.co|later\.com|lnk\.bio|campsite\.bio|taplink\.cc|milkshake\.app)\/[^\s,;<>"')\]]+)/gi;

// Social handle patterns inside bio text.
const SOCIAL_PATTERNS = [
  { kind: "instagram", re: /(?:instagram\.com\/|ig[:\s@]+@?|insta[:\s@]+@?)([a-z0-9_.]{2,30})/gi },
  { kind: "youtube",   re: /(?:youtube\.com\/(?:@|c\/|channel\/|user\/)|yt[:\s@]+@?)([a-z0-9_.\-]{2,40})/gi },
  { kind: "facebook",  re: /(?:facebook\.com\/|fb[:\s@]+@?)([a-z0-9_.\-]{2,40})/gi },
  { kind: "twitter",   re: /(?:twitter\.com\/|x\.com\/|twitter[:\s@]+@?|tw[:\s@]+@?)([a-z0-9_]{2,30})/gi },
];

// Things that genuinely produce 10+ digit non-phone strings in bios. Follower
// counts, view counts, and years never reach 10 digits, so they are not here.
// We look for these tokens immediately before the digit run (within ~10 chars).
const PHONE_NEGATIVE_CONTEXT = /\b(id|uid|ref|order|tracking|code|sku|invoice|isbn)\s*[:#]?\s*$/i;

function normalizeText(s) {
  if (!s) return "";
  return String(s)
    .replace(/\\u002F/g, "/")
    .replace(/\\n/g, " ")
    .replace(/\\\//g, "/")
    .replace(/\s+/g, " ")
    .trim();
}

function uniq(arr) {
  return Array.from(new Set(arr.map((v) => v.toLowerCase()))).map((v) => v);
}

/**
 * Normalize a handle reference. Strips leading @, surrounding whitespace, and
 * lowercases. Used everywhere we get a handle from the user, Apify, or Claude.
 */
function normalizeHandle(h) {
  return String(h || "").trim().replace(/^@+/, "").toLowerCase();
}

/**
 * Return true if the phone match is in a context that strongly suggests it
 * isn't a phone number. We only look ~12 chars before the match for tokens
 * like "id:", "order #", "ref:", "tracking", etc. — the realistic ways a
 * 10-13 digit run shows up in a bio that is not a phone.
 */
function looksLikeNonPhoneContext(bio, matchIndex) {
  const before = bio.slice(Math.max(0, matchIndex - 12), matchIndex);
  return PHONE_NEGATIVE_CONTEXT.test(before);
}

/**
 * Extract every interesting contact-like thing from a bio.
 * Returns an array of { kind, value, source }.
 */
function extractContacts(bioRaw, bioLink) {
  const bio = normalizeText(bioRaw);
  const out = [];

  // Emails
  const emails = uniq(bio.match(EMAIL_RE) || []);
  emails.forEach((v) => out.push({ kind: "email", value: v, source: "bio" }));

  // Phones. Strong shapes (starts with "+" or "(") are trusted unconditionally.
  // For weaker shapes (bare digit runs and 3-3-4 separated), we still reject if
  // immediately preceded by an ID-style prefix like "id:", "order #", "ref:".
  const phones = new Set();
  let m;
  PHONE_RE.lastIndex = 0;
  while ((m = PHONE_RE.exec(bio)) !== null) {
    const raw = m[0];
    const digits = raw.replace(/\D/g, "");
    if (digits.length < 10 || digits.length > 13) continue;
    const isStrongShape = raw.startsWith("+") || raw.startsWith("(");
    if (!isStrongShape && looksLikeNonPhoneContext(bio, m.index)) continue;
    phones.add(raw.trim());
  }
  for (const v of phones) out.push({ kind: "phone", value: v, source: "bio" });

  // URLs in bio text
  const urls = uniq(bio.match(URL_RE) || []);
  urls.forEach((v) => out.push({ kind: "link", value: v, source: "bio" }));

  // Dedicated bio_link field on the TikTok profile
  if (bioLink) {
    out.push({ kind: "link", value: String(bioLink), source: "bio_link" });
  }

  // Social handle mentions
  for (const { kind, re } of SOCIAL_PATTERNS) {
    re.lastIndex = 0;
    let s;
    while ((s = re.exec(bio)) !== null) {
      const handle = s[1].toLowerCase();
      if (handle && handle.length >= 2 && !/^https?$/i.test(handle)) {
        out.push({ kind, value: handle, source: "bio" });
      }
    }
  }

  // Dedup
  const seen = new Set();
  return out.filter((c) => {
    const k = `${c.kind}::${c.value.toLowerCase()}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

/**
 * Extract contacts from an HTML page body (used by the linktree worker).
 * Strips tags, normalizes whitespace, then runs the same extraction pipeline.
 */
function extractContactsFromHtml(html, sourceLabel) {
  if (!html) return [];
  const text = String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&#x?[0-9a-f]+;/gi, " ")
    .replace(/\s+/g, " ");
  // We also pick up mailto: and tel: hrefs that may have been stripped, by
  // scanning the raw HTML before tag-stripping.
  const mailtos = (String(html).match(/mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/gi) || [])
    .map((m) => m.replace(/^mailto:/i, ""));
  const tels = (String(html).match(/tel:([+0-9 .\-()]{7,30})/gi) || [])
    .map((m) => m.replace(/^tel:/i, "").trim())
    .filter((v) => v.replace(/\D/g, "").length >= 7);
  const base = extractContacts(text, null);
  for (const e of mailtos) {
    base.push({ kind: "email", value: e.toLowerCase(), source: sourceLabel || "linktree" });
  }
  for (const p of tels) {
    base.push({ kind: "phone", value: p, source: sourceLabel || "linktree" });
  }
  // Rewrite source from "bio" to the supplied source label and dedup
  const out = [];
  const seen = new Set();
  for (const c of base) {
    const k = `${c.kind}::${c.value.toLowerCase()}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push({ ...c, source: c.source === "bio" ? (sourceLabel || "linktree") : c.source });
  }
  return out;
}

module.exports = {
  extractContacts,
  extractContactsFromHtml,
  normalizeText,
  normalizeHandle,
};
