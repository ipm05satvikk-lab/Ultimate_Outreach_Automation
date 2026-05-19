// workers/imap_listener.js — Polls Gmail inbox via IMAP for replies + bounces.
//
// Every IMAP_POLL_MS we walk through each active sender's inbox, fetch any
// messages we haven't seen before (UID > sender.imap_last_uid), parse them,
// and:
//   - If the message's In-Reply-To header matches a Message-ID we sent,
//     mark that creator_campaign as 'replied' and cancel any pending follow-up.
//   - If the message looks like a bounce (sender mailer-daemon or subject
//     "Delivery Status Notification" / "Mail Delivery Failure" etc), match
//     the original-message references in the body and mark 'bounced'.
//   - Either way, store a row in inbox_messages for audit.
//
// We use imapflow for the IMAP protocol and mailparser to parse MIME headers.
// Both work over the same App Password as SMTP. No additional Workspace
// configuration required beyond enabling IMAP in Gmail settings (default on
// for Workspace accounts unless an admin has disabled it).

const { pool } = require("../db");

const POLL_MS = parseInt(process.env.IMAP_POLL_MS || "60000", 10);
const FETCH_LIMIT = parseInt(process.env.IMAP_FETCH_LIMIT || "100", 10);
const ENABLED = process.env.IMAP_LISTENER_ENABLED !== "false";

// Heuristics for detecting bounce messages.
const BOUNCE_FROM_RE = /(mailer-daemon|postmaster)@/i;
const BOUNCE_SUBJECT_RE = /(delivery status notification|undelivered mail|mail delivery|failure notice|returned mail)/i;

function normalizeMessageId(s) {
  if (!s) return "";
  const t = String(s).trim();
  if (!t) return "";
  return t.startsWith("<") ? t : `<${t}>`;
}

/**
 * Try to extract any original Message-ID from a bounce body. Bounces typically
 * include the rejected message's Message-ID inside a quoted block.
 */
function extractOriginalMessageId(body) {
  if (!body) return null;
  const m = body.match(/Message-ID:\s*<([^>]+)>/i) || body.match(/Message-Id:\s*<([^>]+)>/i);
  return m ? `<${m[1]}>` : null;
}

async function listActiveSenders() {
  const { rows } = await pool.query(
    `SELECT * FROM email_senders WHERE active = TRUE`
  );
  return rows;
}

async function findEmailByMessageId(msgId) {
  if (!msgId) return null;
  const { rows } = await pool.query(
    `SELECT id, handle, campaign_id, stage FROM outreach_emails WHERE message_id=$1 LIMIT 1`,
    [msgId]
  );
  return rows[0] || null;
}

async function markReplied(matched, fromEmail) {
  if (!matched) return;
  await pool.query(
    `UPDATE creator_campaigns
     SET status='replied', replied_at=now(), next_send_at=NULL
     WHERE handle=$1 AND campaign_id=$2 AND status='active'`,
    [matched.handle, matched.campaign_id]
  );
  // Cancel any pending future stages
  await pool.query(
    `UPDATE outreach_emails
     SET status='cancelled'
     WHERE handle=$1 AND campaign_id=$2 AND status IN ('draft','approved') AND sent_at IS NULL`,
    [matched.handle, matched.campaign_id]
  );
  console.log(`[imap] replied: @${matched.handle} campaign ${matched.campaign_id} (from ${fromEmail})`);
}

async function markBounced(matched) {
  if (!matched) return;
  await pool.query(
    `UPDATE creator_campaigns
     SET status='bounced', bounced_at=now(), next_send_at=NULL
     WHERE handle=$1 AND campaign_id=$2 AND status='active'`,
    [matched.handle, matched.campaign_id]
  );
  await pool.query(
    `UPDATE outreach_emails
     SET status='bounced' WHERE id=$1`,
    [matched.id]
  );
  // Cancel pending future stages too — auto-stop on bounce per user preference
  await pool.query(
    `UPDATE outreach_emails
     SET status='cancelled'
     WHERE handle=$1 AND campaign_id=$2 AND status IN ('draft','approved') AND sent_at IS NULL`,
    [matched.handle, matched.campaign_id]
  );
  console.log(`[imap] bounced: @${matched.handle} campaign ${matched.campaign_id}`);
}

async function storeInbox(sender, uid, parsed, isBounce, matchedEmailId) {
  await pool.query(
    `INSERT INTO inbox_messages
       (sender_id, uid, message_id, in_reply_to, references_header,
        from_email, to_email, subject, body_snippet, is_bounce, matched_email_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     ON CONFLICT (sender_id, uid) DO NOTHING`,
    [
      sender.id, uid,
      parsed.messageId || null,
      parsed.inReplyTo || null,
      parsed.references || null,
      parsed.fromEmail || null,
      parsed.toEmail || null,
      parsed.subject || null,
      (parsed.text || "").slice(0, 1000),
      isBounce,
      matchedEmailId,
    ]
  );
}

async function processSender(sender) {
  // Lazy-load imapflow so the module imports don't blow up if the dep is
  // missing in environments where imap isn't needed.
  const { ImapFlow } = require("imapflow");
  const { simpleParser } = require("mailparser");

  const client = new ImapFlow({
    host: sender.imap_host,
    port: sender.imap_port,
    secure: true,
    auth: { user: sender.smtp_user, pass: sender.smtp_app_password },
    logger: false,
  });

  await client.connect();
  let lock;
  try {
    lock = await client.getMailboxLock("INBOX");

    // Build a search: messages with UID greater than what we last saw.
    const minUid = (sender.imap_last_uid || 0) + 1;
    const range = `${minUid}:*`;
    let maxUidSeen = sender.imap_last_uid || 0;

    let processed = 0;
    for await (const msg of client.fetch(range, { envelope: true, source: true, uid: true })) {
      processed++;
      if (processed > FETCH_LIMIT) break;
      const uid = msg.uid;
      if (uid <= sender.imap_last_uid) continue;
      maxUidSeen = Math.max(maxUidSeen, uid);

      let parsed = null;
      try {
        parsed = await simpleParser(msg.source);
      } catch (e) {
        console.warn(`[imap] parse failed uid=${uid}: ${e.message}`);
        continue;
      }

      const fromEmail = parsed.from?.value?.[0]?.address?.toLowerCase() || null;
      const toEmail   = parsed.to?.value?.[0]?.address?.toLowerCase() || null;
      const subject   = parsed.subject || "";
      const messageId = parsed.messageId || null;
      const inReplyTo = parsed.inReplyTo || null;
      const references = Array.isArray(parsed.references)
        ? parsed.references.join(" ")
        : (parsed.references || null);
      const bodyText = parsed.text || "";

      const looksLikeBounce =
        (fromEmail && BOUNCE_FROM_RE.test(fromEmail)) ||
        (subject && BOUNCE_SUBJECT_RE.test(subject));

      let matched = null;
      let isBounce = false;

      if (looksLikeBounce) {
        // Bounces: pull the original Message-ID from the body
        const originalMsgId = extractOriginalMessageId(bodyText);
        if (originalMsgId) {
          matched = await findEmailByMessageId(originalMsgId);
          if (matched) {
            isBounce = true;
            await markBounced(matched);
          }
        }
      } else if (inReplyTo) {
        matched = await findEmailByMessageId(normalizeMessageId(inReplyTo));
        if (matched) {
          await markReplied(matched, fromEmail);
        }
      }

      await storeInbox(
        sender,
        uid,
        { messageId, inReplyTo, references, fromEmail, toEmail, subject, text: bodyText },
        isBounce,
        matched ? matched.id : null,
      );
    }

    if (maxUidSeen > (sender.imap_last_uid || 0)) {
      await pool.query(
        `UPDATE email_senders SET imap_last_uid=$1 WHERE id=$2`,
        [maxUidSeen, sender.id]
      );
    }
  } finally {
    if (lock) lock.release();
    await client.logout().catch(() => {});
  }
}

async function loop() {
  if (!ENABLED) {
    console.log("[imap] disabled");
    return;
  }
  try {
    const senders = await listActiveSenders();
    for (const sender of senders) {
      try {
        await processSender(sender);
      } catch (e) {
        console.warn(`[imap] sender ${sender.id} (${sender.from_email}): ${e.message}`);
      }
    }
  } catch (e) {
    console.error("[imap] loop error:", e);
  } finally {
    setTimeout(loop, POLL_MS);
  }
}

function start() {
  console.log(`[imap] starting — poll ${POLL_MS}ms`);
  loop();
}

module.exports = { start };
