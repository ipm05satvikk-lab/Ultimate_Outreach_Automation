// lib/smtp.js — Wrapper around nodemailer for sending one outreach email.
//
// Threading: pass parent.message_id to set In-Reply-To and References headers.
// Gmail will then group follow-ups under the original conversation in the
// recipient's inbox. The subject of the follow-up matters less than the
// headers — Gmail threads primarily on Message-ID, with subject as a fallback.
//
// Attachments: real attachments only (paperclip in Gmail). We pass them as
// nodemailer's { filename, content, contentType } objects; nodemailer builds
// the multipart/mixed MIME for us. No inline CID images.

const nodemailer = require("nodemailer");

const transporterCache = new Map();

function getTransporter(sender) {
  const key = `${sender.id}:${sender.smtp_host}:${sender.smtp_port}:${sender.smtp_user}`;
  if (transporterCache.has(key)) return transporterCache.get(key);
  const t = nodemailer.createTransport({
    host: sender.smtp_host,
    port: sender.smtp_port,
    secure: sender.smtp_port === 465,
    auth: { user: sender.smtp_user, pass: sender.smtp_app_password },
    // Gmail requires STARTTLS on 587; nodemailer defaults handle this correctly.
    pool: true,
    maxConnections: 3,
    maxMessages: 100,
  });
  transporterCache.set(key, t);
  return t;
}

function invalidateTransporter(senderId) {
  for (const k of transporterCache.keys()) {
    if (k.startsWith(`${senderId}:`)) transporterCache.delete(k);
  }
}

/**
 * Send one email.
 *
 * @param {object} sender - row from email_senders
 * @param {object} email - { to_email, subject, body }
 * @param {object|null} parent - { message_id, subject } of the email this is replying to, or null
 * @param {Array} attachments - [{ filename, content: Buffer, contentType }]
 * @returns { messageId: string, response: string }
 */
async function sendOne(sender, email, parent, attachments) {
  const transporter = getTransporter(sender);

  const headers = {};
  let subject = email.subject;
  if (parent && parent.message_id) {
    // Gmail will thread on the In-Reply-To + References chain alone, but we
    // also normalize the subject to "Re: <original>" because some clients use
    // subject heuristics in addition to headers.
    headers["In-Reply-To"] = parent.message_id.startsWith("<") ? parent.message_id : `<${parent.message_id}>`;
    headers["References"] = headers["In-Reply-To"];
    if (parent.subject && !/^re:/i.test(subject)) {
      subject = `Re: ${parent.subject}`;
    }
  }

  const fromHeader = sender.from_name
    ? `"${sender.from_name.replace(/"/g, "")}" <${sender.from_email}>`
    : sender.from_email;

  const mailOptions = {
    from: fromHeader,
    to: email.to_email,
    subject,
    text: email.body,                // plaintext primary
    headers,
    attachments: (attachments || []).map((a) => ({
      filename: a.filename,
      content: a.content,
      contentType: a.contentType || a.mime_type,
    })),
  };

  const info = await transporter.sendMail(mailOptions);
  return { messageId: info.messageId, response: info.response };
}

/**
 * Quick health check — verifies SMTP creds are valid without sending anything.
 * Used by the "Add Sender" form.
 */
async function verifyCredentials(sender) {
  const transporter = nodemailer.createTransport({
    host: sender.smtp_host,
    port: sender.smtp_port,
    secure: sender.smtp_port === 465,
    auth: { user: sender.smtp_user, pass: sender.smtp_app_password },
  });
  await transporter.verify();
}

module.exports = { sendOne, verifyCredentials, invalidateTransporter };
