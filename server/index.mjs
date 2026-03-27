import express from "express";
import dotenv from "dotenv";
import { GoogleGenAI, Type } from "@google/genai";
import Database from "better-sqlite3";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const envPath = path.join(__dirname, "..", ".env");
dotenv.config({ path: envPath });

const app = express();
const port = Number(process.env.PORT || 3001);
const KNOWLEDGE_FILE_PATH = path.join(__dirname, "salonKnowledge.json");
const DATA_DIR_PATH = path.join(__dirname, "data");
const SQLITE_DB_PATH = process.env.IA_DB_PATH?.trim() || path.join(DATA_DIR_PATH, "ia_agendamento.sqlite");
const MAX_WHATSAPP_HISTORY_MESSAGES = 20;
const BOOKING_CONFIRMATION_TTL_MS = 20 * 60 * 1000;
const HUMAN_HANDOFF_TTL_MS = 12 * 60 * 60 * 1000;
const TRINKS_MAX_RETRIES = Math.max(1, Number(process.env.TRINKS_MAX_RETRIES || 4));
const TRINKS_RETRY_BASE_MS = Math.max(200, Number(process.env.TRINKS_RETRY_BASE_MS || 700));
const BOOKING_SEQUENCE_GAP_MS = Math.max(0, Number(process.env.BOOKING_SEQUENCE_GAP_MS || 450));
const TRINKS_STATUS_ID_CANCELLED = Math.max(1, Number(process.env.TRINKS_STATUS_ID_CANCELLED || 9));
const UNSUPPORTED_MESSAGE_REPLY =
  toNonEmptyString(process.env.WHATSAPP_UNSUPPORTED_MESSAGE_REPLY) ||
  "Recebi sua mensagem, mas no momento consigo ler apenas texto. Pode me escrever por texto, por favor?";
const whatsappConversations = new Map();
const recentWebhookMessages = new Map();
const pendingBookingConfirmations = new Map();
const humanHandoffSessions = new Map();
const WEBHOOK_DEDUPE_WINDOW_MS = 30_000;
const db = initDatabase();

function initDatabase() {
  mkdirSync(DATA_DIR_PATH, { recursive: true });
  const database = new Database(SQLITE_DB_PATH);
  database.pragma("journal_mode = WAL");

  database.exec(`
    CREATE TABLE IF NOT EXISTS whatsapp_messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      phone TEXT NOT NULL,
      role TEXT NOT NULL,
      content TEXT NOT NULL,
      sender_name TEXT DEFAULT '',
      at TEXT NOT NULL,
      source TEXT DEFAULT 'runtime'
    );

    CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_phone_at
      ON whatsapp_messages(phone, at DESC);

    CREATE TABLE IF NOT EXISTS appointment_audit (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_type TEXT NOT NULL,
      status TEXT NOT NULL,
      establishment_id INTEGER,
      appointment_id INTEGER,
      confirmation_code TEXT,
      client_phone TEXT,
      client_name TEXT,
      service_name TEXT,
      professional_name TEXT,
      appointment_date TEXT,
      appointment_time TEXT,
      request_payload TEXT,
      response_payload TEXT,
      error_message TEXT,
      created_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_appointment_audit_created_at
      ON appointment_audit(created_at DESC);

    CREATE INDEX IF NOT EXISTS idx_appointment_audit_phone
      ON appointment_audit(client_phone);

    CREATE TABLE IF NOT EXISTS webhook_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event TEXT,
      instance_name TEXT,
      sender_raw TEXT,
      sender_number TEXT,
      sender_name TEXT,
      message_id TEXT,
      message_type TEXT,
      message_text TEXT,
      status TEXT NOT NULL,
      reason TEXT,
      details TEXT,
      received_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_webhook_events_received_at
      ON webhook_events(received_at DESC);

    CREATE INDEX IF NOT EXISTS idx_webhook_events_sender
      ON webhook_events(sender_number, received_at DESC);

    CREATE TABLE IF NOT EXISTS client_phone_map (
      phone TEXT PRIMARY KEY,
      client_id INTEGER NOT NULL,
      client_name TEXT,
      updated_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_client_phone_map_client
      ON client_phone_map(client_id);
  `);

  return database;
}

function safeJsonStringify(value) {
  try {
    return JSON.stringify(value ?? null);
  } catch {
    return JSON.stringify({ nonSerializable: true });
  }
}

function escapeInvalidJsonBackslashes(value) {
  return String(value || "").replace(/\\(?!["\\/bfnrtu])/g, "\\\\");
}

function tryParseJsonLoose(value) {
  const raw = toNonEmptyString(value);
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch {
    try {
      return JSON.parse(escapeInvalidJsonBackslashes(raw));
    } catch {
      return null;
    }
  }
}

function inferSenderFromRawWebhookText(rawText) {
  const text = toNonEmptyString(rawText);
  if (!text) {
    return "";
  }

  const jidMatch = text.match(/(\d{10,15})@s\.whatsapp\.net/i);
  if (jidMatch?.[1]) {
    return normalizeWhatsappNumber(jidMatch[1]);
  }

  const numberMatch = text.match(/\b55\d{10,13}\b/);
  if (numberMatch?.[0]) {
    return normalizeWhatsappNumber(numberMatch[0]);
  }

  return "";
}

function parseWebhookPayload(rawBody) {
  if (rawBody && typeof rawBody === "object") {
    return {
      payload: rawBody,
      rawText: "",
      parseStatus: "parsed_object",
      parseError: "",
    };
  }

  const rawText = toNonEmptyString(rawBody);
  if (!rawText) {
    return {
      payload: {},
      rawText: "",
      parseStatus: "empty",
      parseError: "",
    };
  }

  const direct = tryParseJsonLoose(rawText);
  if (direct && typeof direct === "object") {
    return {
      payload: direct,
      rawText,
      parseStatus: "parsed_json",
      parseError: "",
    };
  }

  const params = new URLSearchParams(rawText);
  const candidateKeys = ["payload", "data", "body"];
  for (const key of candidateKeys) {
    const value = params.get(key);
    if (!value) {
      continue;
    }
    const parsed = tryParseJsonLoose(value);
    if (parsed && typeof parsed === "object") {
      return {
        payload: parsed,
        rawText,
        parseStatus: `parsed_form_${key}`,
        parseError: "",
      };
    }
  }

  return {
    payload: {},
    rawText,
    parseStatus: "invalid",
    parseError: "invalid_json",
  };
}

function recordWebhookEvent({
  event = "",
  instanceName = "",
  senderRaw = "",
  senderNumber = "",
  senderName = "",
  messageId = "",
  messageType = "",
  messageText = "",
  status = "processed",
  reason = "",
  details = null,
} = {}) {
  try {
    db.prepare(
      `
        INSERT INTO webhook_events (
          event, instance_name, sender_raw, sender_number, sender_name,
          message_id, message_type, message_text, status, reason, details, received_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    ).run(
      String(event || ""),
      String(instanceName || ""),
      String(senderRaw || ""),
      normalizePhone(senderNumber || ""),
      String(senderName || ""),
      String(messageId || ""),
      String(messageType || ""),
      String(messageText || "").slice(0, 2000),
      String(status || "processed"),
      String(reason || ""),
      safeJsonStringify(details),
      new Date().toISOString(),
    );
  } catch (error) {
    console.error("[webhook] failed to persist webhook event:", error?.message || error);
  }
}

function isLikelyIncomingMessageEvent(incoming) {
  const event = toNonEmptyString(incoming?.event).toLowerCase();
  if (!event) {
    return false;
  }

  return (
    event.includes("message") ||
    event.includes("messages.upsert") ||
    event.includes("messages-update") ||
    event.includes("upsert")
  );
}

function unsupportedInboundPlaceholder(incoming) {
  const type = toNonEmptyString(incoming?.messageType) || "mensagem";
  return `[mensagem sem texto: ${type}]`;
}

const jsonBodyParser = express.json({ limit: "2mb" });
const webhookBodyParser = express.text({ type: "*/*", limit: "2mb" });

app.use((req, res, next) => {
  if (req.path === "/webhook/whatsapp") {
    return next();
  }
  return jsonBodyParser(req, res, next);
});
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Api-Key");
  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }
  next();
});

function ensureEnv(name) {
  const value = process.env[name];
  if (!value || !value.trim()) {
    throw new Error(`Variavel obrigatoria ausente: ${name}`);
  }
  return value.trim();
}

function isKnowledgeWriteAuthorized(req) {
  const requiredToken = String(process.env.KNOWLEDGE_ADMIN_TOKEN || "").trim();
  if (!requiredToken) {
    return true;
  }

  const provided = String(req.headers["x-admin-token"] || "").trim();
  return provided && provided === requiredToken;
}

function getConfiguredEstablishmentId() {
  const raw =
    String(process.env.TRINKS_ESTABLISHMENT_ID || "").trim() ||
    String(process.env.VITE_TRINKS_ESTABLISHMENT_ID || "").trim();

  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

function toIsoDateTime(date, time) {
  return `${date}T${time}:00`;
}

function normalizePhone(phone) {
  return String(phone || "").replace(/\D/g, "");
}

function normalizeWhatsappNumber(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  const withoutDeviceSuffix = raw.split(":")[0];
  const withoutJidSuffix = withoutDeviceSuffix.split("@")[0];
  return normalizePhone(withoutJidSuffix);
}

function toNonEmptyString(value) {
  const text = String(value || "").trim();
  return text || "";
}

function firstNonEmpty(values) {
  for (const value of values) {
    const text = toNonEmptyString(value);
    if (text) {
      return text;
    }
  }
  return "";
}

function normalizeWhatsappMessage(item) {
  if (!item || typeof item !== "object") {
    return null;
  }

  const role = toNonEmptyString(item.role);
  const content = toNonEmptyString(item.content || item.text);
  if (!role || !content) {
    return null;
  }

  return {
    role,
    content,
    at: toNonEmptyString(item.at || item.timestamp),
    senderName: toNonEmptyString(item.senderName),
  };
}

function persistWhatsappMessage({ phone, role, content, at, senderName = "", source = "runtime" }) {
  if (!phone || !role || !content) {
    return;
  }

  db.prepare(
    `
      INSERT INTO whatsapp_messages (phone, role, content, sender_name, at, source)
      VALUES (?, ?, ?, ?, ?, ?)
    `,
  ).run(
    String(phone),
    String(role),
    String(content),
    senderName ? String(senderName) : "",
    at ? String(at) : new Date().toISOString(),
    source ? String(source) : "runtime",
  );
}

function loadWhatsappMessagesFromDb(phone, limit = MAX_WHATSAPP_HISTORY_MESSAGES) {
  if (!phone) {
    return [];
  }

  const rows = db.prepare(
    `
      SELECT role, content, at, sender_name AS senderName
      FROM whatsapp_messages
      WHERE phone = ?
      ORDER BY datetime(at) DESC, id DESC
      LIMIT ?
    `,
  ).all(String(phone), Number(limit));

  return rows
    .reverse()
    .map(normalizeWhatsappMessage)
    .filter(Boolean);
}

function getWhatsappHistory(phone) {
  if (!phone) {
    return [];
  }

  const current = Array.isArray(whatsappConversations.get(phone))
    ? whatsappConversations.get(phone)
    : [];

  const normalized = current.map(normalizeWhatsappMessage).filter(Boolean);
  if (normalized.length) {
    return normalized;
  }

  const fromDb = loadWhatsappMessagesFromDb(phone, MAX_WHATSAPP_HISTORY_MESSAGES);
  if (fromDb.length) {
    whatsappConversations.set(phone, fromDb);
  }
  return fromDb;
}

function pushWhatsappHistory(phone, role, content, senderName = "") {
  if (!phone || !role || !content) {
    return;
  }

  const current = getWhatsappHistory(phone);
  const entry = {
    role,
    content: String(content),
    at: new Date().toISOString(),
    senderName: senderName ? String(senderName) : "",
  };
  const updated = [...current, entry].slice(-MAX_WHATSAPP_HISTORY_MESSAGES);
  whatsappConversations.set(phone, updated);
  persistWhatsappMessage({
    phone,
    role,
    content,
    at: entry.at,
    senderName: entry.senderName,
    source: "runtime",
  });
}

function cleanupWebhookDedupeCache(now = Date.now()) {
  for (const [key, value] of recentWebhookMessages.entries()) {
    if (!value || now - Number(value.at || 0) > WEBHOOK_DEDUPE_WINDOW_MS) {
      recentWebhookMessages.delete(key);
    }
  }
}

function isDuplicateIncomingWhatsapp(incoming) {
  const now = Date.now();
  cleanupWebhookDedupeCache(now);

  const sender = normalizePhone(incoming?.senderNumber || "");
  const messageId = toNonEmptyString(incoming?.messageId);
  const text = toNonEmptyString(incoming?.messageText).toLowerCase();
  const withMessageId = sender && messageId ? `id:${sender}:${messageId}` : "";
  const withoutMessageId = sender && text ? `txt:${sender}:${text}` : "";

  if (withMessageId) {
    if (recentWebhookMessages.has(withMessageId)) {
      return true;
    }
    recentWebhookMessages.set(withMessageId, { at: now });
  }

  if (withoutMessageId) {
    const previous = recentWebhookMessages.get(withoutMessageId);
    if (previous && now - Number(previous.at || 0) <= 10_000) {
      return true;
    }
    recentWebhookMessages.set(withoutMessageId, { at: now });
  }

  return false;
}

function summarizeWhatsappConversations() {
  const rows = db.prepare(
    `
      SELECT m.phone,
             m.content AS lastMessage,
             m.role AS lastRole,
             m.at AS updatedAt,
             m.sender_name AS senderName,
             (
               SELECT sender_name
               FROM whatsapp_messages u
               WHERE u.phone = m.phone
                 AND u.role = 'user'
                 AND COALESCE(u.sender_name, '') <> ''
               ORDER BY datetime(u.at) DESC, u.id DESC
               LIMIT 1
             ) AS userSenderName,
             (
               SELECT COUNT(*)
               FROM whatsapp_messages c
               WHERE c.phone = m.phone
             ) AS count
      FROM whatsapp_messages m
      JOIN (
        SELECT phone, MAX(id) AS max_id
        FROM whatsapp_messages
        GROUP BY phone
      ) latest ON latest.max_id = m.id
      ORDER BY datetime(m.at) DESC, m.id DESC
    `,
  ).all();

  if (rows.length) {
    return rows.map((row) => ({
      phone: String(row.phone || ""),
      name: String(row.userSenderName || row.senderName || ""),
      lastMessage: String(row.lastMessage || ""),
      lastRole: String(row.lastRole || ""),
      updatedAt: String(row.updatedAt || ""),
      count: Number(row.count || 0),
    }));
  }

  const summaries = [];

  for (const [phone, messages] of whatsappConversations.entries()) {
    const normalized = Array.isArray(messages) ? messages.map(normalizeWhatsappMessage).filter(Boolean) : [];
    if (!normalized.length) {
      continue;
    }

    const last = normalized[normalized.length - 1];
    const lastUser = [...normalized].reverse().find((item) => item.role === "user" && item.senderName);

    summaries.push({
      phone,
      name: lastUser?.senderName || "",
      lastMessage: last?.content || "",
      lastRole: last?.role || "",
      updatedAt: last?.at || "",
      count: normalized.length,
    });
  }

  return summaries.sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
}

function cleanupPendingBookingConfirmations(now = Date.now()) {
  for (const [key, value] of pendingBookingConfirmations.entries()) {
    if (!value || now > Number(value.expiresAt || 0)) {
      pendingBookingConfirmations.delete(key);
    }
  }
}

function resolvePendingSessionKey(establishmentId, customerContext = {}, fallback = {}) {
  const phone = normalizePhone(customerContext?.phone || fallback.clientPhone || "");
  if (phone) {
    return `${Number(establishmentId)}:phone:${phone}`;
  }

  const name = normalizeForMatch(customerContext?.name || fallback.clientName || "").trim();
  if (name) {
    return `${Number(establishmentId)}:name:${name}`;
  }

  return "";
}

function getPendingBookingConfirmation(sessionKey) {
  if (!sessionKey) {
    return null;
  }

  cleanupPendingBookingConfirmations();
  return pendingBookingConfirmations.get(sessionKey) || null;
}

function setPendingBookingConfirmation(sessionKey, payload) {
  if (!sessionKey) {
    return null;
  }

  const now = Date.now();
  const value = {
    ...payload,
    createdAt: now,
    expiresAt: now + BOOKING_CONFIRMATION_TTL_MS,
  };
  pendingBookingConfirmations.set(sessionKey, value);
  return value;
}

function clearPendingBookingConfirmation(sessionKey) {
  if (!sessionKey) {
    return;
  }
  pendingBookingConfirmations.delete(sessionKey);
}

function isHumanHandoffEnabled() {
  const raw = String(process.env.HUMAN_HANDOFF_ENABLED || "true").trim().toLowerCase();
  return !["0", "false", "off", "no", "nao"].includes(raw);
}

function cleanupHumanHandoffSessions(now = Date.now()) {
  for (const [phone, value] of humanHandoffSessions.entries()) {
    if (!value || now > Number(value.expiresAt || 0)) {
      humanHandoffSessions.delete(phone);
    }
  }
}

function getHumanHandoffSession(phone) {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) {
    return null;
  }

  cleanupHumanHandoffSessions();
  return humanHandoffSessions.get(normalizedPhone) || null;
}

function setHumanHandoffSession(phone, payload = {}) {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) {
    return null;
  }

  const now = Date.now();
  const value = {
    active: true,
    phone: normalizedPhone,
    createdAt: now,
    updatedAt: now,
    expiresAt: now + HUMAN_HANDOFF_TTL_MS,
    ...payload,
  };

  humanHandoffSessions.set(normalizedPhone, value);
  return value;
}

function clearHumanHandoffSession(phone) {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) {
    return false;
  }

  return humanHandoffSessions.delete(normalizedPhone);
}

function listHumanAlertPhones() {
  const raw = firstNonEmpty([
    process.env.HUMAN_ALERT_NUMBERS,
    process.env.RECEPTION_ALERT_NUMBERS,
    process.env.RECEPCAO_ALERT_NUMBERS,
  ]);

  if (!raw) {
    return [];
  }

  return [...new Set(
    String(raw)
      .split(/[;,]/)
      .map((item) => normalizePhone(item))
      .filter(Boolean),
  )];
}

function isHumanHandoffRequest(text) {
  const normalized = normalizeForMatch(text);
  if (!normalized) {
    return false;
  }

  return (
    /\b(falar com atendente|falar com humano|atendimento humano|suporte humano|quero humano|quero atendente)\b/.test(
      normalized,
    ) ||
    /\b(chamar recepcao|chamar recepcao|me transfere|transferir para humano|transferir atendimento)\b/.test(
      normalized,
    )
  );
}

function isHumanHandoffResumeRequest(text) {
  const normalized = normalizeForMatch(text);
  if (!normalized) {
    return false;
  }

  return (
    /\b(voltar para ia|retomar ia|continuar com ia|atendimento automatico|bot pode voltar)\b/.test(normalized) ||
    /\b(ia pode continuar|pode seguir ia)\b/.test(normalized)
  );
}

async function notifyHumanAlertPhones({
  instance,
  establishmentId,
  customerPhone,
  customerName,
  customerMessage,
}) {
  const alertPhones = listHumanAlertPhones();
  if (!alertPhones.length) {
    return { sent: 0, skipped: true, reason: "noAlertNumbersConfigured" };
  }

  const dateContext = getSaoPauloDateContext();
  const clientLabel = toNonEmptyString(customerName) || "Cliente sem nome";
  const text = [
    "ALERTA DE ATENDIMENTO HUMANO",
    `Cliente: ${clientLabel}`,
    `WhatsApp: ${normalizePhone(customerPhone)}`,
    `Mensagem: ${toNonEmptyString(customerMessage) || "-"}`,
    `Data: ${dateContext.brToday}`,
    `Hora: ${new Intl.DateTimeFormat("pt-BR", { timeZone: "America/Sao_Paulo", hour: "2-digit", minute: "2-digit" }).format(new Date())}`,
    `Estabelecimento: ${establishmentId}`,
  ].join("\n");

  const sent = [];
  const failed = [];
  for (const to of alertPhones) {
    try {
      const result = await evolutionRequest(`/message/sendText/${instance}`, {
        method: "POST",
        body: { number: to, text },
      });
      sent.push({ to, result });
    } catch (error) {
      failed.push({
        to,
        message: error?.message || "Erro ao alertar recepcao.",
        status: error?.status || null,
      });
    }
  }

  return {
    sent: sent.length,
    failed: failed.length,
    details: { sent, failed },
  };
}

function detectConfirmationIntent(message) {
  const normalized = normalizeForMatch(message);
  if (!normalized) {
    return "none";
  }

  if (
    /\b(nao|não|negativo|melhor nao|melhor nao|cancelar|cancela|desmarcar|desmarca|mudar|trocar|corrigir)\b/.test(
      normalized,
    )
  ) {
    return "deny";
  }

  if (
    /\b(sim|confirmo|confirmar|confirmado|pode|ok|certo|isso|pode agendar|pode marcar|fechado)\b/.test(
      normalized,
    )
  ) {
    return "confirm";
  }

  return "none";
}

function formatBookingItemSummary(item) {
  const date = toNonEmptyString(item?.date);
  const brDate = isoToBrDate(date) || date;
  const time = normalizeTimeValue(item?.time) || toNonEmptyString(item?.time);
  const professional = professionalDisplayName(item?.professionalName || "");
  return `servico ${item?.service} com ${professional} em ${brDate} as ${time}`;
}

function buildBookingConfirmationMessage(items) {
  if (!Array.isArray(items) || !items.length) {
    return "Nao encontrei itens para confirmar.";
  }

  const lines = items.map((item, index) => `${index + 1}) ${formatBookingItemSummary(item)}`);
  if (items.length === 1) {
    return `Confirma este agendamento?\n${lines.join("\n")}\n\nSe estiver certo, responda "sim".`;
  }

  return `Confirma estes agendamentos?\n${lines.join("\n")}\n\nSe estiver certo, responda "sim".`;
}

function normalizeBookingTime(value) {
  const normalized = normalizeTimeValue(value);
  return normalized || "";
}

function normalizeBookingDate(value, fallbackDate = "") {
  const raw = toNonEmptyString(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }
  return toNonEmptyString(fallbackDate);
}

function generateRequestReference() {
  const now = new Date();
  const compact = now.toISOString().replace(/\D/g, "").slice(2, 14);
  const random = Math.random().toString(36).slice(2, 7).toUpperCase();
  return `REQ-${compact}-${random}`;
}

function buildAppointmentObservation({ requestReference = "", confirmationCode = "" } = {}) {
  const req = toNonEmptyString(requestReference);
  const code = toNonEmptyString(confirmationCode);
  const parts = ["Agendamento via IA.AGENDAMENTO"];
  if (req) {
    parts.push(`Req ${req}`);
  }
  if (code) {
    parts.push(`Cod ${code}`);
  }
  return parts.join(" | ").slice(0, 400);
}

function recordAppointmentAudit({
  eventType,
  status = "success",
  establishmentId = null,
  appointmentId = null,
  confirmationCode = "",
  clientPhone = "",
  clientName = "",
  serviceName = "",
  professionalName = "",
  date = "",
  time = "",
  requestPayload = null,
  responsePayload = null,
  errorMessage = "",
}) {
  try {
    db.prepare(
      `
        INSERT INTO appointment_audit (
          event_type, status, establishment_id, appointment_id, confirmation_code,
          client_phone, client_name, service_name, professional_name,
          appointment_date, appointment_time, request_payload, response_payload,
          error_message, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    ).run(
      String(eventType || "unknown"),
      String(status || "success"),
      establishmentId !== undefined && establishmentId !== null ? Number(establishmentId) : null,
      appointmentId !== undefined && appointmentId !== null ? Number(appointmentId) : null,
      String(confirmationCode || ""),
      normalizePhone(clientPhone || ""),
      String(clientName || ""),
      String(serviceName || ""),
      String(professionalName || ""),
      String(date || ""),
      String(time || ""),
      safeJsonStringify(requestPayload),
      safeJsonStringify(responsePayload),
      String(errorMessage || ""),
      new Date().toISOString(),
    );
  } catch (error) {
    console.error("[audit] failed to persist appointment audit:", error?.message || error);
  }
}

function normalizeTrinksPhone(phone) {
  if (!phone || typeof phone !== "object") {
    return normalizePhone(phone);
  }

  return normalizePhone(`${phone?.ddi || ""}${phone?.ddd || ""}${phone?.telefone || ""}${phone?.numero || ""}`);
}

// DecompÃµe um telefone brasileiro em { ddi, ddd, numero } para criaÃ§Ã£o de clientes na Trinks
function parseBrazilianPhone(phone) {
  const digits = normalizePhone(phone);
  if (!digits) return null;

  // Remove DDI 55 se presente no inÃ­cio (55 + 10 ou 11 dÃ­gitos = 12 ou 13 dÃ­gitos)
  let local = digits;
  if (local.length >= 12 && local.startsWith("55")) {
    local = local.slice(2);
  }

  // Extrai DDD (2 dÃ­gitos) + nÃºmero (8 ou 9 dÃ­gitos)
  if (local.length >= 10) {
    const ddd = local.slice(0, 2);
    const numero = local.slice(2);
    return { ddi: "55", ddd, numero };
  }

  // Sem DDD reconhecÃ­vel â€” retorna sÃ³ o nÃºmero
  return { ddi: "55", ddd: "", numero: local };
}

function buildPhoneVariants(phone) {
  const normalized = normalizePhone(phone);
  const variants = new Set();

  if (!normalized) {
    return variants;
  }

  variants.add(normalized);

  if (normalized.startsWith("55") && normalized.length > 11) {
    variants.add(normalized.slice(2));
  }

  if (normalized.length > 11) {
    variants.add(normalized.slice(-11));
  }

  if (normalized.length > 10) {
    variants.add(normalized.slice(-10));
  }

  return variants;
}

function clientPhonesFrom(item) {
  const phones = item?.telefones || item?.phones || [];
  return phones
    .map((phone) => [normalizePhone(phone?.numero || phone?.phone || phone), normalizeTrinksPhone(phone)])
    .flat()
    .filter(Boolean);
}

function matchesClientPhone(item, clientPhone) {
  const targetVariants = buildPhoneVariants(clientPhone);
  if (!targetVariants.size) {
    return false;
  }

  const existingPhones = clientPhonesFrom(item);
  return existingPhones.some((existingPhone) => {
    const existingVariants = buildPhoneVariants(existingPhone);
    return [...existingVariants].some((variant) => targetVariants.has(variant));
  });
}

function matchesClientName(item, clientName) {
  const normalizedTarget = String(clientName || "").toLowerCase().trim();
  if (!normalizedTarget) {
    return false;
  }

  const normalizedName = String(item?.nome || item?.name || "").toLowerCase().trim();
  return normalizedName === normalizedTarget;
}

function dedupeClients(items) {
  const seen = new Set();
  const result = [];

  for (const item of items) {
    const clientId = clientIdFrom(item);
    const key = clientId ? `id:${clientId}` : `name:${String(item?.nome || item?.name || "").toLowerCase().trim()}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    result.push(item);
  }

  return result;
}

function getMappedClientByPhone(clientPhone) {
  const normalizedPhone = normalizePhone(clientPhone);
  if (!normalizedPhone) {
    return null;
  }

  const row = db.prepare(
    `
      SELECT phone, client_id AS clientId, client_name AS clientName, updated_at AS updatedAt
      FROM client_phone_map
      WHERE phone = ?
      LIMIT 1
    `,
  ).get(normalizedPhone);

  if (!row) {
    return null;
  }

  const parsedClientId = Number(row.clientId);
  if (!Number.isFinite(parsedClientId) || parsedClientId <= 0) {
    return null;
  }

  return {
    phone: normalizedPhone,
    clientId: parsedClientId,
    clientName: toNonEmptyString(row.clientName),
    updatedAt: toNonEmptyString(row.updatedAt),
  };
}

function upsertClientPhoneMap(clientPhone, clientId, clientName = "") {
  const normalizedPhone = normalizePhone(clientPhone);
  const parsedClientId = Number(clientId);
  if (!normalizedPhone || !Number.isFinite(parsedClientId) || parsedClientId <= 0) {
    return;
  }

  db.prepare(
    `
      INSERT INTO client_phone_map (phone, client_id, client_name, updated_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(phone) DO UPDATE SET
        client_id = excluded.client_id,
        client_name = excluded.client_name,
        updated_at = excluded.updated_at
    `,
  ).run(
    normalizedPhone,
    parsedClientId,
    toNonEmptyString(clientName),
    new Date().toISOString(),
  );
}

function scoreClientCandidate(item, { clientPhone = "", clientName = "" } = {}) {
  let score = 0;
  const normalizedTargetName = normalizeForMatch(clientName).trim();
  const normalizedCandidateName = normalizeForMatch(clientDisplayNameFrom(item)).trim();
  const targetTokens = normalizedTargetName.split(/\s+/).filter(Boolean);
  const candidateTokens = normalizedCandidateName.split(/\s+/).filter(Boolean);
  const candidateSet = new Set(candidateTokens);
  const overlap = targetTokens.filter((token) => candidateSet.has(token)).length;

  if (clientPhone && matchesClientPhone(item, clientPhone)) {
    score += 10_000;
  }

  if (normalizedTargetName && normalizedCandidateName) {
    if (normalizedTargetName === normalizedCandidateName) {
      score += 2_000;
    } else if (normalizedCandidateName.startsWith(normalizedTargetName)) {
      score += 1_200;
    } else if (overlap > 0) {
      score += overlap * 120;
    }
  }

  if (targetTokens.length && candidateTokens.length) {
    const missing = Math.max(0, targetTokens.length - overlap);
    score -= missing * 20;
  }

  const phonesCount = Array.isArray(item?.telefones || item?.phones) ? (item?.telefones || item?.phones).length : 0;
  if (phonesCount > 0) {
    score += Math.min(phonesCount, 3) * 15;
  }

  score += Math.min(candidateTokens.length, 4) * 5;

  const id = Number(clientIdFrom(item));
  if (Number.isFinite(id) && id > 0) {
    score += Math.max(0, 1000 - Math.min(id, 1000));
  }

  return score;
}

function pickBestClientCandidate(items, { clientPhone = "", clientName = "" } = {}) {
  if (!Array.isArray(items) || !items.length) {
    return null;
  }

  const ranked = [...items]
    .map((item) => ({
      item,
      score: scoreClientCandidate(item, { clientPhone, clientName }),
      id: Number(clientIdFrom(item)),
    }))
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }

      const leftId = Number.isFinite(left.id) && left.id > 0 ? left.id : Number.MAX_SAFE_INTEGER;
      const rightId = Number.isFinite(right.id) && right.id > 0 ? right.id : Number.MAX_SAFE_INTEGER;
      return leftId - rightId;
    });

  return ranked[0]?.item || null;
}

function clientDisplayNameFrom(item) {
  return firstNonEmpty([item?.nome, item?.name, item?.cliente?.nome, item?.clienteNome]);
}

async function listClients(estabelecimentoId, query = {}) {
  const payload = await trinksRequest("/clientes", {
    method: "GET",
    estabelecimentoId,
    query: {
      page: 1,
      pageSize: 50,
      ...query,
    },
  });

  return extractItems(payload);
}

async function findExistingClientByPhone(estabelecimentoId, clientPhone) {
  const normalizedPhone = normalizePhone(clientPhone);
  if (!normalizedPhone) {
    return null;
  }

  const mapped = getMappedClientByPhone(normalizedPhone);

  const searches = [
    listClients(estabelecimentoId, { telefone: normalizedPhone }),
    listClients(estabelecimentoId, { phone: normalizedPhone }),
    listClients(estabelecimentoId, { celular: normalizedPhone }),
    listClients(estabelecimentoId),
  ];

  const results = await Promise.allSettled(searches);
  const candidates = dedupeClients(
    results
      .filter((item) => item.status === "fulfilled")
      .flatMap((item) => item.value),
  );
  const byPhone = candidates.filter((item) => matchesClientPhone(item, normalizedPhone));
  if (mapped?.clientId) {
    const mappedMatch = byPhone.find((item) => Number(clientIdFrom(item)) === Number(mapped.clientId));
    if (mappedMatch) {
      upsertClientPhoneMap(normalizedPhone, mapped.clientId, clientDisplayNameFrom(mappedMatch) || mapped.clientName);
      return mappedMatch;
    }
  }

  const selected = pickBestClientCandidate(byPhone, {
    clientPhone: normalizedPhone,
    clientName: "",
  });
  if (!selected && mapped?.clientId) {
    return {
      id: mapped.clientId,
      nome: mapped.clientName,
      telefones: [{ numero: normalizedPhone }],
      source: "phone_map_fallback",
    };
  }
  if (!selected) return null;

  const selectedId = Number(clientIdFrom(selected));
  if (Number.isFinite(selectedId) && selectedId > 0) {
    upsertClientPhoneMap(normalizedPhone, selectedId, clientDisplayNameFrom(selected));
  }

  return selected;
}

function extractItems(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.result)) return payload.result;
  return [];
}

function serviceIdFrom(item) {
  return item?.id ?? item?.servicoId ?? item?.servico?.id ?? null;
}

function clientIdFrom(item) {
  return item?.id ?? item?.clienteId ?? item?.cliente?.id ?? null;
}

function professionalIdFrom(item) {
  return item?.id ?? item?.profissionalId ?? item?.profissional?.id ?? null;
}

function professionalNameFrom(item) {
  return item?.nome ?? item?.name ?? item?.profissional?.nome ?? null;
}

function professionalDisplayName(name) {
  const raw = toNonEmptyString(name);
  if (!raw) {
    return "";
  }

  const first = raw.split(/\s+/).filter(Boolean)[0] || raw;
  return first.charAt(0).toUpperCase() + first.slice(1).toLowerCase();
}

function clientFirstName(name) {
  const raw = toNonEmptyString(name);
  if (!raw) {
    return "";
  }

  const first = raw.split(/\s+/).filter(Boolean)[0] || raw;
  return first.charAt(0).toUpperCase() + first.slice(1).toLowerCase();
}

function professionalHasOpenSchedule(item) {
  return Array.isArray(item?.availableTimes) && item.availableTimes.length > 0;
}

function uniqueProfessionalDisplayNames(names) {
  const seen = new Set();
  const result = [];

  for (const name of names) {
    const display = professionalDisplayName(name);
    if (!display) {
      continue;
    }
    const key = normalizeForMatch(display);
    if (!seen.has(key)) {
      seen.add(key);
      result.push(display);
    }
  }

  return result;
}

function loadSalonKnowledge() {
  try {
    const raw = readFileSync(KNOWLEDGE_FILE_PATH, "utf-8");
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function saveSalonKnowledge(nextKnowledge) {
  const json = JSON.stringify(nextKnowledge, null, 2);
  writeFileSync(KNOWLEDGE_FILE_PATH, `${json}\n`, "utf-8");
  return nextKnowledge;
}

function formatKnowledgeForPrompt(knowledge) {
  const identity = knowledge?.identity || {};
  const policies = knowledge?.policies || {};
  const business = knowledge?.business || {};
  const services = Array.isArray(knowledge?.services) ? knowledge.services : [];
  const faq = Array.isArray(knowledge?.faq) ? knowledge.faq : [];

  const servicesText = services.length
    ? services
        .map((item) => {
          const name = item?.name || "Servico";
          const duration = item?.durationMinutes ? `${item.durationMinutes} min` : "duracao nao informada";
          const price = item?.price ? `R$ ${item.price}` : "preco sob consulta";
          return `- ${name} | ${duration} | ${price}`;
        })
        .join("\n")
    : "- Sem servicos cadastrados";

  const faqText = faq.length
    ? faq
        .map((item) => `- P: ${item?.question || ""}\n  R: ${item?.answer || ""}`)
        .join("\n")
    : "- Sem perguntas frequentes cadastradas";

  return [
    "Base de conhecimento do salao (fonte oficial):",
    `- Nome comercial: ${identity?.brandName || "Nao informado"}`,
    `- Endereco: ${business?.address || "Nao informado"}`,
    `- Horario de funcionamento: ${business?.openingHours || "Nao informado"}`,
    `- Telefone: ${business?.phone || "Nao informado"}`,
    `- Formas de pagamento: ${Array.isArray(business?.paymentMethods) && business.paymentMethods.length ? business.paymentMethods.join(", ") : "Nao informado"}`,
    `- Politica de atraso: ${policies?.latePolicy || "Nao informado"}`,
    `- Politica de cancelamento: ${policies?.cancellationPolicy || "Nao informado"}`,
    `- Politica de no-show: ${policies?.noShowPolicy || "Nao informado"}`,
    "",
    "Servicos e referencias:",
    servicesText,
    "",
    "FAQ:",
    faqText,
    "",
    "Regra: sempre priorize esta base para responder duvidas comerciais do salao.",
  ].join("\n");
}

const SYSTEM_INSTRUCTION = `Voce e a IA.AGENDAMENTO, uma concierge digital premium para atendimento e agendamento.
Seu nome e Jacques.

Diretrizes:
- Tom sofisticado, acolhedor e objetivo.
- Frases curtas, sem paragrafos longos.
- Foco em concluir agendamentos com precisao.
- Ao mencionar profissionais para a cliente, use apenas o primeiro nome.
- Ao chamar a cliente pelo nome, use apenas o primeiro nome.

Fluxo:
- Identifique o servico desejado.
- Antes de sugerir horario, consulte disponibilidade real por profissional (checkAvailability).
- Se a cliente nao tiver preferencia de profissional e informar horario desejado, mostre todas as profissionais que executam o servico e estao livres naquele horario.
- Para agendar, use bookAppointment.
- Antes de finalizar o agendamento, sempre valide disponibilidade e apresente um resumo completo para confirmacao explicita da cliente.
- Se houver mais de um servico, monte todos os itens no campo appointments da ferramenta bookAppointment.
- Para reagendar, use rescheduleAppointment.
- Para desmarcar sem remarcar, use cancelAppointment.
- Regra critica: se a cliente pedir alteracao ou cancelamento, nao use bookAppointment antes de concluir reschedule/cancel.
- Para desmarcar, priorize pedir codigo de confirmacao (TRK). Se a cliente nao tiver codigo, tente localizar pelo telefone da cliente na base Trinks e prossiga com seguranca.
- Quando a cliente perguntar nomes de profissionais, consulte a ferramenta listProfessionalsForDate e responda apenas com dados reais.
- Ao receber preferencia de profissional e/ou horario desejado, use checkAvailability com professionalName e preferredTime para trazer os horarios mais proximos possiveis.
- Se a profissional preferida nao estiver livre no horario desejado, primeiro mostre os horarios que ela tem no dia e depois pergunte: "Caso esses horarios nao sirvam para voce, quer saber a disponibilidade de outros profissionais?".

Datas:
- Use obrigatoriamente o contexto temporal oficial enviado no prompt.
- Interprete "hoje", "amanha" e "depois de amanha" com base nesse contexto.
- Nunca invente datas.`;

const chatTools = [
  {
    name: "checkAvailability",
    parameters: {
      type: Type.OBJECT,
      description: "Verifica disponibilidade de horarios para um servico em uma data.",
      properties: {
        service: {
          type: Type.STRING,
          description: "Nome do servico (ex: corte, mechas, manicure).",
        },
        date: {
          type: Type.STRING,
          description: "Data desejada no formato YYYY-MM-DD.",
        },
        professionalName: {
          type: Type.STRING,
          description: "Nome da profissional preferida pela cliente (opcional).",
        },
        preferredTime: {
          type: Type.STRING,
          description: "Horario preferido no formato HH:mm (opcional).",
        },
      },
      required: ["service", "date"],
    },
  },
  {
    name: "listAppointmentsForDate",
    parameters: {
      type: Type.OBJECT,
      description: "Lista horarios ocupados em uma data.",
      properties: {
        date: {
          type: Type.STRING,
          description: "Data desejada no formato YYYY-MM-DD.",
        },
      },
      required: ["date"],
    },
  },
  {
    name: "listProfessionalsForDate",
    parameters: {
      type: Type.OBJECT,
      description: "Lista profissionais disponiveis em uma data, opcionalmente filtrando por servico.",
      properties: {
        date: {
          type: Type.STRING,
          description: "Data desejada no formato YYYY-MM-DD. Se nao informado, usar hoje.",
        },
        service: {
          type: Type.STRING,
          description: "Nome do servico para filtrar profissionais (opcional).",
        },
      },
    },
  },
  {
    name: "bookAppointment",
    parameters: {
      type: Type.OBJECT,
      description: "Prepara um ou mais agendamentos para confirmacao final da cliente.",
      properties: {
        service: { type: Type.STRING },
        date: { type: Type.STRING },
        time: { type: Type.STRING, description: "Horario escolhido (ex: 14:00)." },
        professionalName: {
          type: Type.STRING,
          description: "Nome da profissional desejada (opcional).",
        },
        clientName: { type: Type.STRING },
        clientPhone: { type: Type.STRING },
        appointments: {
          type: Type.ARRAY,
          description:
            "Lista de agendamentos quando houver mais de um servico. Se informado, cada item deve conter service, date, time e opcionalmente professionalName.",
          items: {
            type: Type.OBJECT,
            properties: {
              service: { type: Type.STRING },
              date: { type: Type.STRING },
              time: { type: Type.STRING },
              professionalName: { type: Type.STRING },
            },
            required: ["service", "date", "time"],
          },
        },
      },
      required: [],
    },
  },
  {
    name: "rescheduleAppointment",
    parameters: {
      type: Type.OBJECT,
      description: "Altera a data e horario de um agendamento existente.",
      properties: {
        confirmationCode: {
          type: Type.STRING,
          description: "Codigo de confirmacao como TRK-123 (opcional se appointmentId for informado).",
        },
        appointmentId: {
          type: Type.STRING,
          description: "ID do agendamento (opcional se confirmationCode for informado).",
        },
        date: { type: Type.STRING, description: "Nova data no formato YYYY-MM-DD." },
        time: { type: Type.STRING, description: "Novo horario no formato HH:mm." },
      },
      required: ["date", "time"],
    },
  },
  {
    name: "cancelAppointment",
    parameters: {
      type: Type.OBJECT,
      description: "Desmarca um agendamento existente sem necessidade de reagendamento.",
      properties: {
        confirmationCode: {
          type: Type.STRING,
          description: "Codigo de confirmacao como TRK-123 (opcional se appointmentId for informado).",
        },
        appointmentId: {
          type: Type.STRING,
          description: "ID do agendamento (opcional se confirmationCode for informado).",
        },
        reason: {
          type: Type.STRING,
          description: "Motivo do cancelamento (opcional).",
        },
        clientPhone: {
          type: Type.STRING,
          description: "Telefone da cliente para localizar agendamento quando nao houver codigo (opcional).",
        },
        clientName: {
          type: Type.STRING,
          description: "Nome da cliente para apoiar identificacao (opcional).",
        },
        date: {
          type: Type.STRING,
          description: "Data do agendamento no formato YYYY-MM-DD (opcional).",
        },
        time: {
          type: Type.STRING,
          description: "Horario do agendamento no formato HH:mm (opcional).",
        },
        service: {
          type: Type.STRING,
          description: "Servico do agendamento para filtro (opcional).",
        },
        professionalName: {
          type: Type.STRING,
          description: "Profissional do agendamento para filtro (opcional).",
        },
      },
    },
  },
];

function addDaysToIsoDate(isoDate, days) {
  const [year, month, day] = String(isoDate || "")
    .split("-")
    .map((item) => Number(item));

  if (!year || !month || !day) {
    return "";
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  return date.toISOString().slice(0, 10);
}

function isoToBrDate(isoDate) {
  const [year, month, day] = String(isoDate || "").split("-");
  if (!year || !month || !day) {
    return "";
  }
  return `${day}/${month}/${year}`;
}

function getSaoPauloDateContext() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);

  const pick = (type) => parts.find((item) => item.type === type)?.value || "";
  const isoToday = `${pick("year")}-${pick("month")}-${pick("day")}`;
  const isoTomorrow = addDaysToIsoDate(isoToday, 1);
  const isoAfterTomorrow = addDaysToIsoDate(isoToday, 2);
  const weekdayToday = new Intl.DateTimeFormat("pt-BR", {
    timeZone: "America/Sao_Paulo",
    weekday: "long",
  }).format(now);

  return {
    timeZone: "America/Sao_Paulo",
    nowIso: now.toISOString(),
    isoToday,
    isoTomorrow,
    isoAfterTomorrow,
    brToday: isoToBrDate(isoToday),
    brTomorrow: isoToBrDate(isoTomorrow),
    brAfterTomorrow: isoToBrDate(isoAfterTomorrow),
    weekdayToday,
  };
}

function normalizeForMatch(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function detectRelativeDateReference(message, dateContext = getSaoPauloDateContext()) {
  const normalized = normalizeForMatch(message);

  if (normalized.includes("depois de amanha")) {
    return {
      label: "depois de amanha",
      iso: dateContext.isoAfterTomorrow,
      br: dateContext.brAfterTomorrow,
    };
  }

  if (/\bamanha\b/.test(normalized)) {
    return {
      label: "amanha",
      iso: dateContext.isoTomorrow,
      br: dateContext.brTomorrow,
    };
  }

  if (/\bhoje\b/.test(normalized)) {
    return {
      label: "hoje",
      iso: dateContext.isoToday,
      br: dateContext.brToday,
    };
  }

  return null;
}

function shouldLookupProfessionalsDirectly(message) {
  const normalized = normalizeForMatch(message);
  return (
    normalized.includes("profissional") ||
    normalized.includes("profissionais") ||
    normalized.includes("quem atende") ||
    normalized.includes("cabeleireira") ||
    normalized.includes("cabeleireiro")
  );
}

const FAQ_TOKEN_STOPWORDS = new Set([
  "a",
  "as",
  "ao",
  "aos",
  "de",
  "da",
  "das",
  "do",
  "dos",
  "e",
  "em",
  "na",
  "nas",
  "no",
  "nos",
  "o",
  "os",
  "ou",
  "para",
  "por",
  "que",
  "se",
  "um",
  "uma",
  "uns",
  "umas",
  "com",
  "sem",
  "me",
  "te",
  "voces",
  "voce",
  "eu",
]);

const SERVICE_INFERENCE_STOPWORDS = new Set([
  "agenda",
  "agendar",
  "amanha",
  "atende",
  "atendem",
  "cliente",
  "clientes",
  "com",
  "data",
  "disponibilidade",
  "disponivel",
  "fazer",
  "faz",
  "fazem",
  "hoje",
  "horario",
  "marcar",
  "para",
  "prefiro",
  "profissional",
  "profissionais",
  "qual",
  "quais",
  "quero",
  "servico",
  "servicos",
]);

function tokenizeMeaningfulText(value) {
  return normalizeForMatch(value)
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((token) => token && token.length > 2 && !FAQ_TOKEN_STOPWORDS.has(token));
}

function inferServiceHintFromMessage(message) {
  const tokens = normalizeForMatch(message)
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((token) => token && token.length >= 4 && !SERVICE_INFERENCE_STOPWORDS.has(token));

  if (!tokens.length) {
    return "";
  }

  return tokens.join(" ");
}

function findBestFaqAnswer(knowledge, message) {
  const faq = Array.isArray(knowledge?.faq) ? knowledge.faq : [];
  if (!faq.length) {
    return null;
  }

  const normalizedMessage = normalizeForMatch(message).trim();
  if (!normalizedMessage) {
    return null;
  }

  const messageTokens = [...new Set(tokenizeMeaningfulText(message))];
  let best = null;

  for (const item of faq) {
    const question = toNonEmptyString(item?.question);
    const answer = toNonEmptyString(item?.answer);
    if (!question || !answer) {
      continue;
    }

    const normalizedQuestion = normalizeForMatch(question).trim();
    let score = 0;

    if (
      normalizedQuestion.length >= 8 &&
      (normalizedQuestion.includes(normalizedMessage) || normalizedMessage.includes(normalizedQuestion))
    ) {
      score = 1;
    } else if (messageTokens.length) {
      const questionTokens = new Set(tokenizeMeaningfulText(question));
      if (questionTokens.size) {
        const overlap = messageTokens.filter((token) => questionTokens.has(token)).length;
        const coverage = overlap / messageTokens.length;
        if (overlap >= 2) {
          score = coverage;
        }
      }
    }

    if (!best || score > best.score) {
      best = { score, answer };
    }
  }

  return best && best.score >= 0.55 ? best.answer : null;
}

function messageSuggestsSchedulingIntent(message) {
  const normalized = normalizeForMatch(message);
  return (
    normalized.includes("agendar") ||
    normalized.includes("agendamento") ||
    normalized.includes("marcar") ||
    normalized.includes("reserva") ||
    normalized.includes("horario") ||
    normalized.includes("disponibilidade")
  );
}

function messageSuggestsBookingTimeIntent(message, dateContext = getSaoPauloDateContext()) {
  const normalized = normalizeForMatch(message);
  const hasBookingVerb = /(agend|marc|reserv)/.test(normalized);
  const hasAvailabilityCue = /(tem horario|horario disponivel|qual horario|disponibilid|vaga)/.test(
    normalized,
  );
  const hasClientIntent = /(quero|preciso|gostaria|pode|podemos|consigo)/.test(normalized);
  const hasDateCue =
    Boolean(detectRelativeDateReference(message, dateContext)) ||
    /\b\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\b/.test(normalized) ||
    /\b\d{1,2}(?::\d{2})?\s*h\b/.test(normalized) ||
    /\b\d{1,2}:\d{2}\b/.test(normalized) ||
    /\b(seg|ter|qua|qui|sex|sab|dom)(unda|ca|rta|nta|ta|ado|ingo)?\b/.test(normalized);
  const isBusinessHoursQuestion =
    normalized.includes("horario de funcionamento") ||
    normalized.includes("que horas abre") ||
    normalized.includes("que horas fecha") ||
    normalized.includes("abre que horas") ||
    normalized.includes("fecha que horas");

  if (isBusinessHoursQuestion) {
    return false;
  }

  return (hasBookingVerb && (hasClientIntent || hasDateCue)) || (hasAvailabilityCue && hasDateCue);
}

function messageSuggestsCancellationIntent(message) {
  const normalized = normalizeForMatch(message);
  return /\b(cancel|cancela|cancelar|desmar|demar|remover|apagar|excluir)\b/.test(normalized);
}

function messageSuggestsRescheduleIntent(message) {
  const normalized = normalizeForMatch(message);
  return /\b(remar|reagend|alter|mudar|trocar)\b/.test(normalized);
}

function historyAlreadyAskedProfessionalPreference(history) {
  if (!Array.isArray(history)) {
    return false;
  }

  return history.some((item) => {
    if (!item || item.role !== "assistant") {
      return false;
    }

    const text = normalizeForMatch(item.content);
    return text.includes("preferencia") && text.includes("profissional");
  });
}

function messageContainsProfessionalPreferenceHint(message) {
  const normalized = normalizeForMatch(message)
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!normalized) {
    return false;
  }

  if (/\bpreferenc\w*\s+(de|por)\s+profission/.test(normalized)) {
    return false;
  }

  return (
    /\bcom\s+(a|o)\s+[a-z]{3,}\b/.test(normalized) ||
    /\bsim[, ]+\s*[a-z]{3,}\b/.test(normalized) ||
    /\b(prefiro|quero|gosto)\s+(da|do|de)?\s*[a-z]{3,}\b/.test(normalized)
  );
}

function historyHasProfessionalContext(history) {
  if (!Array.isArray(history) || !history.length) {
    return false;
  }

  const patterns = [
    /\bcom\s+(a|o)\s+[a-z]{3,}\b/,
    /\bhorarios?\s+com\s+[a-z]{3,}\b/,
    /\bprofissional\s*:\s*[a-z]{3,}\b/,
    /\b(a|o)\s+[a-z]{3,}\s+tem disponibilidade\b/,
    /\b(a|o)\s+[a-z]{3,}\s+nao possui disponibilidade\b/,
  ];

  return history.some((item) => {
    const text = normalizeForMatch(item?.content || "")
      .replace(/[^a-z0-9\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (!text) {
      return false;
    }
    return patterns.some((pattern) => pattern.test(text));
  });
}

function buildConversationPrompt(history, message, knowledge, customerContext = null) {
  const dateContext = getSaoPauloDateContext();
  const relativeDate = detectRelativeDateReference(message, dateContext);
  const knownClientName = clientFirstName(customerContext?.name);
  const knownClientPhone = normalizePhone(customerContext?.phone || "");
  const transcript = Array.isArray(history)
    ? history
        .filter((item) => item && item.role && item.content)
        .map((item) => `${item.role === "assistant" ? "Assistente" : "Cliente"}: ${item.content}`)
        .join("\n")
    : "";

  return [
    "Historico da conversa:",
    transcript || "Sem historico anterior.",
    "",
    formatKnowledgeForPrompt(knowledge),
    "",
    "Contexto temporal oficial (usar como verdade):",
    `- Fuso horario: ${dateContext.timeZone}`,
    `- Agora (ISO): ${dateContext.nowIso}`,
    `- Hoje: ${dateContext.isoToday} (${dateContext.brToday}, ${dateContext.weekdayToday})`,
    `- Amanha: ${dateContext.isoTomorrow} (${dateContext.brTomorrow})`,
    `- Depois de amanha: ${dateContext.isoAfterTomorrow} (${dateContext.brAfterTomorrow})`,
    relativeDate
      ? `- Data absoluta para "${relativeDate.label}": ${relativeDate.iso} (${relativeDate.br})`
      : "- Data absoluta para termos relativos: nao aplicavel na mensagem atual.",
    "- Regra: interpretar 'hoje/amanha/depois de amanha' exclusivamente com base neste contexto.",
    knownClientName
      ? `- Cliente identificada na base Trinks: ${knownClientName}.`
      : "- Cliente identificada na base Trinks: nao identificada.",
    knownClientPhone
      ? `- Telefone da cliente (WhatsApp): ${knownClientPhone}.`
      : "- Telefone da cliente (WhatsApp): nao informado.",
    "- Regra: pergunte sobre preferencia de profissional somente quando a cliente estiver tentando agendar horario.",
    "- Regra: se nao houver preferencia de profissional e houver horario desejado, liste todas as profissionais disponiveis naquele horario.",
    "- Regra: se a profissional preferida estiver indisponivel no horario pedido, primeiro mostre os horarios que ela tem no dia e so depois pergunte se a cliente quer disponibilidade de outros profissionais.",
    "- Regra: antes de efetivar qualquer agendamento, sempre apresente resumo completo e aguarde confirmacao explicita da cliente.",
    "- Regra: se o pedido atual for para cancelar ou alterar, nao abrir novo agendamento ate concluir o cancelamento/alteracao.",
    "- Regra: para desmarcacao, prefira solicitar codigo TRK; se nao houver codigo e houver telefone de cliente identificada, tente localizar e continuar com seguranca.",
    "- Regra: antes de responder perguntas comerciais, valide primeiro a FAQ da base de conhecimento.",
    "",
    `Mensagem mais recente da cliente: ${message}`,
    "",
    "Responda como a proxima mensagem da assistente.",
  ].join("\n");
}

async function trinksRequest(path, { method = "GET", estabelecimentoId, body, query } = {}) {
  const baseUrl = ensureEnv("TRINKS_API_BASE_URL").replace(/\/$/, "");
  const apiKey = ensureEnv("TRINKS_API_KEY");

  const url = new URL(`${baseUrl}${path}`);
  if (query && typeof query === "object") {
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && String(v) !== "") {
        url.searchParams.set(k, String(v));
      }
    }
  }

  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
    "X-Api-Key": apiKey,
  };

  if (estabelecimentoId !== undefined && estabelecimentoId !== null) {
    headers.estabelecimentoId = String(estabelecimentoId);
  }

  const totalAttempts = Math.max(1, TRINKS_MAX_RETRIES);
  for (let attempt = 1; attempt <= totalAttempts; attempt += 1) {
    let response = null;
    let text = "";
    let json = null;

    try {
      response = await fetch(url, {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined,
      });
      text = await response.text();
      json = text ? safeJsonParse(text) : null;
    } catch (networkError) {
      if (attempt < totalAttempts) {
        const waitMs = TRINKS_RETRY_BASE_MS * (2 ** (attempt - 1));
        await new Promise((resolve) => setTimeout(resolve, waitMs));
        continue;
      }

      const error = new Error(`Trinks: falha de rede ao acessar ${path}`);
      error.status = 502;
      error.details = {
        path,
        method,
        attempt,
        totalAttempts,
        reason: networkError?.message || "Erro de rede",
      };
      throw error;
    }

    if (response.ok) {
      return json;
    }

    const status = Number(response.status || 0);
    const message = json?.message || json?.mensagem || text || `Erro ${status}`;
    const retryAfterHeader = response.headers?.get("retry-after");
    const retryAfterSeconds = Number(retryAfterHeader);
    const canUseRetryAfter = Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0;
    const retryAfterMs = canUseRetryAfter ? Math.round(retryAfterSeconds * 1000) : 0;
    const isRetryable = status === 429;

    if (isRetryable && attempt < totalAttempts) {
      const backoffMs = TRINKS_RETRY_BASE_MS * (2 ** (attempt - 1));
      const waitMs = Math.max(backoffMs, retryAfterMs);
      await new Promise((resolve) => setTimeout(resolve, waitMs));
      continue;
    }

    const error = new Error(`Trinks: ${message}`);
    error.status = status;
    error.details = {
      response: json || text,
      attempt,
      totalAttempts,
      method,
      path,
    };
    throw error;
  }

  const exhausted = new Error("Trinks: tentativas esgotadas.");
  exhausted.status = 429;
  throw exhausted;
}

function resolveEvolutionBaseUrl() {
  const baseUrl = firstNonEmpty([
    process.env.EVOLUTION_API_BASE_URL,
    process.env.EVOLUTION_URL,
    process.env._EVOLUTION_URL,
  ]);

  if (!baseUrl) {
    throw new Error(
      "Variavel obrigatoria ausente: EVOLUTION_API_BASE_URL (ou EVOLUTION_URL/_EVOLUTION_URL).",
    );
  }

  return String(baseUrl).replace(/\/$/, "");
}

function resolveEvolutionTimeoutMs() {
  const raw = Number(process.env.EVOLUTION_TIMEOUT_MS || 8000);
  if (!Number.isFinite(raw) || raw < 1000) {
    return 8000;
  }
  return Math.floor(raw);
}

async function evolutionRequest(path, { method = "POST", body } = {}) {
  const baseUrl = resolveEvolutionBaseUrl();
  const apiKey = ensureEnv("EVOLUTION_API_KEY");
  const timeoutMs = resolveEvolutionTimeoutMs();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  let response;
  try {
    response = await fetch(`${baseUrl}${path}`, {
      method,
      headers: {
        "Content-Type": "application/json",
        apikey: apiKey,
      },
      body: body ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
  } catch (error) {
    const isTimeout = error?.name === "AbortError";
    const failure = new Error(
      isTimeout
        ? `Evolution: timeout apos ${timeoutMs}ms em ${path}`
        : `Evolution: falha de conexao em ${path}`,
    );
    failure.status = isTimeout ? 504 : 502;
    failure.details = {
      path,
      timeoutMs,
      reason: error?.message || "Erro de rede",
    };
    throw failure;
  } finally {
    clearTimeout(timeout);
  }

  const text = await response.text();
  const json = text ? safeJsonParse(text) : null;

  if (!response.ok) {
    const message = json?.message || json?.error || text || `Erro ${response.status}`;
    const error = new Error(`Evolution: ${message}`);
    error.status = response.status;
    error.details = json || text;
    throw error;
  }

  return json;
}

async function evolutionRequestWithFallback(attempts) {
  const errors = [];

  for (const attempt of attempts) {
    try {
      const payload = await evolutionRequest(attempt.path, {
        method: attempt.method || "POST",
        body: attempt.body,
      });
      return { payload, attempt };
    } catch (error) {
      errors.push({
        path: attempt.path,
        method: attempt.method || "POST",
        message: error.message || "Erro desconhecido",
        details: error.details || null,
        status: error.status || null,
      });
    }
  }

  const aggregate = new Error("Falha em todas as tentativas na Evolution API.");
  aggregate.status = errors[0]?.status || 500;
  aggregate.details = errors;
  throw aggregate;
}

function extractQrValue(payload) {
  return firstNonEmpty([
    payload?.base64,
    payload?.qrcode?.base64,
    payload?.qrcode,
    payload?.qr,
    payload?.code,
    payload?.data?.base64,
    payload?.data?.qrcode?.base64,
    payload?.data?.qrcode,
    payload?.data?.qr,
    payload?.data?.code,
    payload?.pairingCode,
    payload?.data?.pairingCode,
  ]);
}

function toQrDataUrl(rawQr) {
  const value = toNonEmptyString(rawQr);
  if (!value) {
    return "";
  }

  if (value.startsWith("data:image")) {
    return value;
  }

  if (value.startsWith("data:")) {
    return value;
  }

  const looksLikeBase64 = /^[A-Za-z0-9+/=\s]+$/.test(value) && value.replace(/\s/g, "").length > 120;
  if (looksLikeBase64) {
    return `data:image/png;base64,${value.replace(/\s/g, "")}`;
  }

  return "";
}

function resolveEvolutionInstance(preferred) {
  const fromArg = toNonEmptyString(preferred);
  if (fromArg) {
    return fromArg;
  }

  return toNonEmptyString(process.env.EVOLUTION_INSTANCE);
}

async function createEvolutionInstance(instanceName) {
  const name = toNonEmptyString(instanceName);
  if (!name) {
    const error = new Error("Nome da instancia nao informado.");
    error.status = 400;
    throw error;
  }

  try {
    const { payload, attempt } = await evolutionRequestWithFallback([
      {
        path: "/instance/create",
        method: "POST",
        body: {
          instanceName: name,
          qrcode: true,
          integration: "WHATSAPP-BAILEYS",
        },
      },
      {
        path: "/instance/create",
        method: "POST",
        body: {
          name,
          qrcode: true,
          integration: "WHATSAPP-BAILEYS",
        },
      },
      {
        path: `/instance/create/${name}`,
        method: "POST",
        body: {
          qrcode: true,
          integration: "WHATSAPP-BAILEYS",
        },
      },
    ]);

    return { created: true, payload, attempt };
  } catch (error) {
    const message = String(error.message || "").toLowerCase();
    const details = error?.details;

    const extractErrorTexts = (value) => {
      if (value == null) return [];
      if (typeof value === "string") return [value.toLowerCase()];
      if (Array.isArray(value)) {
        return value.flatMap((item) => extractErrorTexts(item));
      }
      if (typeof value === "object") {
        return Object.values(value).flatMap((item) => extractErrorTexts(item));
      }
      return [String(value).toLowerCase()];
    };

    const errorTexts = [message, ...extractErrorTexts(details)];
    const alreadyExists = errorTexts.some(
      (text) =>
        text.includes("already") ||
        text.includes("already in use") ||
        text.includes("ja existe") ||
        text.includes("exists"),
    );

    if (alreadyExists) {
      return { created: false, alreadyExists: true, message: error.message, details: error.details || null };
    }
    throw error;
  }
}

async function fetchEvolutionQr(instanceName) {
  const instance = toNonEmptyString(instanceName);
  if (!instance) {
    const error = new Error("Nome da instancia nao informado.");
    error.status = 400;
    throw error;
  }

  const { payload, attempt } = await evolutionRequestWithFallback([
    { path: `/instance/connect/${instance}`, method: "GET" },
    { path: `/instance/qrcode/${instance}`, method: "GET" },
    { path: `/instance/qr/${instance}`, method: "GET" },
    { path: `/instance/connect/${instance}`, method: "POST" },
  ]);

  const qrRaw = extractQrValue(payload);
  const qrDataUrl = toQrDataUrl(qrRaw);
  const pairingCode = firstNonEmpty([payload?.pairingCode, payload?.data?.pairingCode]);

  return {
    payload,
    attempt,
    qrRaw,
    qrDataUrl,
    pairingCode,
  };
}

function detectWhatsappMessageType(...candidates) {
  const priority = [
    ["conversation", "text"],
    ["extendedTextMessage", "text"],
    ["buttonsResponseMessage", "button_reply"],
    ["listResponseMessage", "list_reply"],
    ["templateButtonReplyMessage", "template_reply"],
    ["imageMessage", "image"],
    ["videoMessage", "video"],
    ["documentMessage", "document"],
    ["audioMessage", "audio"],
    ["stickerMessage", "sticker"],
    ["contactMessage", "contact"],
    ["locationMessage", "location"],
    ["reactionMessage", "reaction"],
  ];

  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== "object") {
      continue;
    }

    for (const [key, label] of priority) {
      if (candidate[key]) {
        return label;
      }
    }
  }

  return "unknown";
}

function extractIncomingWhatsapp(body) {
  const data = body?.data && typeof body.data === "object" ? body.data : body;
  const firstMessage =
    (Array.isArray(data?.messages) ? data.messages.find((item) => item && typeof item === "object") : null) ||
    (Array.isArray(body?.messages) ? body.messages.find((item) => item && typeof item === "object") : null) ||
    (Array.isArray(data?.data?.messages)
      ? data.data.messages.find((item) => item && typeof item === "object")
      : null);

  const key = (data?.key && typeof data.key === "object" ? data.key : null) || firstMessage?.key || body?.key || {};
  const message =
    (data?.message && typeof data.message === "object" ? data.message : null) ||
    firstMessage?.message ||
    body?.message ||
    {};

  const nestedMessage =
    message?.ephemeralMessage?.message ||
    message?.viewOnceMessage?.message ||
    message?.viewOnceMessageV2?.message ||
    message?.viewOnceMessageV2Extension?.message ||
    {};
  const messageType = detectWhatsappMessageType(message, nestedMessage, firstMessage?.message);

  const senderRaw = firstNonEmpty([
    key?.remoteJid,
    firstMessage?.key?.remoteJid,
    firstMessage?.remoteJid,
    data?.sender,
    data?.from,
    body?.sender,
    body?.from,
  ]);

  const text = firstNonEmpty([
    message?.conversation,
    message?.extendedTextMessage?.text,
    message?.imageMessage?.caption,
    message?.videoMessage?.caption,
    message?.documentMessage?.caption,
    message?.buttonsResponseMessage?.selectedDisplayText,
    message?.listResponseMessage?.title,
    message?.templateButtonReplyMessage?.selectedDisplayText,
    nestedMessage?.conversation,
    nestedMessage?.extendedTextMessage?.text,
    message?.imageMessage?.caption,
    message?.videoMessage?.caption,
    message?.documentMessage?.caption,
    nestedMessage?.imageMessage?.caption,
    nestedMessage?.videoMessage?.caption,
    nestedMessage?.documentMessage?.caption,
    firstMessage?.message?.conversation,
    firstMessage?.message?.extendedTextMessage?.text,
    data?.text,
    data?.body,
    firstMessage?.text,
    firstMessage?.body,
    body?.text,
    body?.body,
  ]);

  const instanceName = resolveEvolutionInstance(
    firstNonEmpty([
      body?.instance,
      body?.instanceName,
      data?.instance,
      data?.instanceName,
    ]),
  );

  return {
    event: firstNonEmpty([body?.event, body?.type, data?.event]),
    fromMe: Boolean(key?.fromMe ?? data?.fromMe ?? body?.fromMe ?? false),
    senderRaw,
    senderNumber: normalizeWhatsappNumber(senderRaw),
    senderName: firstNonEmpty([data?.pushName, body?.pushName, data?.senderName]),
    messageText: text,
    messageId: firstNonEmpty([key?.id, data?.id, body?.id]),
    isGroup: senderRaw.includes("@g.us"),
    messageType,
    instanceName,
  };
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderQrPage({ instance, qrDataUrl, pairingCode, statusMessage, details }) {
  const safeInstance = escapeHtml(instance || "nao informado");
  const safeStatus = escapeHtml(statusMessage || "Aguardando leitura do QR Code.");
  const safePairingCode = escapeHtml(pairingCode || "");
  const safeDetails = escapeHtml(details || "");

  return `<!doctype html>
<html lang="pt-BR">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Conectar WhatsApp - IA.AGENDAMENTO</title>
    <style>
      :root {
        color-scheme: light;
      }
      body {
        margin: 0;
        font-family: "Segoe UI", sans-serif;
        background: radial-gradient(circle at top, #f3f7ff, #eef3f7 55%, #e8eef4);
        color: #123;
      }
      main {
        max-width: 760px;
        margin: 24px auto;
        padding: 24px;
      }
      .card {
        background: #fff;
        border-radius: 16px;
        border: 1px solid #d8e2ed;
        padding: 24px;
        box-shadow: 0 8px 30px rgba(23, 42, 63, 0.08);
      }
      h1 {
        margin-top: 0;
        font-size: 1.5rem;
      }
      .meta {
        margin: 8px 0;
        font-size: 0.95rem;
      }
      .status {
        background: #f7fbff;
        border: 1px solid #d5e6f6;
        border-radius: 10px;
        padding: 10px 12px;
        margin: 12px 0 18px;
      }
      .qr {
        display: grid;
        place-items: center;
        padding: 20px;
        border: 1px dashed #bdd2e5;
        border-radius: 12px;
        background: #fbfdff;
        min-height: 280px;
      }
      img {
        width: min(320px, 100%);
        height: auto;
      }
      .actions {
        display: flex;
        gap: 10px;
        margin-top: 18px;
        flex-wrap: wrap;
      }
      button {
        border: 0;
        border-radius: 10px;
        background: #0a6bd8;
        color: #fff;
        font-weight: 600;
        padding: 10px 14px;
        cursor: pointer;
      }
      code {
        background: #f2f6fa;
        padding: 2px 6px;
        border-radius: 6px;
      }
      pre {
        white-space: pre-wrap;
        word-break: break-word;
        background: #f2f6fa;
        border: 1px solid #d8e2ed;
        border-radius: 10px;
        padding: 10px;
        font-size: 0.85rem;
      }
    </style>
  </head>
  <body>
    <main>
      <div class="card">
        <h1>Conectar WhatsApp por QR Code</h1>
        <div class="meta">Instancia: <code>${safeInstance}</code></div>
        <div class="status">${safeStatus}</div>
        <div class="qr">
          ${qrDataUrl ? `<img src="${qrDataUrl}" alt="QR Code WhatsApp" />` : "<div>QR indisponivel no momento. Clique em atualizar.</div>"}
        </div>
        ${safePairingCode ? `<p class="meta">Pairing code: <code>${safePairingCode}</code></p>` : ""}
        <div class="actions">
          <button onclick="window.location.reload()">Atualizar QR</button>
          <button onclick="window.location.href='/api/evolution/instance/status?instance=${encodeURIComponent(instance || "")}'">Ver status</button>
        </div>
        ${safeDetails ? `<pre>${safeDetails}</pre>` : ""}
      </div>
    </main>
  </body>
</html>`;
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return { raw: value };
  }
}

function scoreServiceMatch(serviceName, candidate) {
  const normalizeServiceText = (value) =>
    normalizeForMatch(value)
      .replace(/[^a-z0-9\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();

  const targetRaw = normalizeForMatch(serviceName).trim();
  const candidateRaw = normalizeForMatch(candidate?.nome).trim();
  const target = normalizeServiceText(serviceName);
  const candidateName = normalizeServiceText(candidate?.nome);

  if (!targetRaw || !candidateRaw || !target || !candidateName) {
    return 0;
  }

  if (target === candidateName || targetRaw === candidateRaw) {
    return 1;
  }

  const targetTokens = target.split(" ").filter(Boolean);
  const candidateTokens = candidateName.split(" ").filter(Boolean);
  if (!targetTokens.length || !candidateTokens.length) {
    return 0;
  }

  const candidateSet = new Set(candidateTokens);
  const overlap = targetTokens.filter((token) => candidateSet.has(token)).length;
  if (!overlap) {
    return 0;
  }

  // Caso comum: cliente digita apenas o nome base do servico ("escova")
  // e a Trinks retorna variantes ("escova definitiva", "escova*"). Aqui
  // privilegiamos o match mais direto para evitar mudar o servico no meio do fluxo.
  const allTargetTokensPresent = overlap === targetTokens.length;
  if (allTargetTokensPresent) {
    const extraTokens = Math.max(0, candidateTokens.length - targetTokens.length);
    return Math.max(0.6, 0.98 - (extraTokens * 0.08));
  }

  if (candidateName.includes(target) || target.includes(candidateName)) {
    return 0.75;
  }

  const coverage = overlap / targetTokens.length;
  const precision = overlap / candidateTokens.length;
  const combined = (coverage * 0.7) + (precision * 0.3);
  return combined >= 0.5 ? combined : 0;
}

function findBestServiceMatch(serviceName, services) {
  let best = null;
  for (const service of services) {
    const score = scoreServiceMatch(serviceName, service);
    if (!best || score > best.score) {
      best = { score, service };
    }
  }

  if (!best || best.score < 0.55) {
    return null;
  }

  return best.service;
}

async function findServiceByName(estabelecimentoId, serviceName) {
  const normalizedInput = toNonEmptyString(serviceName);
  if (!normalizedInput) {
    return null;
  }

  const directPayload = await trinksRequest("/servicos", {
    method: "GET",
    estabelecimentoId,
    query: {
      nome: normalizedInput,
      page: 1,
      pageSize: 100,
    },
  });

  const directItems = extractItems(directPayload);
  const directMatch = findBestServiceMatch(normalizedInput, directItems);
  if (directMatch) {
    return directMatch;
  }

  // Segunda tentativa: busca por termos importantes da frase (ex.: "pedicure").
  const tokenQueries = [...new Set(tokenizeMeaningfulText(normalizedInput).filter((token) => token.length >= 4))].slice(
    0,
    6,
  );

  for (const token of tokenQueries) {
    const tokenPayload = await trinksRequest("/servicos", {
      method: "GET",
      estabelecimentoId,
      query: {
        nome: token,
        page: 1,
        pageSize: 100,
      },
    });

    const tokenItems = extractItems(tokenPayload);
    const tokenMatch = findBestServiceMatch(normalizedInput, tokenItems);
    if (tokenMatch) {
      return tokenMatch;
    }
  }

  // Fallback: varre algumas paginas para cobrir nomes com acento/variacoes.
  const fallbackItems = [];
  const maxPages = 6;
  for (let page = 1; page <= maxPages; page += 1) {
    const payload = await trinksRequest("/servicos", {
      method: "GET",
      estabelecimentoId,
      query: {
        page,
        pageSize: 100,
      },
    });
    const items = extractItems(payload);
    fallbackItems.push(...items);
    if (items.length < 100) {
      break;
    }
  }

  return findBestServiceMatch(normalizedInput, fallbackItems);
}

function collectProfessionalsFromPayload(payload) {
  const result = [];
  const seen = new Set();

  function visit(node) {
    if (!node) return;
    if (Array.isArray(node)) {
      node.forEach(visit);
      return;
    }
    if (typeof node !== "object") return;

    const id = professionalIdFrom(node);
    const name = professionalNameFrom(node);
    if (id && name) {
      const key = `${id}:${String(name).toLowerCase()}`;
      if (!seen.has(key)) {
        seen.add(key);
        result.push({ id: Number(id), name: String(name) });
      }
    }

    for (const value of Object.values(node)) {
      visit(value);
    }
  }

  visit(payload);
  return result;
}

function normalizeTimeValue(value) {
  const raw = toNonEmptyString(value).replace(/\./g, ":").toLowerCase();
  if (!raw) {
    return "";
  }

  let match = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) {
    match = raw.match(/^(\d{1,2})h(?:\s*(\d{2}))?$/);
  }
  if (!match) {
    match = raw.match(/^(\d{1,2})$/);
  }
  if (!match) {
    return "";
  }

  const hours = Number(match[1]);
  const minutes = Number(match[2] || "00");
  if (!Number.isInteger(hours) || !Number.isInteger(minutes) || hours < 0 || hours > 23 || minutes < 0 || minutes > 59) {
    return "";
  }

  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
}

function parseTimeToMinutes(value) {
  const normalized = normalizeTimeValue(value);
  if (!normalized) {
    return null;
  }
  const [hours, minutes] = normalized.split(":").map((part) => Number(part));
  return hours * 60 + minutes;
}

function uniqueSortedTimes(values) {
  const set = new Set(
    (Array.isArray(values) ? values : [])
      .map((item) => normalizeTimeValue(item))
      .filter(Boolean),
  );

  return [...set].sort((a, b) => parseTimeToMinutes(a) - parseTimeToMinutes(b));
}

function extractAvailableTimesFromProfessional(item) {
  const candidates = [
    item?.horariosVagos,
    item?.horariosDisponiveis,
    item?.horariosLivres,
    item?.availableTimes,
  ];

  for (const list of candidates) {
    if (Array.isArray(list) && list.length) {
      return uniqueSortedTimes(list);
    }
  }

  return [];
}

function extractAvailableIntervalsFromProfessional(item) {
  const candidates = [
    item?.intervalosVagos,
    item?.intervalosDisponiveis,
    item?.intervalosLivres,
    item?.availableIntervals,
  ];

  for (const list of candidates) {
    if (!Array.isArray(list) || !list.length) {
      continue;
    }

    const normalized = list
      .map((interval) => {
        const start = normalizeTimeValue(interval?.inicio || interval?.start);
        const end = normalizeTimeValue(interval?.fim || interval?.end);
        if (!start || !end) {
          return null;
        }
        return { inicio: start, fim: end };
      })
      .filter(Boolean);

    if (normalized.length) {
      const unique = new Map();
      for (const interval of normalized) {
        unique.set(`${interval.inicio}|${interval.fim}`, interval);
      }
      return [...unique.values()].sort((a, b) => parseTimeToMinutes(a.inicio) - parseTimeToMinutes(b.inicio));
    }
  }

  return [];
}

function isSlotCompatibleWithIntervals(startTime, durationMinutes, intervals) {
  const start = parseTimeToMinutes(startTime);
  if (!Number.isFinite(start)) {
    return false;
  }

  const duration = Number(durationMinutes);
  const safeDuration = Number.isFinite(duration) && duration > 0 ? duration : 0;
  const end = start + safeDuration;
  const availableIntervals = Array.isArray(intervals) ? intervals : [];

  if (!availableIntervals.length) {
    return true;
  }

  return availableIntervals.some((interval) => {
    const intervalStart = parseTimeToMinutes(interval?.inicio);
    const intervalEnd = parseTimeToMinutes(interval?.fim);
    if (!Number.isFinite(intervalStart) || !Number.isFinite(intervalEnd)) {
      return false;
    }
    return start >= intervalStart && end <= intervalEnd;
  });
}

function rankTimesByPreferredTime(times, preferredTime) {
  const normalizedPreferred = normalizeTimeValue(preferredTime);
  const preferredMinutes = parseTimeToMinutes(normalizedPreferred);
  const normalizedTimes = uniqueSortedTimes(times);

  if (!Number.isFinite(preferredMinutes)) {
    return normalizedTimes;
  }

  return [...normalizedTimes].sort((left, right) => {
    const leftMinutes = parseTimeToMinutes(left);
    const rightMinutes = parseTimeToMinutes(right);
    const leftDiff = Math.abs(leftMinutes - preferredMinutes);
    const rightDiff = Math.abs(rightMinutes - preferredMinutes);
    if (leftDiff !== rightDiff) {
      return leftDiff - rightDiff;
    }
    return leftMinutes - rightMinutes;
  });
}

function extractPreferredTimeFromMessage(message) {
  const normalized = normalizeForMatch(message);
  if (!normalized) {
    return "";
  }

  const patterns = [
    /(?:as|a|às)\s*(\d{1,2})(?::(\d{2}))?/i,
    /\b(\d{1,2})h(?:\s*(\d{2}))?\b/i,
    /\b(\d{1,2}):(\d{2})\b/,
  ];

  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (!match) {
      continue;
    }
    const candidate = `${match[1]}:${match[2] || "00"}`;
    const normalizedTime = normalizeTimeValue(candidate);
    if (normalizedTime) {
      return normalizedTime;
    }
  }

  return "";
}

function findProfessionalByName(professionals, professionalName) {
  const normalized = normalizeForMatch(professionalName).trim();
  if (!normalized) {
    return null;
  }

  const normalizeName = (item) => normalizeForMatch(item?.name || "").trim();
  const firstName = (item) => normalizeForMatch(item?.name || "").split(/\s+/).filter(Boolean)[0] || "";
  const rank = (items) =>
    [...items].sort((left, right) => {
      const leftSlots = Array.isArray(left?.availableTimes) ? left.availableTimes.length : 0;
      const rightSlots = Array.isArray(right?.availableTimes) ? right.availableTimes.length : 0;
      return rightSlots - leftSlots;
    });

  const exactMatches = rank(professionals.filter((item) => normalizeName(item) === normalized));
  if (exactMatches.length) {
    return exactMatches[0];
  }

  const firstNameMatches = rank(professionals.filter((item) => firstName(item) === normalized));
  if (firstNameMatches.length) {
    return firstNameMatches[0];
  }

  const partialMatches = rank(professionals.filter((item) => normalizeName(item).includes(normalized)));
  if (partialMatches.length) {
    return partialMatches[0];
  }

  return null;
}

function buildGlobalSuggestedSlots(professionals, preferredTime, maxSuggestions = 8) {
  const candidates = [];
  for (const professional of professionals) {
    const displayName = professionalDisplayName(professional.name);
    const ranked = rankTimesByPreferredTime(professional.availableTimes, preferredTime);
    for (const time of ranked) {
      candidates.push({
        time,
        professionalName: displayName || professional.name,
        distance: Math.abs((parseTimeToMinutes(time) || 0) - (parseTimeToMinutes(preferredTime) || 0)),
      });
    }
  }

  candidates.sort((left, right) => {
    if (normalizeTimeValue(preferredTime)) {
      if (left.distance !== right.distance) {
        return left.distance - right.distance;
      }
    }
    const leftMinutes = parseTimeToMinutes(left.time) || 0;
    const rightMinutes = parseTimeToMinutes(right.time) || 0;
    if (leftMinutes !== rightMinutes) {
      return leftMinutes - rightMinutes;
    }
    return left.professionalName.localeCompare(right.professionalName, "pt-BR");
  });

  const grouped = new Map();
  for (const item of candidates) {
    const entry = grouped.get(item.time) || { time: item.time, professionals: [] };
    if (!entry.professionals.includes(item.professionalName)) {
      entry.professionals.push(item.professionalName);
    }
    grouped.set(item.time, entry);
    if (grouped.size >= maxSuggestions) {
      break;
    }
  }

  return [...grouped.values()];
}

function formatTimeFromDateTime(value) {
  const text = toNonEmptyString(value);
  if (!text) {
    return "";
  }

  const split = text.includes("T") ? text.split("T")[1] : text.split(" ")[1];
  if (!split) {
    return "";
  }

  return split.slice(0, 5);
}

function formatDateFromDateTime(value) {
  const text = toNonEmptyString(value);
  if (!text) {
    return "";
  }

  if (text.includes("T")) {
    return text.slice(0, 10);
  }

  const firstToken = text.split(" ")[0];
  if (/^\d{4}-\d{2}-\d{2}$/.test(firstToken)) {
    return firstToken;
  }

  return "";
}

function extractServiceNames(item) {
  const direct = firstNonEmpty([
    item?.servico?.nome,
    item?.servicoNome,
    item?.servico,
  ]);

  if (direct) {
    return [direct];
  }

  if (Array.isArray(item?.servicos)) {
    return item.servicos
      .map((service) => firstNonEmpty([service?.nome, service?.servico?.nome, service?.name]))
      .filter(Boolean);
  }

  return [];
}

function normalizeAppointmentItem(item) {
  if (!item || typeof item !== "object") {
    return null;
  }

  const start = firstNonEmpty([item?.dataHoraInicio, item?.dataHora, item?.inicio, item?.start]);
  const time = formatTimeFromDateTime(start);
  const professional = firstNonEmpty([
    item?.profissional?.nome,
    item?.profissionalNome,
    item?.professionalName,
  ]);
  const client = firstNonEmpty([item?.cliente?.nome, item?.clienteNome, item?.clientName]);
  const services = extractServiceNames(item);

  return {
    id: item?.id ?? item?.agendamentoId ?? null,
    dateTime: start || null,
    date: formatDateFromDateTime(start) || null,
    time: time || null,
    professional: professional || null,
    client: client || null,
    services,
    raw: item,
  };
}

async function getAppointmentsForDate(establishmentId, date) {
  const attempts = [
    {
      query: { data: date, page: 1, pageSize: 200 },
      label: "data",
    },
    {
      query: { dataInicial: `${date}T00:00:00`, dataFinal: `${date}T23:59:59`, page: 1, pageSize: 200 },
      label: "dataInicial-dataFinal",
    },
    {
      query: { dataInicial: date, dataFinal: date, page: 1, pageSize: 200 },
      label: "dataInicial-dataFinal-short",
    },
  ];

  const errors = [];

  for (const attempt of attempts) {
    try {
      const payload = await trinksRequest("/agendamentos", {
        method: "GET",
        estabelecimentoId: establishmentId,
        query: attempt.query,
      });

      const items = extractItems(payload);
      return {
        source: attempt.label,
        items,
        raw: payload,
      };
    } catch (error) {
      errors.push({
        label: attempt.label,
        message: error.message || "Erro desconhecido",
        details: error.details || null,
        status: error.status || null,
      });
    }
  }

  const aggregate = new Error("Falha ao buscar agendamentos do dia na Trinks.");
  aggregate.status = errors[0]?.status || 500;
  aggregate.details = errors;
  throw aggregate;
}

async function getProfessionals({ establishmentId, date, serviceId }) {
  const payload = await trinksRequest(`/agendamentos/profissionais/${date}`, {
    method: "GET",
    estabelecimentoId: establishmentId,
    query: {
      serviceId: serviceId || undefined,
      servicoId: serviceId || undefined,
    },
  });

  const items = extractItems(payload);
  if (!items.length) {
    return collectProfessionalsFromPayload(payload).map((item) => ({
      ...item,
      availableTimes: [],
      availableIntervals: [],
      raw: null,
    }));
  }

  const aggregated = new Map();
  for (const item of items) {
    const id = Number(professionalIdFrom(item));
    const name = toNonEmptyString(professionalNameFrom(item));
    if (!id || !name) {
      continue;
    }

    const key = `${id}:${normalizeForMatch(name)}`;
    const previous = aggregated.get(key) || {
      id,
      name,
      availableTimes: [],
      availableIntervals: [],
      raw: item,
    };

    previous.availableTimes = uniqueSortedTimes([
      ...previous.availableTimes,
      ...extractAvailableTimesFromProfessional(item),
    ]);

    const intervalMap = new Map();
    for (const interval of [...previous.availableIntervals, ...extractAvailableIntervalsFromProfessional(item)]) {
      intervalMap.set(`${interval.inicio}|${interval.fim}`, interval);
    }
    previous.availableIntervals = [...intervalMap.values()].sort(
      (left, right) => parseTimeToMinutes(left.inicio) - parseTimeToMinutes(right.inicio),
    );

    aggregated.set(key, previous);
  }

  return [...aggregated.values()].sort((left, right) =>
    left.name.localeCompare(right.name, "pt-BR"),
  );
}

async function findProfessionalForBooking({ establishmentId, date, professionalName, serviceId }) {
  const professionals = await getProfessionals({ establishmentId, date, serviceId });
  if (!professionals.length) {
    const error = new Error("Nenhuma profissional disponivel para a data informada.");
    error.status = 422;
    throw error;
  }

  if (!professionalName) {
    const withOpenSchedule = professionals.filter((item) => item.availableTimes.length > 0);
    return withOpenSchedule[0] || professionals[0];
  }

  const matched = findProfessionalByName(professionals, professionalName);
  if (matched) {
    return matched;
  }

  const error = new Error(`Profissional nao encontrada para: ${professionalName}`);
  error.status = 404;
  throw error;
}

async function findOrCreateClient(estabelecimentoId, clientName, clientPhone) {
  const searches = [];
  const normalizedPhone = normalizePhone(clientPhone);
  const normalizedName = toNonEmptyString(clientName);
  const mapped = normalizedPhone ? getMappedClientByPhone(normalizedPhone) : null;

  if (normalizedPhone) {
    searches.push(listClients(estabelecimentoId, { telefone: normalizedPhone }));
    searches.push(listClients(estabelecimentoId, { phone: normalizedPhone }));
    searches.push(listClients(estabelecimentoId, { celular: normalizedPhone }));
  }

  if (clientName) {
    searches.push(listClients(estabelecimentoId, { nome: clientName }));
  }
  searches.push(listClients(estabelecimentoId));

  const searchResults = await Promise.allSettled(searches);
  const candidates = dedupeClients(
    searchResults
      .filter((result) => result.status === "fulfilled")
      .flatMap((result) => result.value),
  );

  const matchingPool = candidates.filter(
    (item) =>
      (normalizedPhone && matchesClientPhone(item, normalizedPhone)) ||
      (normalizedName && matchesClientName(item, normalizedName)),
  );
  const mappedMatch = mapped?.clientId
    ? matchingPool.find((item) => Number(clientIdFrom(item)) === Number(mapped.clientId))
    : null;
  const existing = mappedMatch || pickBestClientCandidate(matchingPool, {
    clientPhone: normalizedPhone,
    clientName: normalizedName,
  });

  if (existing) {
    const selectedId = Number(clientIdFrom(existing));
    if (normalizedPhone && Number.isFinite(selectedId) && selectedId > 0) {
      upsertClientPhoneMap(normalizedPhone, selectedId, clientDisplayNameFrom(existing));
    }
    return existing;
  }

  if (mapped?.clientId && normalizedPhone) {
    return {
      id: mapped.clientId,
      nome: mapped.clientName || normalizedName,
      telefones: [{ numero: normalizedPhone }],
      source: "phone_map_fallback",
    };
  }

  const parsedPhone = parseBrazilianPhone(clientPhone);
  const phoneEntry = parsedPhone
    ? { ddi: parsedPhone.ddi, ddd: parsedPhone.ddd, numero: parsedPhone.numero, tipoId: 1 }
    : { numero: normalizePhone(clientPhone), tipoId: 1 };

  const createPayload = await trinksRequest("/clientes", {
    method: "POST",
    estabelecimentoId,
    body: {
      nome: clientName,
      telefones: [phoneEntry],
    },
  });

  const created = createPayload?.item || createPayload?.data || createPayload;
  const createdId = Number(clientIdFrom(created));
  if (normalizedPhone && Number.isFinite(createdId) && createdId > 0) {
    upsertClientPhoneMap(normalizedPhone, createdId, clientDisplayNameFrom(created) || normalizedName);
  }
  return created;
}

async function getAvailability(
  establishmentId,
  service,
  date,
  { professionalName = "", preferredTime = "", strictProfessional = false } = {},
) {
  const foundService = await findServiceByName(establishmentId, service);
  if (!foundService) {
    return {
      availableTimes: [],
      professionals: [],
      suggestions: [],
      message: `Servico nao encontrado para: ${service}`,
    };
  }

  const serviceId = Number(serviceIdFrom(foundService));
  const resolvedServiceName = toNonEmptyString(
    foundService?.nome || foundService?.name || foundService?.servicoNome || service,
  ) || toNonEmptyString(service);
  const duration = Number(
    foundService?.duracaoEmMinutos || foundService?.duracao || foundService?.duracaoMinutos || 60,
  );
  const durationMinutes = Number.isFinite(duration) ? duration : 60;
  const serviceAmount = Number(foundService?.valor || foundService?.preco || 0);

  const professionals = await getProfessionals({
    establishmentId,
    date,
    serviceId: Number.isFinite(serviceId) ? serviceId : undefined,
  });

  const requestedProfessional = toNonEmptyString(professionalName);
  const normalizedPreferredTime = normalizeTimeValue(preferredTime);

  const byProfessional = professionals.map((professional) => {
    const compatibleTimes = uniqueSortedTimes(
      professional.availableTimes.filter((time) =>
        isSlotCompatibleWithIntervals(time, durationMinutes, professional.availableIntervals),
      ),
    );
    return {
      id: professional.id,
      name: professional.name,
      availableTimes: compatibleTimes,
      availableIntervals: professional.availableIntervals,
    };
  });

  let byProfessionalAllDay = byProfessional;
  if (!strictProfessional) {
    try {
      const allDayProfessionalsRaw = await getProfessionals({
        establishmentId,
        date,
      });
      byProfessionalAllDay = allDayProfessionalsRaw.map((professional) => {
        const compatibleTimes = uniqueSortedTimes(
          professional.availableTimes.filter((time) =>
            isSlotCompatibleWithIntervals(time, durationMinutes, professional.availableIntervals),
          ),
        );
        return {
          id: professional.id,
          name: professional.name,
          availableTimes: compatibleTimes,
          availableIntervals: professional.availableIntervals,
        };
      });
    } catch {
      byProfessionalAllDay = byProfessional;
    }
  }

  const allOpenProfessionals = byProfessional.filter(professionalHasOpenSchedule);
  const allOpenDisplayNames = uniqueProfessionalDisplayNames(allOpenProfessionals.map((item) => item.name));
  const allOpenProfessionalsDay = byProfessionalAllDay.filter(professionalHasOpenSchedule);
  const allOpenDisplayNamesDay = uniqueProfessionalDisplayNames(allOpenProfessionalsDay.map((item) => item.name));
  const professionalsAtPreferredTime = normalizedPreferredTime
    ? uniqueProfessionalDisplayNames(
        allOpenProfessionals
          .filter((item) => Array.isArray(item.availableTimes) && item.availableTimes.includes(normalizedPreferredTime))
          .map((item) => item.name),
      )
    : [];
  const professionalsAtPreferredTimeDay = normalizedPreferredTime
    ? uniqueProfessionalDisplayNames(
        allOpenProfessionalsDay
          .filter((item) => Array.isArray(item.availableTimes) && item.availableTimes.includes(normalizedPreferredTime))
          .map((item) => item.name),
      )
    : [];

  let scopedProfessionals = allOpenProfessionals;
  let requestedProfessionalDisplay = requestedProfessional ? professionalDisplayName(requestedProfessional) : null;
  let preferredProfessionalUnavailable = false;
  let preferredProfessionalTimes = [];
  let preferredProfessionalGeneralTimes = [];
  let preferredProfessionalNearestTimes = [];
  let otherOpenDisplayNames = allOpenDisplayNames;
  let otherProfessionalsAtPreferredTime = professionalsAtPreferredTime;

  if (requestedProfessional) {
    const matched = findProfessionalByName(byProfessional, requestedProfessional);
    if (!matched) {
      requestedProfessionalDisplay = professionalDisplayName(requestedProfessional);
      if (!strictProfessional) {
        preferredProfessionalUnavailable = true;
        const matchedAllDay = findProfessionalByName(byProfessionalAllDay, requestedProfessional);
        preferredProfessionalGeneralTimes = matchedAllDay?.availableTimes ? uniqueSortedTimes(matchedAllDay.availableTimes) : [];
        const normalizedRequested = normalizeForMatch(requestedProfessional);
        otherOpenDisplayNames = uniqueProfessionalDisplayNames(
          allOpenProfessionals
            .filter((item) => normalizeForMatch(item.name) !== normalizedRequested)
            .map((item) => item.name),
        );
        otherProfessionalsAtPreferredTime = normalizedPreferredTime
          ? uniqueProfessionalDisplayNames(
              allOpenProfessionals
                .filter((item) => normalizeForMatch(item.name) !== normalizedRequested)
                .filter((item) => item.availableTimes.includes(normalizedPreferredTime))
                .map((item) => item.name),
            )
          : otherOpenDisplayNames;
      } else {
        const error = new Error(
          `${requestedProfessionalDisplay} nao possui disponibilidade para este servico nessa data. Caso queira, posso verificar com outras profissionais.`,
        );
        error.status = 404;
        error.details = {
          requestedProfessional: requestedProfessionalDisplay,
          availableProfessionals: byProfessional.map((item) => item.name),
        };
        throw error;
      }
    } else {
      requestedProfessionalDisplay = professionalDisplayName(matched.name);
      preferredProfessionalTimes = uniqueSortedTimes(matched.availableTimes);
      preferredProfessionalNearestTimes = rankTimesByPreferredTime(matched.availableTimes, normalizedPreferredTime).slice(0, 8);
      const matchedAllDay = findProfessionalByName(byProfessionalAllDay, requestedProfessional);
      preferredProfessionalGeneralTimes = matchedAllDay?.availableTimes ? uniqueSortedTimes(matchedAllDay.availableTimes) : [];
      otherOpenDisplayNames = uniqueProfessionalDisplayNames(
        allOpenProfessionals
          .filter((item) => normalizeForMatch(item.name) !== normalizeForMatch(matched.name))
          .map((item) => item.name),
      );
      otherProfessionalsAtPreferredTime = normalizedPreferredTime
        ? uniqueProfessionalDisplayNames(
            allOpenProfessionals
              .filter((item) => normalizeForMatch(item.name) !== normalizeForMatch(matched.name))
              .filter((item) => item.availableTimes.includes(normalizedPreferredTime))
              .map((item) => item.name),
          )
        : otherOpenDisplayNames;

      if (!professionalHasOpenSchedule(matched)) {
        preferredProfessionalUnavailable = true;
        if (strictProfessional) {
          const error = new Error(
            `${requestedProfessionalDisplay} nao possui agenda aberta para este servico nesta data. Caso queira, posso verificar com outras profissionais.`,
          );
          error.status = 409;
          error.details = {
            requestedProfessional: requestedProfessionalDisplay,
            requestedDate: date,
            preferredProfessionalTimes,
            preferredProfessionalGeneralTimes,
            preferredProfessionalNearestTimes,
            otherProfessionals: otherOpenDisplayNames,
            otherProfessionalsAtPreferredTime,
          };
          throw error;
        }
      } else {
        if (normalizedPreferredTime && !matched.availableTimes.includes(normalizedPreferredTime)) {
          preferredProfessionalUnavailable = true;
          if (strictProfessional) {
            const preferredTimesList = preferredProfessionalNearestTimes.length
              ? preferredProfessionalNearestTimes
              : preferredProfessionalTimes;
            const error = new Error(
              preferredTimesList.length
                ? `${requestedProfessionalDisplay} nao possui agenda livre as ${normalizedPreferredTime}. Horarios de ${requestedProfessionalDisplay} no dia ${isoToBrDate(date) || date}: ${preferredTimesList.join(", ")}. Caso esses horarios nao sirvam para voce, quer saber a disponibilidade de outros profissionais?`
                : `${requestedProfessionalDisplay} nao possui agenda livre as ${normalizedPreferredTime}. Caso esses horarios nao sirvam para voce, quer saber a disponibilidade de outros profissionais?`,
            );
            error.status = 409;
            error.details = {
              requestedProfessional: requestedProfessionalDisplay,
              requestedDate: date,
              requestedTime: normalizedPreferredTime,
              preferredProfessionalTimes,
              preferredProfessionalGeneralTimes,
              preferredProfessionalNearestTimes,
              otherProfessionals: otherOpenDisplayNames,
              otherProfessionalsAtPreferredTime,
            };
            throw error;
          }
        } else {
          scopedProfessionals = [matched];
        }
      }
    }
  }

  const flattenedTimes = uniqueSortedTimes(scopedProfessionals.flatMap((item) => item.availableTimes));
  const scopedWithNearest = scopedProfessionals.map((item) => ({
    ...item,
    nearestTimes: rankTimesByPreferredTime(item.availableTimes, normalizedPreferredTime).slice(0, 8),
  }));
  const suggestions = buildGlobalSuggestedSlots(scopedWithNearest, normalizedPreferredTime, 12);

  const responseProfessionals = scopedWithNearest.map((item) => ({
    id: item.id,
    name: professionalDisplayName(item.name),
    availableTimes: item.availableTimes,
    availableIntervals: item.availableIntervals,
    nearestTimes: item.nearestTimes,
  }));

  return {
    serviceId: Number.isFinite(serviceId) ? serviceId : null,
    serviceName: resolvedServiceName,
    durationMinutes,
    serviceAmount: Number.isFinite(serviceAmount) ? serviceAmount : 0,
    availableTimes: flattenedTimes,
    occupiedTimes: [],
    professionals: responseProfessionals,
    suggestions,
    professionalsAtPreferredTime,
    allOpenProfessionals: allOpenDisplayNames,
    allOpenProfessionalsDay: allOpenDisplayNamesDay,
    requestedProfessional: requestedProfessionalDisplay,
    preferredProfessionalUnavailable,
    preferredProfessionalTimes,
    preferredProfessionalGeneralTimes,
    preferredProfessionalNearestTimes,
    otherProfessionals: otherOpenDisplayNames,
    otherProfessionalsAtPreferredTime,
    professionalsAtPreferredTimeDay,
    preferredTime: normalizedPreferredTime || null,
    message:
      requestedProfessional
        ? preferredProfessionalUnavailable
          ? preferredProfessionalNearestTimes.length || preferredProfessionalTimes.length
            ? `${requestedProfessionalDisplay} tem disponibilidade para este servico no dia ${isoToBrDate(date) || date} em: ${
                (preferredProfessionalNearestTimes.length
                  ? preferredProfessionalNearestTimes
                  : preferredProfessionalTimes).join(", ")
              }. Caso esses horarios nao sirvam para voce, quer saber a disponibilidade de outros profissionais?`
            : `${requestedProfessionalDisplay} nao tem disponibilidade para este servico no dia ${isoToBrDate(date) || date}. Quer que eu verifique a disponibilidade de outros profissionais?`
          : `Horarios com ${requestedProfessionalDisplay} em ${isoToBrDate(date) || date}.`
        : normalizedPreferredTime
          ? professionalsAtPreferredTime.length
            ? `Para ${service} em ${date} as ${normalizedPreferredTime}, profissionais disponiveis: ${professionalsAtPreferredTime.join(", ")}.${
                professionalsAtPreferredTimeDay.length &&
                normalizeForMatch(professionalsAtPreferredTimeDay.join(",")) !== normalizeForMatch(professionalsAtPreferredTime.join(","))
                  ? ` No salao, em agenda geral no mesmo horario, tambem aparecem: ${professionalsAtPreferredTimeDay.join(", ")} (nem todas atendem este servico).`
                  : ""
              }`
            : `Nao encontrei profissionais disponiveis para ${service} em ${date} as ${normalizedPreferredTime}. Profissionais com agenda neste dia: ${allOpenDisplayNames.join(", ") || "nenhuma"}.${
                allOpenDisplayNamesDay.length &&
                normalizeForMatch(allOpenDisplayNamesDay.join(",")) !== normalizeForMatch(allOpenDisplayNames.join(","))
                  ? ` Em agenda geral do salao no dia, tambem aparecem: ${allOpenDisplayNamesDay.join(", ")} (nem todas atendem este servico).`
                  : ""
              }`
          : `Horarios consultados para ${service} em ${date}`,
  };
}

async function upsertAppointmentConfirmationNote({
  establishmentId,
  appointmentId,
  confirmationCode,
  requestReference = "",
}) {
  if (!appointmentId) {
    return { updated: false, reason: "missingData" };
  }

  const note = buildAppointmentObservation({ requestReference, confirmationCode });
  const attempts = [
    { method: "PATCH", body: { observacoesDoEstabelecimento: note } },
    { method: "PATCH", body: { observacoes: note } },
    { method: "PUT", body: { observacoesDoEstabelecimento: note } },
    { method: "PUT", body: { observacoes: note } },
  ];

  const errors = [];
  for (const attempt of attempts) {
    try {
      const response = await trinksRequest(`/agendamentos/${appointmentId}`, {
        method: attempt.method,
        estabelecimentoId: establishmentId,
        body: attempt.body,
      });
      return { updated: true, note, method: attempt.method, body: attempt.body, response };
    } catch (error) {
      errors.push({
        method: attempt.method,
        body: attempt.body,
        message: error.message || "Erro desconhecido",
      });
    }
  }

  return {
    updated: false,
    note,
    errors,
  };
}

async function resolveBookingPreviewItem({
  establishmentId,
  service,
  date,
  time,
  professionalName,
}) {
  const normalizedService = toNonEmptyString(service);
  const normalizedDate = normalizeBookingDate(date);
  const normalizedTime = normalizeBookingTime(time);
  const requestedProfessional = toNonEmptyString(professionalName);

  if (!normalizedService || !normalizedDate || !normalizedTime) {
    const error = new Error("Cada item do agendamento precisa de servico, data e horario validos.");
    error.status = 400;
    throw error;
  }

  const availability = await getAvailability(
    establishmentId,
    normalizedService,
    normalizedDate,
    {
      professionalName: requestedProfessional,
      preferredTime: normalizedTime,
      strictProfessional: true,
    },
  );

  const professionals = Array.isArray(availability?.professionals) ? availability.professionals : [];
  if (!professionals.length) {
    const error = new Error(
      `Nao encontrei profissionais com agenda aberta para ${normalizedService} em ${normalizedDate}.`,
    );
    error.status = 409;
    throw error;
  }

  let selected = null;
  if (requestedProfessional) {
    selected = professionals.find(
      (item) => normalizeForMatch(item.name) === normalizeForMatch(professionalDisplayName(requestedProfessional)),
    );
    if (!selected) {
      const error = new Error(`Profissional nao encontrada para: ${requestedProfessional}.`);
      error.status = 404;
      throw error;
    }
  } else {
    selected = professionals.find((item) => Array.isArray(item.availableTimes) && item.availableTimes.includes(normalizedTime));
    if (!selected) {
      selected = professionals[0] || null;
    }
  }

  if (!selected || !Array.isArray(selected.availableTimes) || !selected.availableTimes.includes(normalizedTime)) {
    const nearest = Array.isArray(selected?.nearestTimes) ? selected.nearestTimes.slice(0, 5) : [];
    const display = professionalDisplayName(selected?.name || requestedProfessional || "");
    const alternativesAtSameTime = uniqueProfessionalDisplayNames(
      professionals
        .filter((item) => normalizeForMatch(item.name) !== normalizeForMatch(display))
        .filter((item) => Array.isArray(item.availableTimes) && item.availableTimes.includes(normalizedTime))
        .map((item) => item.name),
    );
    const error = new Error(
      display
        ? nearest.length
          ? `${display} nao possui agenda livre as ${normalizedTime}. Horarios de ${display} no dia ${isoToBrDate(normalizedDate) || normalizedDate}: ${nearest.join(", ")}. Caso esses horarios nao sirvam para voce, quer saber a disponibilidade de outros profissionais?`
          : `${display} nao possui agenda livre as ${normalizedTime}. Caso esses horarios nao sirvam para voce, quer saber a disponibilidade de outros profissionais?`
        : `Nao encontrei agenda livre as ${normalizedTime} para ${normalizedService}.`,
    );
    error.status = 409;
    error.details = {
      service: normalizedService,
      date: normalizedDate,
      time: normalizedTime,
      professionalName: display || null,
      nearestTimes: nearest,
      alternativesAtSameTime,
      suggestions: availability?.suggestions || [],
    };
    throw error;
  }

  return {
    service: normalizedService,
    serviceResolvedName: toNonEmptyString(availability?.serviceName) || normalizedService,
    serviceId: Number.isFinite(Number(availability?.serviceId)) ? Number(availability.serviceId) : null,
    durationMinutes: Number.isFinite(Number(availability?.durationMinutes))
      ? Number(availability.durationMinutes)
      : null,
    serviceAmount: Number.isFinite(Number(availability?.serviceAmount))
      ? Number(availability.serviceAmount)
      : 0,
    date: normalizedDate,
    time: normalizedTime,
    professionalId: Number.isFinite(Number(selected?.id)) ? Number(selected.id) : null,
    professionalName: professionalDisplayName(selected.name),
  };
}

async function executeConfirmedBookings({ establishmentId, clientName, clientPhone, items }) {
  const successes = [];
  let failures = [];
  let resolvedClientId = null;

  try {
    const client = await findOrCreateClient(establishmentId, clientName, clientPhone);
    const parsedClientId = Number(clientIdFrom(client));
    resolvedClientId = Number.isFinite(parsedClientId) && parsedClientId > 0 ? parsedClientId : null;
  } catch {
    resolvedClientId = null;
  }

  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (index > 0 && BOOKING_SEQUENCE_GAP_MS > 0) {
      await new Promise((resolve) => setTimeout(resolve, BOOKING_SEQUENCE_GAP_MS));
    }

    try {
      const created = await createAppointment({
        establishmentId,
        service: item.service,
        serviceResolvedName: item.serviceResolvedName,
        serviceId: item.serviceId,
        durationMinutes: item.durationMinutes,
        serviceAmount: item.serviceAmount,
        clientId: resolvedClientId,
        date: item.date,
        time: item.time,
        professionalId: item.professionalId,
        professionalName: item.professionalName,
        clientName,
        clientPhone,
      });

      successes.push({
        ...item,
        confirmationCode: created?.confirmationCode || "",
        requestReference: toNonEmptyString(created?.requestReference),
      });
    } catch (error) {
      failures.push({
        ...item,
        message: error?.message || "Erro ao criar agendamento.",
        status: Number(error?.status || 0) || null,
        requestReference: toNonEmptyString(error?.details?.requestReference || error?.requestReference),
      });
    }
  }

  const retryable = failures.filter(
    (item) => Number(item?.status || 0) === 429 || /too many requests/i.test(String(item?.message || "")),
  );
  if (retryable.length) {
    failures = failures.filter((item) => !retryable.includes(item));
    await new Promise((resolve) => setTimeout(resolve, Math.max(1200, BOOKING_SEQUENCE_GAP_MS)));

    for (let index = 0; index < retryable.length; index += 1) {
      const item = retryable[index];
      if (index > 0 && BOOKING_SEQUENCE_GAP_MS > 0) {
        await new Promise((resolve) => setTimeout(resolve, BOOKING_SEQUENCE_GAP_MS));
      }

      try {
        const created = await createAppointment({
          establishmentId,
          service: item.service,
          serviceResolvedName: item.serviceResolvedName,
          serviceId: item.serviceId,
          durationMinutes: item.durationMinutes,
          serviceAmount: item.serviceAmount,
          clientId: resolvedClientId,
          date: item.date,
          time: item.time,
          professionalId: item.professionalId,
          professionalName: item.professionalName,
          clientName,
          clientPhone,
        });
        successes.push({
          service: item.service,
          serviceResolvedName: item.serviceResolvedName,
          serviceId: item.serviceId,
          durationMinutes: item.durationMinutes,
          serviceAmount: item.serviceAmount,
          date: item.date,
          time: item.time,
          professionalId: item.professionalId,
          professionalName: item.professionalName,
          confirmationCode: created?.confirmationCode || "",
          requestReference: toNonEmptyString(created?.requestReference),
        });
      } catch (error) {
        failures.push({
          ...item,
          message: error?.message || "Erro ao criar agendamento.",
          status: Number(error?.status || 0) || null,
          requestReference: toNonEmptyString(error?.details?.requestReference || error?.requestReference),
        });
      }
    }
  }

  return {
    successes,
    failures,
  };
}

async function createAppointment({
  establishmentId,
  service,
  serviceResolvedName = "",
  serviceId = null,
  durationMinutes = null,
  serviceAmount = null,
  clientId = null,
  date,
  time,
  professionalId = null,
  professionalName,
  clientName,
  clientPhone,
}) {
  const normalizedClientPhone = normalizePhone(clientPhone);
  const normalizedClientName = toNonEmptyString(clientName);
  const normalizedRequestedProfessional = toNonEmptyString(professionalName);
  const requestReference = generateRequestReference();
  const resolvedServiceName = toNonEmptyString(serviceResolvedName) || toNonEmptyString(service);
  const resolvedServiceId = Number(serviceId);
  const resolvedDurationMinutes = Number(durationMinutes);
  const resolvedServiceAmount = Number(serviceAmount);
  const resolvedClientId = Number(clientId);
  const resolvedProfessionalId = Number(professionalId);
  const requestedTime = normalizeTimeValue(time);
  let payload = null;
  let resolvedProfessionalDisplay = "";

  try {
    if (!requestedTime) {
      const error = new Error(`Horario invalido: ${time}. Use o formato HH:mm.`);
      error.status = 400;
      throw error;
    }

    let selectedServiceId = Number.isFinite(resolvedServiceId) ? resolvedServiceId : null;
    let selectedDurationMinutes = Number.isFinite(resolvedDurationMinutes)
      ? resolvedDurationMinutes
      : 60;
    let selectedAmount = Number.isFinite(resolvedServiceAmount) ? resolvedServiceAmount : 0;

    if (!selectedServiceId) {
      const foundService = await findServiceByName(establishmentId, service);
      if (!foundService) {
        const error = new Error(`Servico nao encontrado para: ${service}`);
        error.status = 404;
        throw error;
      }

      const foundServiceId = Number(serviceIdFrom(foundService));
      if (!Number.isFinite(foundServiceId) || foundServiceId <= 0) {
        const error = new Error("Servico sem ID valido no retorno da API Trinks.");
        error.status = 422;
        throw error;
      }

      selectedServiceId = foundServiceId;
      const duration = Number(
        foundService?.duracaoEmMinutos || foundService?.duracao || foundService?.duracaoMinutos || 60,
      );
      const amount = Number(foundService?.valor || foundService?.preco || 0);
      selectedDurationMinutes = Number.isFinite(duration) ? duration : 60;
      selectedAmount = Number.isFinite(amount) ? amount : 0;
    }

    const professionals = await getProfessionals({
      establishmentId,
      date,
      serviceId: selectedServiceId,
    });

    if (!professionals.length) {
      const error = new Error("Nao ha profissionais com agenda aberta para este servico nesta data.");
      error.status = 422;
      throw error;
    }

    const normalizedProfessionals = professionals.map((item) => ({
      id: item.id,
      name: item.name,
      availableTimes: uniqueSortedTimes(
        item.availableTimes.filter((slot) =>
          isSlotCompatibleWithIntervals(slot, selectedDurationMinutes, item.availableIntervals),
        ),
      ),
      availableIntervals: item.availableIntervals,
    }));

    let professional = null;
    if (Number.isFinite(resolvedProfessionalId) && resolvedProfessionalId > 0) {
      professional =
        normalizedProfessionals.find((item) => Number(item.id) === resolvedProfessionalId) || null;
    }

    if (!professional && normalizedRequestedProfessional) {
      professional = findProfessionalByName(normalizedProfessionals, normalizedRequestedProfessional);
      if (!professional) {
        const error = new Error(`Profissional nao encontrada para: ${normalizedRequestedProfessional}`);
        error.status = 404;
        error.details = {
          requestedProfessional: normalizedRequestedProfessional,
          availableProfessionals: normalizedProfessionals.map((item) => item.name),
        };
        throw error;
      }

      if (!professional.availableTimes.includes(requestedTime)) {
        const nearestTimes = rankTimesByPreferredTime(professional.availableTimes, requestedTime).slice(0, 6);
        const displayName = professionalDisplayName(professional.name);
        const alternativesAtSameTime = uniqueProfessionalDisplayNames(
          normalizedProfessionals
            .filter((item) => normalizeForMatch(item.name) !== normalizeForMatch(professional.name))
            .filter((item) => item.availableTimes.includes(requestedTime))
            .map((item) => item.name),
        );
        const error = new Error(
          nearestTimes.length
            ? `${displayName} nao possui agenda livre as ${requestedTime}. Horarios de ${displayName} no dia ${isoToBrDate(date) || date}: ${nearestTimes.join(", ")}. Caso esses horarios nao sirvam para voce, quer saber a disponibilidade de outros profissionais?`
            : `${displayName} nao possui agenda livre na data informada para este servico. Caso esses horarios nao sirvam para voce, quer saber a disponibilidade de outros profissionais?`,
        );
        error.status = 409;
        error.details = {
          requestedProfessional: displayName,
          requestedTime,
          nearestTimes,
          alternativesAtSameTime,
        };
        throw error;
      }
    } else if (!professional) {
      professional =
        normalizedProfessionals.find((item) => item.availableTimes.includes(requestedTime)) || null;

      if (!professional) {
        const suggestions = buildGlobalSuggestedSlots(normalizedProfessionals, requestedTime, 8);
        const suggestionText = suggestions
          .map((item) => `${item.time} (${item.professionals.join(", ")})`)
          .join("; ");
        const error = new Error(
          suggestions.length
            ? `Nao encontrei agenda livre as ${requestedTime}. Opcoes proximas: ${suggestionText}.`
            : `Nao encontrei agenda livre na data informada para este servico.`,
        );
        error.status = 409;
        error.details = {
          requestedTime,
          suggestions,
        };
        throw error;
      }
    }

    resolvedProfessionalDisplay = professionalDisplayName(professional.name);
    let selectedClientId = Number.isFinite(resolvedClientId) && resolvedClientId > 0
      ? resolvedClientId
      : null;
    if (!selectedClientId) {
      const client = await findOrCreateClient(establishmentId, normalizedClientName, normalizedClientPhone);
      const foundClientId = Number(clientIdFrom(client));
      selectedClientId = Number.isFinite(foundClientId) && foundClientId > 0 ? foundClientId : null;
    }

    if (!selectedClientId) {
      const error = new Error("Cliente sem ID valido no retorno da API Trinks.");
      error.status = 422;
      throw error;
    }

    payload = {
      servicoId: Number(selectedServiceId),
      clienteId: Number(selectedClientId),
      profissionalId: Number(professional.id),
      dataHoraInicio: toIsoDateTime(date, requestedTime),
      duracaoEmMinutos: selectedDurationMinutes,
      valor: Number.isFinite(selectedAmount) ? selectedAmount : 0,
      observacoes: buildAppointmentObservation({ requestReference }),
      confirmado: true,
    };

    const created = await trinksRequest("/agendamentos", {
      method: "POST",
      estabelecimentoId: establishmentId,
      body: payload,
    });

    const appointmentId =
      created?.id || created?.agendamentoId || created?.data?.id || created?.item?.id || null;
    const confirmationCode = appointmentId ? `TRK-${appointmentId}` : "TRK-PENDENTE";
    const noteUpdate = await upsertAppointmentConfirmationNote({
      establishmentId,
      appointmentId,
      confirmationCode,
      requestReference,
    });

    const result = {
      status: "success",
      confirmationCode,
      requestReference,
      message: `Agendamento enviado ao Trinks com sucesso com ${resolvedProfessionalDisplay}.`,
      professional,
      raw: created,
      noteUpdate,
    };

    recordAppointmentAudit({
      eventType: "create",
      status: "success",
      establishmentId,
      appointmentId,
      confirmationCode,
      clientPhone: normalizedClientPhone,
      clientName: normalizedClientName,
      serviceName: resolvedServiceName || service,
      professionalName: resolvedProfessionalDisplay,
      date,
      time: requestedTime,
      requestPayload: {
        requestReference,
        payload,
      },
      responsePayload: result,
    });

    return result;
  } catch (error) {
    if (error && typeof error === "object") {
      const existingDetails =
        error?.details && typeof error.details === "object"
          ? error.details
          : { raw: error?.details ?? null };
      error.details = {
        ...existingDetails,
        requestReference,
      };
      error.requestReference = requestReference;
    }

    recordAppointmentAudit({
      eventType: "create",
      status: "error",
      establishmentId,
      confirmationCode: "",
      clientPhone: normalizedClientPhone,
      clientName: normalizedClientName,
      serviceName: resolvedServiceName || service,
      professionalName: resolvedProfessionalDisplay || normalizedRequestedProfessional,
      date,
      time: requestedTime || String(time || ""),
      requestPayload: {
        requestReference,
        payload,
      },
      responsePayload: {
        requestReference,
        details: error?.details || null,
      },
      errorMessage: error?.message || "Erro desconhecido no createAppointment",
    });
    throw error;
  }
}

async function resolveAppointmentContext({
  establishmentId,
  service,
  date,
  professionalName,
  clientName,
  clientPhone,
}) {
  const foundService = await findServiceByName(establishmentId, service);
  if (!foundService) {
    const error = new Error(`Servico nao encontrado para: ${service}`);
    error.status = 404;
    throw error;
  }

  const serviceId = serviceIdFrom(foundService);
  const client = await findOrCreateClient(establishmentId, clientName, clientPhone);
  const clientId = clientIdFrom(client);
  const professional = await findProfessionalForBooking({
    establishmentId,
    date,
    professionalName,
    serviceId,
  });

  const duration = Number(
    foundService?.duracaoEmMinutos || foundService?.duracao || foundService?.duracaoMinutos || 60,
  );
  const amount = Number(foundService?.valor || foundService?.preco || 0);

  return {
    service: foundService,
    serviceId: Number(serviceId),
    client,
    clientId: Number(clientId),
    professional,
    duration: Number.isFinite(duration) ? duration : 60,
    amount: Number.isFinite(amount) ? amount : 0,
  };
}

async function tryCreateAppointmentVariants({
  establishmentId,
  date,
  time,
  context,
}) {
  const baseDateTime = toIsoDateTime(date, time);
  const requestReference = generateRequestReference();
  const attempts = [
    {
      name: "current-shape",
      payload: {
        servicoId: context.serviceId,
        clienteId: context.clientId,
        profissionalId: Number(context.professional.id),
        dataHoraInicio: baseDateTime,
        duracaoEmMinutos: context.duration,
        valor: context.amount,
        observacoes: buildAppointmentObservation({ requestReference }),
        confirmado: true,
      },
    },
    {
      name: "without-confirmation-flags",
      payload: {
        servicoId: context.serviceId,
        clienteId: context.clientId,
        profissionalId: Number(context.professional.id),
        dataHoraInicio: baseDateTime,
        duracaoEmMinutos: context.duration,
      },
    },
    {
      name: "with-datahora",
      payload: {
        servicoId: context.serviceId,
        clienteId: context.clientId,
        profissionalId: Number(context.professional.id),
        dataHora: baseDateTime,
        duracaoEmMinutos: context.duration,
        valor: context.amount,
      },
    },
    {
      name: "servicos-array",
      payload: {
        clienteId: context.clientId,
        profissionalId: Number(context.professional.id),
        dataHoraInicio: baseDateTime,
        servicos: [
          {
            servicoId: context.serviceId,
            valor: context.amount,
            duracaoEmMinutos: context.duration,
          },
        ],
      },
    },
  ];

  const results = [];

  for (const attempt of attempts) {
    try {
      const response = await trinksRequest("/agendamentos", {
        method: "POST",
        estabelecimentoId: establishmentId,
        body: attempt.payload,
      });

      return {
        success: true,
        variant: attempt.name,
        payload: attempt.payload,
        response,
        attempts: results,
      };
    } catch (error) {
      results.push({
        variant: attempt.name,
        payload: attempt.payload,
        message: error.message,
        details: error.details || null,
        status: error.status || 500,
      });
    }
  }

  return {
    success: false,
    attempts: results,
  };
}

function parseAppointmentId({ confirmationCode, appointmentId }) {
  const parsed = parseAppointmentIdOrNull({ confirmationCode, appointmentId });
  if (parsed) {
    return parsed;
  }

  const error = new Error("Informe um codigo TRK valido ou ID numerico do agendamento.");
  error.status = 400;
  throw error;
}

function parseAppointmentIdOrNull({ confirmationCode, appointmentId }) {
  const directId = String(appointmentId || "").trim();
  if (/^\d+$/.test(directId)) {
    return Number(directId);
  }

  const fromCode = String(confirmationCode || "").trim().match(/(\d+)/);
  if (fromCode?.[1]) {
    return Number(fromCode[1]);
  }

  return null;
}

async function getAppointmentSnapshot(establishmentId, appointmentId) {
  const parsedId = Number(appointmentId);
  if (!Number.isFinite(parsedId) || parsedId <= 0) {
    const error = new Error("ID de agendamento invalido para consulta.");
    error.status = 400;
    throw error;
  }

  return trinksRequest(`/agendamentos/${parsedId}`, {
    method: "GET",
    estabelecimentoId: establishmentId,
  });
}

function buildAppointmentUpdatePayload(snapshot, overrides = {}) {
  const clienteId = Number(snapshot?.cliente?.id ?? snapshot?.clienteId ?? snapshot?.clienteEstabelecimentoId);
  const servicoId = Number(snapshot?.servico?.id ?? snapshot?.servicoId ?? snapshot?.servicoEstabelecimentoId);
  const profissionalId = Number(
    snapshot?.profissional?.id ?? snapshot?.profissionalId ?? snapshot?.profissionalEstabelecimentoId,
  );
  const dataHoraInicio = toNonEmptyString(snapshot?.dataHoraInicio || snapshot?.inicio);
  const duracaoEmMinutos = Number(snapshot?.duracaoEmMinutos ?? snapshot?.duracao ?? snapshot?.duracaoMinutos);
  const valor = Number(snapshot?.valor ?? snapshot?.preco ?? 0);
  const observacoes = firstNonEmpty([
    snapshot?.observacoesDoEstabelecimento,
    snapshot?.observacoes,
    snapshot?.observacao,
  ]);
  const confirmedStatusName = normalizeForMatch(firstNonEmpty([snapshot?.status?.nome, snapshot?.situacao]));
  const confirmado = confirmedStatusName.includes("confirm");

  if (!Number.isFinite(clienteId) || clienteId <= 0) {
    const error = new Error("Agendamento sem clienteId valido para atualizacao.");
    error.status = 422;
    throw error;
  }
  if (!Number.isFinite(servicoId) || servicoId <= 0) {
    const error = new Error("Agendamento sem servicoId valido para atualizacao.");
    error.status = 422;
    throw error;
  }
  if (!Number.isFinite(profissionalId) || profissionalId <= 0) {
    const error = new Error("Agendamento sem profissionalId valido para atualizacao.");
    error.status = 422;
    throw error;
  }
  if (!dataHoraInicio) {
    const error = new Error("Agendamento sem dataHoraInicio valida para atualizacao.");
    error.status = 422;
    throw error;
  }

  const basePayload = {
    clienteId,
    servicoId,
    profissionalId,
    dataHoraInicio,
    duracaoEmMinutos: Number.isFinite(duracaoEmMinutos) && duracaoEmMinutos > 0 ? duracaoEmMinutos : 60,
    valor: Number.isFinite(valor) ? valor : 0,
    observacoes,
    confirmado,
  };

  return {
    ...basePayload,
    ...overrides,
  };
}

function appointmentClientIdFrom(item) {
  return item?.clienteId ?? item?.cliente?.id ?? item?.clientId ?? item?.cliente?.clienteId ?? null;
}

function appointmentLooksCanceled(item) {
  if (!item || typeof item !== "object") {
    return false;
  }

  if (item?.cancelado === true || item?.cancelada === true || item?.isCanceled === true) {
    return true;
  }

  const statusText = normalizeForMatch(firstNonEmpty([item?.status, item?.situacao, item?.state, item?.estado]));
  return statusText.includes("cancel");
}

function appointmentMatchesServiceFilter(appointment, requestedService) {
  const target = normalizeForMatch(requestedService);
  if (!target) {
    return true;
  }

  const names = Array.isArray(appointment?.services) ? appointment.services : [];
  if (!names.length) {
    return false;
  }

  return names.some((name) => {
    const candidate = normalizeForMatch(name);
    return candidate.includes(target) || target.includes(candidate);
  });
}

function appointmentMatchesProfessionalFilter(appointment, requestedProfessional) {
  const target = normalizeForMatch(requestedProfessional);
  if (!target) {
    return true;
  }

  const candidate = normalizeForMatch(appointment?.professional);
  if (!candidate) {
    return false;
  }

  return candidate.includes(target) || target.includes(candidate);
}

function appointmentSortDateTime(appointment) {
  const date = toNonEmptyString(appointment?.date);
  const time = normalizeTimeValue(appointment?.time) || "00:00";
  if (!date) {
    return "9999-12-31T23:59";
  }
  return `${date}T${time}`;
}

function buildCancellationOption(appointment) {
  return {
    appointmentId: appointment?.id || null,
    confirmationCode: appointment?.id ? `TRK-${appointment.id}` : null,
    date: appointment?.date || null,
    time: appointment?.time || null,
    professional: professionalDisplayName(appointment?.professional || ""),
    services: appointment?.services || [],
    client: appointment?.client || null,
  };
}

async function listAppointmentsByClientId(establishmentId, clientId, { dateFrom, dateTo } = {}) {
  if (!clientId) {
    return [];
  }

  const attempts = [
    {
      label: "clienteId-dateRange",
      query: {
        clienteId: clientId,
        dataInicial: `${dateFrom}T00:00:00`,
        dataFinal: `${dateTo}T23:59:59`,
      },
    },
    {
      label: "clientId-dateRange",
      query: {
        clientId: clientId,
        dataInicial: `${dateFrom}T00:00:00`,
        dataFinal: `${dateTo}T23:59:59`,
      },
    },
    {
      label: "idCliente-dateRange",
      query: {
        idCliente: clientId,
        dataInicial: dateFrom,
        dataFinal: dateTo,
      },
    },
    {
      label: "clienteId-noRange",
      query: {
        clienteId: clientId,
      },
    },
  ];

  const allItems = [];
  const seenIds = new Set();
  const errors = [];

  for (const attempt of attempts) {
    try {
      for (let page = 1; page <= 8; page += 1) {
        const payload = await trinksRequest("/agendamentos", {
          method: "GET",
          estabelecimentoId: establishmentId,
          query: {
            ...attempt.query,
            page,
            pageSize: 200,
          },
        });
        const items = extractItems(payload);
        if (!items.length) {
          break;
        }

        for (const item of items) {
          const id = item?.id ?? item?.agendamentoId ?? null;
          const key = id ? `id:${id}` : JSON.stringify(item);
          if (seenIds.has(key)) {
            continue;
          }
          seenIds.add(key);
          allItems.push(item);
        }

        if (items.length < 200) {
          break;
        }
      }
    } catch (error) {
      errors.push({
        label: attempt.label,
        message: error?.message || "Erro ao listar agendamentos da cliente.",
        status: error?.status || null,
      });
    }
  }

  if (!allItems.length && errors.length) {
    const aggregate = new Error("Nao foi possivel listar agendamentos da cliente na Trinks.");
    aggregate.status = errors[0]?.status || 500;
    aggregate.details = errors;
    throw aggregate;
  }

  return allItems;
}

async function resolveAppointmentForCancellationWithoutCode({
  establishmentId,
  clientPhone,
  clientName,
  date,
  time,
  service,
  professionalName,
}) {
  const normalizedPhone = normalizePhone(clientPhone);
  const normalizedClientName = toNonEmptyString(clientName);
  if (!normalizedPhone) {
    const error = new Error("Sem codigo TRK, preciso do telefone da cliente para localizar o agendamento.");
    error.status = 400;
    throw error;
  }

  const knownClient = await findExistingClientByPhone(establishmentId, normalizedPhone);
  if (!knownClient) {
    const error = new Error("Nao encontrei essa cliente na base Trinks. Envie o codigo TRK para desmarcar.");
    error.status = 404;
    throw error;
  }

  const knownClientId = Number(clientIdFrom(knownClient));
  const knownClientLabel = clientDisplayNameFrom(knownClient) || normalizedClientName || "Cliente";
  const today = getSaoPauloDateContext().isoToday;
  const dateFrom = today;
  const dateTo = addDaysToIsoDate(today, 90);
  const requestedDate = toNonEmptyString(date);
  const requestedTime = normalizeTimeValue(time);
  const requestedService = toNonEmptyString(service);
  const requestedProfessional = toNonEmptyString(professionalName);

  const items = await listAppointmentsByClientId(establishmentId, knownClientId, { dateFrom, dateTo });
  const normalized = items
    .map(normalizeAppointmentItem)
    .filter(Boolean)
    .filter((appointment) => Number.isFinite(Number(appointment?.id)) && Number(appointment.id) > 0)
    .filter((appointment) => {
      const candidateClientId = Number(appointmentClientIdFrom(appointment.raw));
      if (Number.isFinite(candidateClientId) && candidateClientId > 0) {
        return candidateClientId === knownClientId;
      }

      const candidateClientName = normalizeForMatch(appointment.client || "");
      const expectedClientName = normalizeForMatch(knownClientLabel);
      return Boolean(candidateClientName && expectedClientName && candidateClientName === expectedClientName);
    })
    .filter((appointment) => !appointmentLooksCanceled(appointment.raw))
    .filter((appointment) => toNonEmptyString(appointment.date) >= today)
    .sort((left, right) => appointmentSortDateTime(left).localeCompare(appointmentSortDateTime(right)));

  if (!normalized.length) {
    const error = new Error(`Nao encontrei agendamentos futuros para ${knownClientLabel}.`);
    error.status = 404;
    throw error;
  }

  const filtered = normalized.filter((appointment) => {
    if (requestedDate && appointment.date !== requestedDate) {
      return false;
    }
    if (requestedTime && normalizeTimeValue(appointment.time) !== requestedTime) {
      return false;
    }
    if (!appointmentMatchesServiceFilter(appointment, requestedService)) {
      return false;
    }
    if (!appointmentMatchesProfessionalFilter(appointment, requestedProfessional)) {
      return false;
    }
    return true;
  });

  const candidatePool = filtered.length ? filtered : normalized;

  if (candidatePool.length === 1) {
    return {
      appointmentId: Number(candidatePool[0].id),
      resolution: "clientPhoneAuto",
      client: {
        id: knownClientId,
        name: knownClientLabel,
        phone: normalizedPhone,
      },
      matchedFilters: {
        date: requestedDate || null,
        time: requestedTime || null,
        service: requestedService || null,
        professionalName: requestedProfessional || null,
      },
      matchedAppointment: buildCancellationOption(candidatePool[0]),
    };
  }

  const error = new Error(
    "Encontrei mais de um agendamento para esta cliente. Informe o codigo TRK ou confirme data/horario para desmarcar com seguranca.",
  );
  error.status = 409;
  error.details = {
    totalFound: candidatePool.length,
    options: candidatePool.slice(0, 5).map(buildCancellationOption),
  };
  throw error;
}

async function cancelAppointmentById({ establishmentId, appointmentId, reason, requestPayload }) {
  const normalizedReason = toNonEmptyString(reason);
  const cancellationNote = normalizedReason
    ? `Cancelado via IA.AGENDAMENTO | Motivo: ${normalizedReason}`
    : "Cancelado via IA.AGENDAMENTO";

  const parsedId = Number(appointmentId);
  const snapshot = await getAppointmentSnapshot(establishmentId, parsedId);
  const payloadCancelByStatus = buildAppointmentUpdatePayload(snapshot, {
    statusId: TRINKS_STATUS_ID_CANCELLED,
    status: { id: TRINKS_STATUS_ID_CANCELLED },
    confirmado: false,
    observacoes: cancellationNote,
  });
  const payloadCancelByFlag = {
    ...payloadCancelByStatus,
    cancelado: true,
  };

  const attempts = [
    {
      method: "PUT",
      path: `/agendamentos/${parsedId}`,
      body: payloadCancelByStatus,
      mode: "PUT_STATUS_CANCELLED_FULL",
    },
    {
      method: "PATCH",
      path: `/agendamentos/${parsedId}`,
      body: payloadCancelByStatus,
      mode: "PATCH_STATUS_CANCELLED_FULL",
    },
    {
      method: "PUT",
      path: `/agendamentos/${parsedId}`,
      body: payloadCancelByFlag,
      mode: "PUT_CANCEL_FLAG_FULL",
    },
    {
      method: "PATCH",
      path: `/agendamentos/${parsedId}`,
      body: payloadCancelByFlag,
      mode: "PATCH_CANCEL_FLAG_FULL",
    },
    {
      method: "DELETE",
      path: `/agendamentos/${parsedId}`,
      body: undefined,
      mode: "DELETE",
    },
    {
      method: "PATCH",
      path: `/agendamentos/${parsedId}/cancelar`,
      body: { observacoes: cancellationNote, confirmado: false },
      mode: "PATCH_CANCELAR",
    },
    {
      method: "PATCH",
      path: `/agendamentos/${parsedId}`,
      body: { cancelado: true, observacoes: cancellationNote, confirmado: false },
      mode: "PATCH_CANCELADO",
    },
    {
      method: "PUT",
      path: `/agendamentos/${parsedId}`,
      body: { cancelado: true, observacoes: cancellationNote, confirmado: false },
      mode: "PUT_CANCELADO",
    },
  ];

  const attemptErrors = [];
  let lastError = null;

  for (const attempt of attempts) {
    try {
      const response = await trinksRequest(attempt.path, {
        method: attempt.method,
        estabelecimentoId: establishmentId,
        body: attempt.body,
      });

      const result = {
        status: "success",
        appointmentId: parsedId,
        confirmationCode: `TRK-${parsedId}`,
        message: "Agendamento desmarcado com sucesso.",
        method: attempt.mode,
        raw: response,
      };

      recordAppointmentAudit({
        eventType: "cancel",
        status: "success",
        establishmentId,
        appointmentId: parsedId,
        confirmationCode: `TRK-${parsedId}`,
        requestPayload: {
          ...requestPayload,
          reason: normalizedReason,
          attemptsTried: attempts.map((item) => item.mode),
          successMode: attempt.mode,
          requestBody: attempt.body || null,
        },
        responsePayload: result,
      });

      return result;
    } catch (error) {
      lastError = error;
      attemptErrors.push({
        mode: attempt.mode,
        method: attempt.method,
        path: attempt.path,
        requestBody: attempt.body || null,
        status: error?.status || null,
        message: error?.message || "Erro desconhecido",
        details: error?.details || null,
      });
    }
  }

  recordAppointmentAudit({
    eventType: "cancel",
    status: "error",
    establishmentId,
    appointmentId: parsedId,
    confirmationCode: `TRK-${parsedId}`,
    requestPayload: {
      ...requestPayload,
      reason: normalizedReason,
      payloadCancelByStatus,
      attemptsTried: attempts.map((item) => item.mode),
    },
    responsePayload: attemptErrors,
    errorMessage: lastError?.message || "Erro ao cancelar agendamento",
  });

  throw lastError || new Error("Nao foi possivel cancelar o agendamento.");
}

async function rescheduleAppointment({ establishmentId, confirmationCode, appointmentId, date, time }) {
  const parsedId = parseAppointmentId({ confirmationCode, appointmentId });
  const requestedTime = normalizeTimeValue(time) || String(time || "");
  const dataHoraInicio = toIsoDateTime(date, requestedTime);
  const snapshot = await getAppointmentSnapshot(establishmentId, parsedId);
  const payload = buildAppointmentUpdatePayload(snapshot, {
    dataHoraInicio,
    confirmado: true,
  });

  const attempts = [
    { method: "PUT", path: `/agendamentos/${parsedId}`, mode: "PUT_FULL" },
    { method: "PATCH", path: `/agendamentos/${parsedId}`, mode: "PATCH_FULL" },
  ];

  const errors = [];
  for (const attempt of attempts) {
    try {
      const updated = await trinksRequest(attempt.path, {
        method: attempt.method,
        estabelecimentoId: establishmentId,
        body: payload,
      });

      const result = {
        status: "success",
        confirmationCode: `TRK-${parsedId}`,
        message: `Horario alterado para ${date} as ${requestedTime}.`,
        method: attempt.mode,
        raw: updated,
      };

      recordAppointmentAudit({
        eventType: "reschedule",
        status: "success",
        establishmentId,
        appointmentId: parsedId,
        confirmationCode: `TRK-${parsedId}`,
        date,
        time: requestedTime,
        requestPayload: {
          mode: attempt.mode,
          payload,
        },
        responsePayload: result,
      });

      return result;
    } catch (error) {
      errors.push({
        mode: attempt.mode,
        method: attempt.method,
        message: error?.message || "Erro ao reagendar.",
        details: error?.details || null,
        status: error?.status || null,
      });
    }
  }

  const last = errors[errors.length - 1];
  recordAppointmentAudit({
    eventType: "reschedule",
    status: "error",
    establishmentId,
    appointmentId: parsedId,
    confirmationCode: `TRK-${parsedId}`,
    date,
    time: requestedTime,
    requestPayload: payload,
    responsePayload: errors,
    errorMessage: last?.message || "Erro ao reagendar",
  });

  const failure = new Error(last?.message || "Nao foi possivel alterar o agendamento.");
  failure.status = last?.status || 500;
  failure.details = errors;
  throw failure;
}

async function cancelAppointment({
  establishmentId,
  confirmationCode,
  appointmentId,
  reason,
  clientPhone,
  clientName,
  date,
  time,
  service,
  professionalName,
}) {
  const parsedId = parseAppointmentIdOrNull({ confirmationCode, appointmentId });

  if (parsedId) {
    return cancelAppointmentById({
      establishmentId,
      appointmentId: parsedId,
      reason,
      requestPayload: {
        resolution: "confirmationCodeOrId",
        confirmationCode: toNonEmptyString(confirmationCode),
      },
    });
  }

  const resolved = await resolveAppointmentForCancellationWithoutCode({
    establishmentId,
    clientPhone,
    clientName,
    date,
    time,
    service,
    professionalName,
  });

  return cancelAppointmentById({
    establishmentId,
    appointmentId: resolved.appointmentId,
    reason,
    requestPayload: {
      resolution: resolved.resolution,
      matchedFilters: resolved.matchedFilters,
      client: resolved.client,
      matchedAppointment: resolved.matchedAppointment,
    },
  });
}

async function sendChatMessage({ establishmentId, message, history, customerContext }) {
  const knowledge = loadSalonKnowledge();
  const dateContext = getSaoPauloDateContext();
  const relativeDate = detectRelativeDateReference(message, dateContext);
  const knownClientName = clientFirstName(customerContext?.name);
  const normalizedMessageForGate = normalizeForMatch(message);
  const inferredPreferredTime = extractPreferredTimeFromMessage(message);
  const hasBookingTimeIntent = messageSuggestsBookingTimeIntent(message, dateContext);
  const hasSchedulingIntent = messageSuggestsSchedulingIntent(message);
  const hasCancellationIntent = messageSuggestsCancellationIntent(message);
  const hasRescheduleIntent = messageSuggestsRescheduleIntent(message);
  const hasChangeRequestIntent = hasCancellationIntent || hasRescheduleIntent;
  const asksProfessionals = /(profission|quem atende|cabeleireir)/.test(normalizedMessageForGate);
  const hasProfessionalHintInMessage = messageContainsProfessionalPreferenceHint(message);
  const hasProfessionalHintInHistory = historyHasProfessionalContext(history);
  const pendingSessionKey = resolvePendingSessionKey(establishmentId, customerContext);
  const pendingConfirmation = getPendingBookingConfirmation(pendingSessionKey);

  if (pendingConfirmation) {
    const confirmationIntent = detectConfirmationIntent(message);

    if (confirmationIntent === "confirm") {
      const execution = await executeConfirmedBookings({
        establishmentId,
        clientName: pendingConfirmation.clientName,
        clientPhone: pendingConfirmation.clientPhone,
        items: pendingConfirmation.items,
      });
      clearPendingBookingConfirmation(pendingSessionKey);

      const successLines = execution.successes.map((item) => {
        const codeSuffix = item.confirmationCode ? ` | codigo ${item.confirmationCode}` : "";
        const reqSuffix = item.requestReference ? ` | req ${item.requestReference}` : "";
        return `- ${formatBookingItemSummary(item)}${codeSuffix}${reqSuffix}`;
      });
      const failureLines = execution.failures.map((item) => {
        const reqSuffix = item.requestReference ? ` | req ${item.requestReference}` : "";
        return `- ${formatBookingItemSummary(item)}${reqSuffix} | erro: ${item.message}`;
      });

      if (execution.successes.length && !execution.failures.length) {
        return `Perfeito, agendamento confirmado com sucesso:\n${successLines.join("\n")}`;
      }

      if (execution.successes.length && execution.failures.length) {
        return [
          "Consegui confirmar parte dos agendamentos.",
          "",
          "Confirmados:",
          successLines.join("\n"),
          "",
          "Nao confirmados:",
          failureLines.join("\n"),
        ].join("\n");
      }

      return `Nao consegui confirmar os agendamentos solicitados:\n${failureLines.join("\n")}`;
    }

    if (confirmationIntent === "deny") {
      clearPendingBookingConfirmation(pendingSessionKey);
      return "Perfeito, nao vou confirmar ainda. Me diga o que deseja ajustar (servico, profissional, data ou horario).";
    }

    const hasAdjustmentIntent =
      hasSchedulingIntent ||
      hasBookingTimeIntent ||
      Boolean(inferredPreferredTime) ||
      /\b(ajust|troc|mudar|alterar|outro|dia|horario|hora|profissional|servico)\b/.test(normalizedMessageForGate);

    if (hasAdjustmentIntent) {
      clearPendingBookingConfirmation(pendingSessionKey);
    } else {
      return `${buildBookingConfirmationMessage(pendingConfirmation.items)}\n\nSe quiser ajustar algo, me diga o que devo alterar.`;
    }

  }

  const shouldAskPreferenceFirst =
    hasBookingTimeIntent &&
    !hasProfessionalHintInMessage &&
    !hasProfessionalHintInHistory &&
    !asksProfessionals &&
    !historyAlreadyAskedProfessionalPreference(history);

  if (shouldAskPreferenceFirst) {
    const salutation = knownClientName ? `Perfeito, ${knownClientName}. ` : "Perfeito. ";
    return `${salutation}Antes de eu sugerir os horarios, voce tem preferencia por alguma profissional?`;
  }

  if (shouldLookupProfessionalsDirectly(message)) {
    const targetDate = relativeDate?.iso || dateContext.isoToday;
    let serviceId;
    try {
      const maybeService = await findServiceByName(establishmentId, message);
      serviceId = Number(serviceIdFrom(maybeService)) || undefined;
    } catch {
      serviceId = undefined;
    }

    if (!serviceId) {
      const hintedService = inferServiceHintFromMessage(message);
      if (hintedService) {
        try {
          const hinted = await findServiceByName(establishmentId, hintedService);
          serviceId = Number(serviceIdFrom(hinted)) || undefined;
        } catch {
          serviceId = undefined;
        }
      }
    }

    const professionals = await getProfessionals({
      establishmentId,
      date: targetDate,
      serviceId,
    });

    const names = uniqueProfessionalDisplayNames(
      professionals
        .filter(professionalHasOpenSchedule)
        .map((item) => toNonEmptyString(item?.name || professionalNameFrom(item))),
    );

    if (!names.length) {
      return `No momento, nao encontrei profissionais disponiveis para ${isoToBrDate(targetDate)}. Posso consultar outra data para voce.`;
    }

    return `Para ${isoToBrDate(targetDate)}, as profissionais disponiveis sao: ${names.join(", ")}.`;
  }

  const faqAnswer = findBestFaqAnswer(knowledge, message);
  if (faqAnswer && !hasSchedulingIntent) {
    return faqAnswer;
  }

  const ai = new GoogleGenAI({ apiKey: ensureEnv("GEMINI_API_KEY") });
  const chat = ai.chats.create({
    model: "gemini-3-flash-preview",
    config: {
      systemInstruction: SYSTEM_INSTRUCTION,
      temperature: 0.4,
      tools: [{ functionDeclarations: chatTools }],
    },
  });

  let response = await chat.sendMessage({
    message: buildConversationPrompt(history, message, knowledge, customerContext || null),
  });

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const calls = response.functionCalls || [];
    if (!calls.length) {
      const text = String(response.text || "");
      if (
        relativeDate &&
        /(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?|\d{4}-\d{2}-\d{2})/.test(text) &&
        !text.includes(relativeDate.br) &&
        !text.includes(relativeDate.iso)
      ) {
        const corrected = await chat.sendMessage({
          message: `Correcao obrigatoria: para esta conversa, "${relativeDate.label}" = ${relativeDate.iso} (${relativeDate.br}). Reescreva a ultima resposta com a data correta, sem alterar o restante do sentido.`,
        });
        return String(corrected.text || text);
      }
      return text;
    }

    const results = [];
    for (const call of calls) {
      try {
        if (call.name === "checkAvailability") {
          const requestedDate = relativeDate?.iso || toNonEmptyString(call.args?.date);
          const requestedProfessional = toNonEmptyString(call.args?.professionalName);
          const requestedPreferredTime =
            normalizeTimeValue(call.args?.preferredTime) || inferredPreferredTime;
          const availability = await getAvailability(
            establishmentId,
            String(call.args.service),
            requestedDate,
            {
              professionalName: requestedProfessional,
              preferredTime: requestedPreferredTime,
            },
          );
          results.push({ name: call.name, result: availability });
          continue;
        }

        if (call.name === "listAppointmentsForDate") {
          const requestedDate =
            relativeDate?.iso || toNonEmptyString(call.args?.date) || dateContext.isoToday;
          const { items, source } = await getAppointmentsForDate(
            establishmentId,
            requestedDate,
          );
          const normalized = items.map(normalizeAppointmentItem).filter(Boolean);
          results.push({ name: call.name, result: { source, appointments: normalized } });
          continue;
        }

        if (call.name === "listProfessionalsForDate") {
          const requestedDate =
            relativeDate?.iso || toNonEmptyString(call.args?.date) || dateContext.isoToday;
          const requestedService = toNonEmptyString(call.args?.service);

          let serviceId;
          if (requestedService) {
            try {
              const service = await findServiceByName(establishmentId, requestedService);
              serviceId = Number(serviceIdFrom(service)) || undefined;
            } catch {
              serviceId = undefined;
            }
          }

          const professionals = await getProfessionals({
            establishmentId,
            date: requestedDate,
            serviceId,
          });

          const names = uniqueProfessionalDisplayNames(
            professionals
              .filter(professionalHasOpenSchedule)
              .map((item) => toNonEmptyString(item?.name || professionalNameFrom(item))),
          );

          results.push({
            name: call.name,
            result: {
              date: requestedDate,
              service: requestedService || null,
              total: names.length,
              professionals: names,
            },
          });
          continue;
        }

        if (call.name === "bookAppointment") {
          if (hasChangeRequestIntent) {
            results.push({
              name: call.name,
              result: {
                status: "blocked_by_change_intent",
                message:
                  "Detectei pedido de cancelamento/alteracao. Nao vou criar novo agendamento ate concluir a alteracao ou cancelamento solicitado.",
                requiredAction:
                  "Use rescheduleAppointment para alterar horario e/ou cancelAppointment para desmarcar.",
              },
            });
            continue;
          }

          const resolvedClientName =
            toNonEmptyString(call.args?.clientName) ||
            toNonEmptyString(customerContext?.name);
          const resolvedClientPhone =
            normalizePhone(call.args?.clientPhone) ||
            normalizePhone(customerContext?.phone);

          if (!resolvedClientName || !resolvedClientPhone) {
            results.push({
              name: call.name,
              result: {
                status: "error",
                message:
                  "Nao foi possivel confirmar os dados da cliente para agendamento (nome e telefone).",
                missing: {
                  clientName: !resolvedClientName,
                  clientPhone: !resolvedClientPhone,
                },
              },
            });
            continue;
          }

          const requestedDateFallback = relativeDate?.iso || toNonEmptyString(call.args?.date);
          const rawItems =
            Array.isArray(call.args?.appointments) && call.args.appointments.length
              ? call.args.appointments
              : [call.args];

          const normalizedItems = rawItems.map((item) => ({
            service: toNonEmptyString(item?.service || call.args?.service),
            date: normalizeBookingDate(item?.date || call.args?.date, requestedDateFallback),
            time: normalizeBookingTime(item?.time || call.args?.time),
            professionalName: toNonEmptyString(item?.professionalName || call.args?.professionalName),
          }));

          const invalidItem = normalizedItems.find(
            (item) => !item.service || !item.date || !item.time,
          );
          if (invalidItem) {
            results.push({
              name: call.name,
              result: {
                status: "error",
                message:
                  "Para agendar, preciso de servico, data e horario em cada item solicitado.",
                invalidItem,
              },
            });
            continue;
          }

          const previewItems = [];
          let previewError = null;
          for (const item of normalizedItems) {
            try {
              const preview = await resolveBookingPreviewItem({
                establishmentId,
                service: item.service,
                date: item.date,
                time: item.time,
                professionalName: item.professionalName,
              });
              previewItems.push(preview);
            } catch (error) {
              previewError = error;
              break;
            }
          }

          if (previewError) {
            results.push({
              name: call.name,
              result: {
                status: "error",
                message: previewError?.message || "Nao foi possivel validar disponibilidade.",
                details: previewError?.details || null,
              },
            });
            continue;
          }

          const sessionKey = resolvePendingSessionKey(
            establishmentId,
            customerContext,
            { clientPhone: resolvedClientPhone, clientName: resolvedClientName },
          );

          if (!sessionKey) {
            results.push({
              name: call.name,
              result: {
                status: "error",
                message: "Nao consegui identificar a sessao da cliente para confirmar o agendamento.",
              },
            });
            continue;
          }

          const pending = setPendingBookingConfirmation(sessionKey, {
            establishmentId,
            clientName: resolvedClientName,
            clientPhone: resolvedClientPhone,
            items: previewItems,
          });

          results.push({
            name: call.name,
            result: {
              status: "pending_confirmation",
              clientName: resolvedClientName,
              clientPhone: resolvedClientPhone,
              total: previewItems.length,
              items: previewItems,
              expiresAt: new Date(pending.expiresAt).toISOString(),
              message: buildBookingConfirmationMessage(previewItems),
            },
          });
          continue;
        }

        if (call.name === "rescheduleAppointment") {
          const requestedDate = relativeDate?.iso || toNonEmptyString(call.args?.date);
          const rescheduled = await rescheduleAppointment({
            establishmentId,
            confirmationCode: String(call.args.confirmationCode || ""),
            appointmentId: String(call.args.appointmentId || ""),
            date: requestedDate,
            time: String(call.args.time),
          });
          results.push({ name: call.name, result: rescheduled });
          continue;
        }

        if (call.name === "cancelAppointment") {
          const cancelled = await cancelAppointment({
            establishmentId,
            confirmationCode: String(call.args?.confirmationCode || ""),
            appointmentId: String(call.args?.appointmentId || ""),
            reason: String(call.args?.reason || ""),
            clientPhone:
              normalizePhone(call.args?.clientPhone) ||
              normalizePhone(customerContext?.phone),
            clientName:
              toNonEmptyString(call.args?.clientName) ||
              toNonEmptyString(customerContext?.name),
            date: relativeDate?.iso || toNonEmptyString(call.args?.date),
            time: String(call.args?.time || ""),
            service: String(call.args?.service || ""),
            professionalName: String(call.args?.professionalName || ""),
          });
          results.push({ name: call.name, result: cancelled });
          continue;
        }

        results.push({
          name: call.name,
          result: { status: "error", message: `Ferramenta nao suportada: ${call.name}` },
        });
      } catch (error) {
        results.push({
          name: call.name,
          result: {
            status: "error",
            message: error?.message || "Erro ao executar ferramenta.",
            details: error?.details || null,
          },
        });
      }
    }

    response = await chat.sendMessage({
      message: JSON.stringify(results),
    });
  }

  return "Tive uma instabilidade momentanea aqui. Consegue repetir sua ultima mensagem para eu continuar seu atendimento?";
}

app.get("/api/health", (req, res) => {
  res.json({ status: "ok" });
});

app.get("/api/knowledge", (req, res) => {
  try {
    const knowledge = loadSalonKnowledge();
    return res.json({ knowledge });
  } catch (error) {
    return res.status(500).json({
      message: error.message || "Erro ao carregar base de conhecimento.",
    });
  }
});

app.put("/api/knowledge", (req, res) => {
  try {
    if (!isKnowledgeWriteAuthorized(req)) {
      return res.status(401).json({
        message: "Nao autorizado para atualizar a base de conhecimento.",
      });
    }

    const knowledge = req.body?.knowledge;
    if (!knowledge || typeof knowledge !== "object" || Array.isArray(knowledge)) {
      return res.status(400).json({
        message: "Campo obrigatorio: knowledge (objeto JSON).",
      });
    }

    const saved = saveSalonKnowledge(knowledge);
    return res.json({ status: "ok", knowledge: saved });
  } catch (error) {
    return res.status(500).json({
      message: error.message || "Erro ao salvar base de conhecimento.",
    });
  }
});

app.get("/", (req, res) => {
  res.json({
    name: "IA.AGENDAMENTO Backend",
    status: "online",
    docs: {
      health: "/api/health",
      chat: "POST /api/chat",
      trinksAvailability: "POST /api/trinks/availability",
      trinksAppointments: "POST /api/trinks/appointments",
      trinksAppointmentsDay: "POST /api/trinks/appointments/day",
      trinksProfessionals: "POST /api/trinks/professionals",
      trinksReschedule: "POST /api/trinks/appointments/reschedule",
      trinksCancel: "POST /api/trinks/appointments/cancel",
      evolutionSendText: "POST /api/evolution/send-text",
      evolutionQrPage: "GET /api/evolution/instance/connect?instance=SEU_NOME",
      handoffStatus: "GET /api/handoff/status?phone=5511999999999",
      handoffActivate: "POST /api/handoff/activate",
      handoffResume: "POST /api/handoff/resume",
      dbConversations: "GET /api/db/conversations?limit=50",
      dbMessages: "GET /api/db/messages?phone=5511999999999&limit=100",
      dbAppointmentAudit: "GET /api/db/appointments-audit?limit=100",
    },
  });
});

app.post("/api/evolution/instance/create", async (req, res) => {
  try {
    const instance = resolveEvolutionInstance(req.body?.instance || req.body?.instanceName);
    if (!instance) {
      return res.status(400).json({ message: "Informe instance/instanceName ou configure EVOLUTION_INSTANCE." });
    }

    const created = await createEvolutionInstance(instance);
    return res.json({ status: "ok", instance, created });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao criar instancia na Evolution.",
      details: error.details || null,
    });
  }
});

app.get("/api/evolution/instance/status", async (req, res) => {
  try {
    const instance = resolveEvolutionInstance(req.query.instance || req.query.instanceName);
    if (!instance) {
      return res.status(400).json({ message: "Informe instance ou configure EVOLUTION_INSTANCE." });
    }

    const payload = await evolutionRequest("/instance/fetchInstances", { method: "GET" });
    const instances = Array.isArray(payload) ? payload : Array.isArray(payload?.instances) ? payload.instances : [];
    const found = instances.find(
      (item) =>
        toNonEmptyString(item?.name || item?.instanceName || item?.instance).toLowerCase() === instance.toLowerCase(),
    );

    return res.json({ status: "ok", instance, connected: Boolean(found), data: found || null, raw: payload });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao consultar status da instancia.",
      details: error.details || null,
    });
  }
});

app.get("/api/evolution/instance/qr", async (req, res) => {
  try {
    const instance = resolveEvolutionInstance(req.query.instance || req.query.instanceName);
    if (!instance) {
      return res.status(400).json({ message: "Informe instance ou configure EVOLUTION_INSTANCE." });
    }

    const qr = await fetchEvolutionQr(instance);
    return res.json({
      status: "ok",
      instance,
      qr: {
        hasQrImage: Boolean(qr.qrDataUrl),
        qrDataUrl: qr.qrDataUrl || null,
        qrRaw: qr.qrRaw || null,
        pairingCode: qr.pairingCode || null,
        sourcePath: qr.attempt?.path || null,
      },
      raw: qr.payload,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao buscar QR code da instancia.",
      details: error.details || null,
    });
  }
});

app.get("/api/evolution/instance/connect", async (req, res) => {
  try {
    const instance = resolveEvolutionInstance(req.query.instance || req.query.instanceName);
    if (!instance) {
      return res.status(400).send("Informe ?instance=nome-da-instancia ou configure EVOLUTION_INSTANCE.");
    }

    const created = await createEvolutionInstance(instance);
    const qr = await fetchEvolutionQr(instance);

    const statusMessage = created?.alreadyExists
      ? "Instancia ja existia. Escaneie o QR para conectar."
      : "Instancia criada. Escaneie o QR no WhatsApp Business.";

    const details = qr.qrDataUrl
      ? ""
      : JSON.stringify({ create: created, qrSource: qr.attempt?.path, qrRaw: qr.qrRaw, raw: qr.payload }, null, 2);

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    return res.send(
      renderQrPage({
        instance,
        qrDataUrl: qr.qrDataUrl,
        pairingCode: qr.pairingCode,
        statusMessage,
        details,
      }),
    );
  } catch (error) {
    const html = renderQrPage({
      instance: resolveEvolutionInstance(req.query.instance || req.query.instanceName),
      qrDataUrl: "",
      pairingCode: "",
      statusMessage: "Falha ao criar/conectar instancia na Evolution.",
      details: JSON.stringify(
        {
          message: error.message || "Erro desconhecido",
          details: error.details || null,
        },
        null,
        2,
      ),
    });
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    return res.status(error.status || 500).send(html);
  }
});

app.post("/api/trinks/availability", async (req, res) => {
  try {
    const { establishmentId, service, date, professionalName, preferredTime } = req.body || {};
    if (!establishmentId || !service || !date) {
      return res.status(400).json({ message: "Campos obrigatorios: establishmentId, service, date" });
    }

    const availability = await getAvailability(establishmentId, service, date, {
      professionalName: professionalName ? String(professionalName) : "",
      preferredTime: preferredTime ? String(preferredTime) : "",
    });

    return res.json(availability);
  } catch (error) {
    return res.status(error.status || 500).json({
      message: error.message || "Erro ao consultar disponibilidade.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/appointments", async (req, res) => {
  try {
    const {
      establishmentId,
      service,
      date,
      time,
      professionalName,
      clientName,
      clientPhone,
    } = req.body || {};

    if (!establishmentId || !service || !date || !time || !clientName || !clientPhone) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, service, date, time, clientName, clientPhone",
      });
    }

    const createdAppointment = await createAppointment({
      establishmentId,
      service,
      date,
      time,
      professionalName: professionalName ? String(professionalName) : "",
      clientName,
      clientPhone,
    });

    return res.status(201).json(createdAppointment);
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao criar agendamento.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/appointments/day", async (req, res) => {
  try {
    const { establishmentId, date } = req.body || {};

    if (!establishmentId || !date) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, date",
      });
    }

    const response = await getAppointmentsForDate(Number(establishmentId), String(date));
    const normalized = response.items.map(normalizeAppointmentItem).filter(Boolean);

    return res.json({
      source: response.source,
      appointments: normalized,
      raw: response.raw,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao buscar agendamentos do dia.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/professionals", async (req, res) => {
  try {
    const { establishmentId, date } = req.body || {};

    if (!establishmentId || !date) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, date",
      });
    }

    const professionals = await getProfessionals({
      establishmentId: Number(establishmentId),
      date: String(date),
      serviceId: req.body?.serviceId ? Number(req.body.serviceId) : undefined,
    });

    const normalized = professionals
      .filter(professionalHasOpenSchedule)
      .map((item) => ({
        id: item.id,
        name: professionalDisplayName(item.name),
        availableTimes: item.availableTimes,
      }));

    return res.json({ professionals: normalized });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao buscar profissionais.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/diagnostics/professionals-raw", async (req, res) => {
  try {
    const { establishmentId, date } = req.body || {};

    if (!establishmentId || !date) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, date",
      });
    }

    const payload = await trinksRequest(`/agendamentos/profissionais/${date}`, {
      method: "GET",
      estabelecimentoId: Number(establishmentId),
      query: {
        serviceId: req.body?.serviceId ? Number(req.body.serviceId) : undefined,
        servicoId: req.body?.serviceId ? Number(req.body.serviceId) : undefined,
      },
    });

    return res.json(payload);
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao buscar profissionais em modo diagnostico.",
      details: error.details || null,
    });
  }
});

app.post("/api/chat", async (req, res) => {
  try {
    const { establishmentId, message, history, customerContext } = req.body || {};

    if (!establishmentId || !message) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, message",
      });
    }

    const text = await sendChatMessage({
      establishmentId: Number(establishmentId),
      message: String(message),
      history: Array.isArray(history) ? history : [],
      customerContext: customerContext && typeof customerContext === "object" ? customerContext : null,
    });

    return res.json({ text });
  } catch (error) {
    return res.status(error.status || 500).json({
      message: error.message || "Erro ao processar conversa com a IA.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/appointments/reschedule", async (req, res) => {
  try {
    const {
      establishmentId,
      confirmationCode,
      appointmentId,
      date,
      time,
    } = req.body || {};

    if (!establishmentId || !date || !time || (!confirmationCode && !appointmentId)) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, date, time e (confirmationCode ou appointmentId)",
      });
    }

    const result = await rescheduleAppointment({
      establishmentId: Number(establishmentId),
      confirmationCode: confirmationCode ? String(confirmationCode) : "",
      appointmentId: appointmentId ? String(appointmentId) : "",
      date: String(date),
      time: String(time),
    });

    return res.json(result);
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao reagendar horario.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/diagnostics/appointment-context", async (req, res) => {
  try {
    const {
      establishmentId,
      service,
      date,
      professionalName,
      clientName,
      clientPhone,
    } = req.body || {};

    if (!establishmentId || !service || !date || !clientName || !clientPhone) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, service, date, clientName, clientPhone",
      });
    }

    const context = await resolveAppointmentContext({
      establishmentId: Number(establishmentId),
      service: String(service),
      date: String(date),
      professionalName: professionalName ? String(professionalName) : "",
      clientName: String(clientName),
      clientPhone: String(clientPhone),
    });

    return res.json({
      serviceId: context.serviceId,
      clientId: context.clientId,
      professional: context.professional,
      duration: context.duration,
      amount: context.amount,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao resolver contexto do agendamento.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/diagnostics/service", async (req, res) => {
  try {
    const { establishmentId, service } = req.body || {};

    if (!establishmentId || !service) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, service",
      });
    }

    const foundService = await findServiceByName(Number(establishmentId), String(service));
    return res.json({ service: foundService, serviceId: serviceIdFrom(foundService) });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao consultar servico.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/diagnostics/client", async (req, res) => {
  try {
    const { establishmentId, clientName, clientPhone } = req.body || {};

    if (!establishmentId || !clientName || !clientPhone) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, clientName, clientPhone",
      });
    }

    const client = await findOrCreateClient(
      Number(establishmentId),
      String(clientName),
      String(clientPhone),
    );

    return res.json({ client, clientId: clientIdFrom(client) });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao consultar/criar cliente.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/diagnostics/client-search", async (req, res) => {
  try {
    const { establishmentId, clientName } = req.body || {};

    if (!establishmentId || !clientName) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, clientName",
      });
    }

    const payload = await trinksRequest("/clientes", {
      method: "GET",
      estabelecimentoId: Number(establishmentId),
      query: {
        nome: String(clientName),
        page: 1,
        pageSize: 20,
      },
    });

    return res.json({ items: extractItems(payload), raw: payload });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao buscar cliente.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/diagnostics/client-create", async (req, res) => {
  try {
    const { establishmentId, clientName, clientPhone } = req.body || {};

    if (!establishmentId || !clientName || !clientPhone) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, clientName, clientPhone",
      });
    }

    const parsedPhone = parseBrazilianPhone(clientPhone);
    const phoneEntry = parsedPhone
      ? { ddi: parsedPhone.ddi, ddd: parsedPhone.ddd, numero: parsedPhone.numero, tipoId: 1 }
      : { numero: normalizePhone(clientPhone), tipoId: 1 };

    const payload = {
      nome: String(clientName),
      telefones: [phoneEntry],
    };

    const created = await trinksRequest("/clientes", {
      method: "POST",
      estabelecimentoId: Number(establishmentId),
      body: payload,
    });

    return res.json({ created, payload });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao criar cliente.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/diagnostics/appointment-variants", async (req, res) => {
  try {
    const {
      establishmentId,
      service,
      date,
      time,
      professionalName,
      clientName,
      clientPhone,
    } = req.body || {};

    if (!establishmentId || !service || !date || !time || !clientName || !clientPhone) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, service, date, time, clientName, clientPhone",
      });
    }

    const context = await resolveAppointmentContext({
      establishmentId: Number(establishmentId),
      service: String(service),
      date: String(date),
      professionalName: professionalName ? String(professionalName) : "",
      clientName: String(clientName),
      clientPhone: String(clientPhone),
    });

    const result = await tryCreateAppointmentVariants({
      establishmentId: Number(establishmentId),
      date: String(date),
      time: String(time),
      context,
    });

    return res.json(result);
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao diagnosticar variacoes de agendamento.",
      details: error.details || null,
    });
  }
});

app.post("/api/trinks/appointments/cancel", async (req, res) => {
  try {
    const {
      establishmentId,
      confirmationCode,
      appointmentId,
      reason,
      clientPhone,
      clientName,
      date,
      time,
      service,
      professionalName,
    } = req.body || {};

    if (!establishmentId || (!confirmationCode && !appointmentId && !clientPhone)) {
      return res.status(400).json({
        message:
          "Campos obrigatorios: establishmentId e (confirmationCode ou appointmentId ou clientPhone)",
      });
    }

    const result = await cancelAppointment({
      establishmentId: Number(establishmentId),
      confirmationCode: confirmationCode ? String(confirmationCode) : "",
      appointmentId: appointmentId ? String(appointmentId) : "",
      reason: reason ? String(reason) : "",
      clientPhone: clientPhone ? String(clientPhone) : "",
      clientName: clientName ? String(clientName) : "",
      date: date ? String(date) : "",
      time: time ? String(time) : "",
      service: service ? String(service) : "",
      professionalName: professionalName ? String(professionalName) : "",
    });

    return res.json(result);
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao cancelar horario.",
      details: error.details || null,
    });
  }
});

app.get("/api/db/conversations", (req, res) => {
  try {
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 500);
    const rows = db.prepare(
      `
        SELECT m.phone,
               m.content AS lastMessage,
               m.role AS lastRole,
               m.at AS updatedAt,
               (
                 SELECT sender_name
                 FROM whatsapp_messages u
                 WHERE u.phone = m.phone
                   AND u.role = 'user'
                   AND COALESCE(u.sender_name, '') <> ''
                 ORDER BY datetime(u.at) DESC, u.id DESC
                 LIMIT 1
               ) AS name,
               (
                 SELECT COUNT(*)
                 FROM whatsapp_messages c
                 WHERE c.phone = m.phone
               ) AS count
        FROM whatsapp_messages m
        JOIN (
          SELECT phone, MAX(id) AS max_id
          FROM whatsapp_messages
          GROUP BY phone
        ) latest ON latest.max_id = m.id
        ORDER BY datetime(m.at) DESC, m.id DESC
        LIMIT ?
      `,
    ).all(limit);

    return res.json({ status: "ok", data: rows });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: error.message || "Erro ao consultar conversas no banco.",
    });
  }
});

app.get("/api/db/messages", (req, res) => {
  try {
    const phone = normalizePhone(req.query.phone || "");
    if (!phone) {
      return res.status(400).json({ message: "Informe ?phone=numero" });
    }

    const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 1000);
    const rows = db.prepare(
      `
        SELECT id, phone, role, content, sender_name AS senderName, at, source
        FROM whatsapp_messages
        WHERE phone = ?
        ORDER BY datetime(at) DESC, id DESC
        LIMIT ?
      `,
    ).all(phone, limit);

    return res.json({ status: "ok", phone, messages: rows.reverse() });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: error.message || "Erro ao consultar mensagens no banco.",
    });
  }
});

app.get("/api/db/appointments-audit", (req, res) => {
  try {
    const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 1000);
    const phone = normalizePhone(req.query.phone || "");
    const status = toNonEmptyString(req.query.status);

    let query = `
      SELECT id, event_type AS eventType, status, establishment_id AS establishmentId,
             appointment_id AS appointmentId, confirmation_code AS confirmationCode,
             client_phone AS clientPhone, client_name AS clientName, service_name AS serviceName,
             professional_name AS professionalName, appointment_date AS date, appointment_time AS time,
             request_payload AS requestPayload, response_payload AS responsePayload,
             error_message AS errorMessage, created_at AS createdAt
      FROM appointment_audit
    `;

    const params = [];
    const conditions = [];
    if (phone) {
      conditions.push("client_phone = ?");
      params.push(phone);
    }
    if (status) {
      conditions.push("status = ?");
      params.push(status);
    }
    if (conditions.length) {
      query += ` WHERE ${conditions.join(" AND ")}`;
    }
    query += " ORDER BY datetime(created_at) DESC, id DESC LIMIT ?";
    params.push(limit);

    const rows = db.prepare(query).all(...params).map((row) => ({
      ...row,
      requestPayload: row.requestPayload ? safeJsonParse(row.requestPayload) : null,
      responsePayload: row.responsePayload ? safeJsonParse(row.responsePayload) : null,
    }));

    return res.json({ status: "ok", data: rows });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: error.message || "Erro ao consultar auditoria de agendamentos.",
    });
  }
});

app.get("/api/db/webhook-events", (req, res) => {
  try {
    const limit = Math.min(Math.max(Number(req.query.limit || 200), 1), 2000);
    const phone = normalizePhone(req.query.phone || "");
    const status = toNonEmptyString(req.query.status);
    const reason = toNonEmptyString(req.query.reason);

    let query = `
      SELECT id, event, instance_name AS instanceName, sender_raw AS senderRaw,
             sender_number AS senderNumber, sender_name AS senderName,
             message_id AS messageId, message_type AS messageType,
             message_text AS messageText, status, reason, details, received_at AS receivedAt
      FROM webhook_events
    `;

    const params = [];
    const conditions = [];
    if (phone) {
      conditions.push("sender_number = ?");
      params.push(phone);
    }
    if (status) {
      conditions.push("status = ?");
      params.push(status);
    }
    if (reason) {
      conditions.push("reason = ?");
      params.push(reason);
    }
    if (conditions.length) {
      query += ` WHERE ${conditions.join(" AND ")}`;
    }
    query += " ORDER BY datetime(received_at) DESC, id DESC LIMIT ?";
    params.push(limit);

    const rows = db.prepare(query).all(...params).map((row) => ({
      ...row,
      details: row.details ? safeJsonParse(row.details) : null,
    }));

    return res.json({ status: "ok", data: rows });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: error.message || "Erro ao consultar eventos de webhook.",
    });
  }
});

app.get("/api/whatsapp/inbox", (req, res) => {
  const conversations = summarizeWhatsappConversations();
  return res.json({ status: "ok", conversations });
});

app.get("/api/whatsapp/messages", (req, res) => {
  const phone = normalizePhone(req.query.phone || "");
  if (!phone) {
    return res.status(400).json({ message: "Informe ?phone=numero" });
  }

  const messages = getWhatsappHistory(phone);
  return res.json({ status: "ok", phone, messages });
});

app.post("/api/evolution/send-text", async (req, res) => {
  try {
    const instance = resolveEvolutionInstance(req.body?.instance || req.body?.instanceName);
    if (!instance) {
      return res.status(400).json({ message: "Informe instance/instanceName ou configure EVOLUTION_INSTANCE." });
    }

    const { to, text } = req.body || {};

    if (!to || !text) {
      return res.status(400).json({ message: "Campos obrigatorios: to, text" });
    }

    const trimmedText = String(text).trim();
    const normalizedTarget = normalizePhone(to);

    if (normalizedTarget && /^\/(retomar(\s|-)?ia|ia\s+on)$/i.test(trimmedText)) {
      clearHumanHandoffSession(normalizedTarget);
      return res.json({
        status: "ok",
        action: "handoffResumed",
        phone: normalizedTarget,
      });
    }

    if (normalizedTarget && /^\/(humano(\s|-)?on|handoff(\s|-)?on)$/i.test(trimmedText)) {
      const session = setHumanHandoffSession(normalizedTarget, {
        source: "manualCommand",
        reason: "Ativado manualmente pelo painel",
      });
      return res.json({
        status: "ok",
        action: "handoffActivated",
        phone: normalizedTarget,
        session,
      });
    }

    const payload = {
      number: String(to),
      text: String(text),
    };

    const result = await evolutionRequest(`/message/sendText/${instance}`, {
      method: "POST",
      body: payload,
    });

    const normalized = normalizePhone(to);
    if (normalized) {
      pushWhatsappHistory(normalized, "assistant", text);
    }

    return res.json({ status: "sent", result });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao enviar mensagem na Evolution.",
      details: error.details || null,
    });
  }
});

app.get("/api/handoff/status", (req, res) => {
  const phone = normalizePhone(req.query.phone || "");
  if (!phone) {
    return res.status(400).json({ message: "Informe ?phone=numero" });
  }

  const session = getHumanHandoffSession(phone);
  return res.json({
    status: "ok",
    phone,
    active: Boolean(session?.active),
    session: session || null,
  });
});

app.post("/api/handoff/activate", (req, res) => {
  const phone = normalizePhone(req.body?.phone || req.body?.clientPhone || "");
  if (!phone) {
    return res.status(400).json({ message: "Campo obrigatorio: phone" });
  }

  const session = setHumanHandoffSession(phone, {
    source: "api",
    reason: toNonEmptyString(req.body?.reason) || "Ativado por endpoint",
    customerName: toNonEmptyString(req.body?.customerName),
  });

  return res.json({
    status: "ok",
    action: "handoffActivated",
    phone,
    session,
  });
});

app.post("/api/handoff/resume", async (req, res) => {
  try {
    const phone = normalizePhone(req.body?.phone || req.body?.clientPhone || "");
    if (!phone) {
      return res.status(400).json({ message: "Campo obrigatorio: phone" });
    }

    const hadSession = Boolean(getHumanHandoffSession(phone));
    clearHumanHandoffSession(phone);

    const notifyClient = Boolean(req.body?.notifyClient);
    if (notifyClient) {
      const instance = resolveEvolutionInstance(req.body?.instance || req.body?.instanceName);
      if (!instance) {
        return res.status(400).json({
          message: "Para notifyClient=true, informe instance/instanceName ou configure EVOLUTION_INSTANCE.",
        });
      }

      const message =
        toNonEmptyString(req.body?.message) ||
        "Perfeito. Voltei com o atendimento automatico do Jacques para te ajudar.";

      await evolutionRequest(`/message/sendText/${instance}`, {
        method: "POST",
        body: { number: phone, text: message },
      });
      pushWhatsappHistory(phone, "assistant", message);
    }

    return res.json({
      status: "ok",
      action: "handoffResumed",
      phone,
      hadSession,
      notifyClient,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao retomar atendimento automatico.",
      details: error.details || null,
    });
  }
});

app.post("/webhook/whatsapp", webhookBodyParser, async (req, res) => {
  let incoming = null;
  let payloadParse = null;
  try {
    payloadParse = parseWebhookPayload(req.body);
    incoming = extractIncomingWhatsapp(payloadParse.payload || {});

    if (payloadParse.parseError) {
      const inferredSender = incoming.senderNumber || inferSenderFromRawWebhookText(payloadParse.rawText);
      recordWebhookEvent({
        event: incoming.event,
        instanceName: incoming.instanceName,
        senderRaw: incoming.senderRaw,
        senderNumber: inferredSender,
        senderName: incoming.senderName,
        messageId: incoming.messageId,
        messageType: incoming.messageType,
        messageText: incoming.messageText,
        status: "ignored",
        reason: "invalidPayload",
        details: {
          parseStatus: payloadParse.parseStatus,
          parseError: payloadParse.parseError,
          rawSnippet: toNonEmptyString(payloadParse.rawText).slice(0, 1200),
        },
      });
      return res.status(200).json({
        received: true,
        ignored: true,
        reason: "invalidPayload",
      });
    }

    if (incoming.fromMe) {
      recordWebhookEvent({
        event: incoming.event,
        instanceName: incoming.instanceName,
        senderRaw: incoming.senderRaw,
        senderNumber: incoming.senderNumber,
        senderName: incoming.senderName,
        messageId: incoming.messageId,
        messageType: incoming.messageType,
        messageText: incoming.messageText,
        status: "ignored",
        reason: "fromMe",
      });
      return res.status(200).json({ received: true, ignored: true, reason: "fromMe" });
    }

    if (incoming.isGroup) {
      recordWebhookEvent({
        event: incoming.event,
        instanceName: incoming.instanceName,
        senderRaw: incoming.senderRaw,
        senderNumber: incoming.senderNumber,
        senderName: incoming.senderName,
        messageId: incoming.messageId,
        messageType: incoming.messageType,
        messageText: incoming.messageText,
        status: "ignored",
        reason: "groupMessage",
      });
      return res.status(200).json({ received: true, ignored: true, reason: "groupMessage" });
    }

    if (!incoming.senderNumber) {
      console.warn("[webhook] ignored missingSenderOrText", {
        event: incoming.event || null,
        fromMe: incoming.fromMe,
        senderRaw: incoming.senderRaw || null,
        senderNumber: incoming.senderNumber || null,
        hasText: Boolean(incoming.messageText),
        messageId: incoming.messageId || null,
        instance: incoming.instanceName || null,
      });
      recordWebhookEvent({
        event: incoming.event,
        instanceName: incoming.instanceName,
        senderRaw: incoming.senderRaw,
        senderNumber: incoming.senderNumber,
        senderName: incoming.senderName,
        messageId: incoming.messageId,
        messageType: incoming.messageType,
        messageText: incoming.messageText,
        status: "ignored",
        reason: "missingSender",
      });
      return res.status(200).json({
        received: true,
        ignored: true,
        reason: "missingSender",
        event: incoming.event || null,
      });
    }

    if (!incoming.messageText) {
      const messageEvent = isLikelyIncomingMessageEvent(incoming);
      if (messageEvent) {
        const establishmentId = getConfiguredEstablishmentId();
        const knownClient = establishmentId
          ? await findExistingClientByPhone(establishmentId, incoming.senderNumber).catch(() => null)
          : null;
        const knownClientName = toNonEmptyString(clientDisplayNameFrom(knownClient));
        const effectiveClientName = knownClientName || incoming.senderName;
        const instance = resolveEvolutionInstance(incoming.instanceName);
        const placeholder = unsupportedInboundPlaceholder(incoming);

        pushWhatsappHistory(incoming.senderNumber, "user", placeholder, effectiveClientName);

        if (instance) {
          await evolutionRequest(`/message/sendText/${instance}`, {
            method: "POST",
            body: {
              number: incoming.senderNumber,
              text: UNSUPPORTED_MESSAGE_REPLY,
            },
          });
          pushWhatsappHistory(incoming.senderNumber, "assistant", UNSUPPORTED_MESSAGE_REPLY);
        }

        recordWebhookEvent({
          event: incoming.event,
          instanceName: incoming.instanceName,
          senderRaw: incoming.senderRaw,
          senderNumber: incoming.senderNumber,
          senderName: effectiveClientName,
          messageId: incoming.messageId,
          messageType: incoming.messageType,
          messageText: "",
          status: "processed",
          reason: "unsupportedMessageType",
          details: {
            messageType: incoming.messageType,
            replied: Boolean(instance),
          },
        });

        return res.status(200).json({
          received: true,
          processed: true,
          reason: "unsupportedMessageType",
          event: incoming.event || null,
          messageType: incoming.messageType || null,
          messageId: incoming.messageId || null,
        });
      }

      console.warn("[webhook] ignored missingSenderOrText", {
        event: incoming.event || null,
        fromMe: incoming.fromMe,
        senderRaw: incoming.senderRaw || null,
        senderNumber: incoming.senderNumber || null,
        hasText: false,
        messageId: incoming.messageId || null,
        instance: incoming.instanceName || null,
      });
      recordWebhookEvent({
        event: incoming.event,
        instanceName: incoming.instanceName,
        senderRaw: incoming.senderRaw,
        senderNumber: incoming.senderNumber,
        senderName: incoming.senderName,
        messageId: incoming.messageId,
        messageType: incoming.messageType,
        messageText: "",
        status: "ignored",
        reason: "missingText",
      });
      return res.status(200).json({
        received: true,
        ignored: true,
        reason: "missingText",
        event: incoming.event || null,
      });
    }

    if (isDuplicateIncomingWhatsapp(incoming)) {
      recordWebhookEvent({
        event: incoming.event,
        instanceName: incoming.instanceName,
        senderRaw: incoming.senderRaw,
        senderNumber: incoming.senderNumber,
        senderName: incoming.senderName,
        messageId: incoming.messageId,
        messageType: incoming.messageType,
        messageText: incoming.messageText,
        status: "ignored",
        reason: "duplicateMessage",
      });
      return res.status(200).json({
        received: true,
        ignored: true,
        reason: "duplicateMessage",
        messageId: incoming.messageId || null,
      });
    }

    const establishmentId = getConfiguredEstablishmentId();
    if (!establishmentId) {
      const configError = new Error(
        "Configure TRINKS_ESTABLISHMENT_ID (ou VITE_TRINKS_ESTABLISHMENT_ID) no backend para usar webhook WhatsApp.",
      );
      configError.status = 500;
      throw configError;
    }

    const knownClient = await findExistingClientByPhone(establishmentId, incoming.senderNumber).catch(() => null);
    const knownClientName = toNonEmptyString(clientDisplayNameFrom(knownClient));
    const effectiveClientName = knownClientName || incoming.senderName;
    const instance = resolveEvolutionInstance(incoming.instanceName);
    if (!instance) {
      const instanceError = new Error("EVOLUTION_INSTANCE nao configurado e nao informado no webhook.");
      instanceError.status = 500;
      throw instanceError;
    }

    const previousHistory = getWhatsappHistory(incoming.senderNumber);
    pushWhatsappHistory(incoming.senderNumber, "user", incoming.messageText, effectiveClientName);

    if (isHumanHandoffEnabled()) {
      const askedHuman = isHumanHandoffRequest(incoming.messageText);
      const askedResume = isHumanHandoffResumeRequest(incoming.messageText);
      const activeHandoff = getHumanHandoffSession(incoming.senderNumber);

      if (askedResume && activeHandoff?.active) {
        clearHumanHandoffSession(incoming.senderNumber);

        const resumeMessage =
          toNonEmptyString(process.env.HUMAN_HANDOFF_RESUME_MESSAGE) ||
          "Perfeito. Voltei com o atendimento automatico do Jacques para te ajudar.";

        await evolutionRequest(`/message/sendText/${instance}`, {
          method: "POST",
          body: {
            number: incoming.senderNumber,
            text: resumeMessage,
          },
        });
        pushWhatsappHistory(incoming.senderNumber, "assistant", resumeMessage);
        recordWebhookEvent({
          event: incoming.event,
          instanceName: instance,
          senderRaw: incoming.senderRaw,
          senderNumber: incoming.senderNumber,
          senderName: effectiveClientName,
          messageId: incoming.messageId,
          messageType: incoming.messageType,
          messageText: incoming.messageText,
          status: "processed",
          reason: "handoffResumed",
        });

        return res.status(200).json({
          received: true,
          processed: true,
          handoff: true,
          handoffAction: "resumed",
          instance,
          to: incoming.senderNumber,
          event: incoming.event || null,
          messageId: incoming.messageId || null,
        });
      }

      if (askedHuman) {
        const session = setHumanHandoffSession(incoming.senderNumber, {
          source: "customerRequest",
          reason: incoming.messageText,
          establishmentId,
          customerName: effectiveClientName,
          messageId: incoming.messageId || "",
        });

        const ackMessage =
          toNonEmptyString(process.env.HUMAN_HANDOFF_ACK_MESSAGE) ||
          "Perfeito. Vou acionar nossa recepcao agora e um atendente humano segue com voce.";

        await evolutionRequest(`/message/sendText/${instance}`, {
          method: "POST",
          body: {
            number: incoming.senderNumber,
            text: ackMessage,
          },
        });
        pushWhatsappHistory(incoming.senderNumber, "assistant", ackMessage);

        const alertResult = await notifyHumanAlertPhones({
          instance,
          establishmentId,
          customerPhone: incoming.senderNumber,
          customerName: effectiveClientName,
          customerMessage: incoming.messageText,
        });
        recordWebhookEvent({
          event: incoming.event,
          instanceName: instance,
          senderRaw: incoming.senderRaw,
          senderNumber: incoming.senderNumber,
          senderName: effectiveClientName,
          messageId: incoming.messageId,
          messageType: incoming.messageType,
          messageText: incoming.messageText,
          status: "processed",
          reason: "handoffActivated",
          details: {
            alertPhones: Array.isArray(alertResult?.targets)
              ? alertResult.targets.map((item) => item.phone || item.number).filter(Boolean)
              : [],
          },
        });

        return res.status(200).json({
          received: true,
          processed: true,
          handoff: true,
          handoffAction: "activated",
          instance,
          to: incoming.senderNumber,
          event: incoming.event || null,
          messageId: incoming.messageId || null,
          session: session || null,
          alertResult,
        });
      }

      if (activeHandoff?.active) {
        const now = Date.now();
        const lastWaitAckAt = Number(activeHandoff.lastWaitAckAt || 0);
        const shouldSendWaitingAck = now - lastWaitAckAt > 5 * 60 * 1000;

        if (shouldSendWaitingAck) {
          const waitingMessage =
            toNonEmptyString(process.env.HUMAN_HANDOFF_WAITING_MESSAGE) ||
            "Nosso atendimento humano ja foi acionado e vai continuar com voce em instantes.";

          await evolutionRequest(`/message/sendText/${instance}`, {
            method: "POST",
            body: {
              number: incoming.senderNumber,
              text: waitingMessage,
            },
          });
          pushWhatsappHistory(incoming.senderNumber, "assistant", waitingMessage);

          setHumanHandoffSession(incoming.senderNumber, {
            ...activeHandoff,
            lastWaitAckAt: now,
          });
        }

        recordWebhookEvent({
          event: incoming.event,
          instanceName: instance,
          senderRaw: incoming.senderRaw,
          senderNumber: incoming.senderNumber,
          senderName: effectiveClientName,
          messageId: incoming.messageId,
          messageType: incoming.messageType,
          messageText: incoming.messageText,
          status: "processed",
          reason: "handoffActive",
          details: { waitingAckSent: shouldSendWaitingAck },
        });

        return res.status(200).json({
          received: true,
          processed: true,
          handoff: true,
          handoffAction: "active",
          instance,
          to: incoming.senderNumber,
          event: incoming.event || null,
          messageId: incoming.messageId || null,
        });
      }
    }

    const answer = await sendChatMessage({
      establishmentId,
      message: incoming.messageText,
      history: previousHistory,
      customerContext: {
        name: effectiveClientName,
        phone: incoming.senderNumber,
        fromTrinks: Boolean(knownClientName),
      },
    });

    await evolutionRequest(`/message/sendText/${instance}`, {
      method: "POST",
      body: {
        number: incoming.senderNumber,
        text: answer,
      },
    });

    pushWhatsappHistory(incoming.senderNumber, "assistant", answer);
    recordWebhookEvent({
      event: incoming.event,
      instanceName: instance,
      senderRaw: incoming.senderRaw,
      senderNumber: incoming.senderNumber,
      senderName: effectiveClientName,
      messageId: incoming.messageId,
      messageType: incoming.messageType,
      messageText: incoming.messageText,
      status: "processed",
      reason: "aiResponseSent",
    });

    return res.status(200).json({
      received: true,
      processed: true,
      instance,
      to: incoming.senderNumber,
      event: incoming.event || null,
      messageId: incoming.messageId || null,
    });
  } catch (error) {
    const fallbackInstance = resolveEvolutionInstance(incoming?.instanceName);
    const fallbackPhone = normalizePhone(incoming?.senderNumber || inferSenderFromRawWebhookText(payloadParse?.rawText || ""));
    const shouldSendFallback = Boolean(fallbackInstance && fallbackPhone);
    const fallbackText =
      "Tive uma instabilidade momentanea aqui. Pode repetir sua ultima mensagem para eu continuar?";

    if (shouldSendFallback) {
      try {
        await evolutionRequest(`/message/sendText/${fallbackInstance}`, {
          method: "POST",
          body: {
            number: fallbackPhone,
            text: fallbackText,
          },
        });
        pushWhatsappHistory(fallbackPhone, "assistant", fallbackText);
      } catch {
        // Evita falha em cascata: ainda devolvemos 200 para o webhook.
      }
    }

    recordWebhookEvent({
      event: incoming?.event || "",
      instanceName: incoming?.instanceName || "",
      senderRaw: incoming?.senderRaw || "",
      senderNumber: fallbackPhone,
      senderName: incoming?.senderName || "",
      messageId: incoming?.messageId || "",
      messageType: incoming?.messageType || "",
      messageText: incoming?.messageText || "",
      status: "error",
      reason: "exception",
      details: {
        message: error?.message || "Erro interno.",
        status: error?.status || 500,
        parseStatus: payloadParse?.parseStatus || "",
        fallbackSent: shouldSendFallback,
      },
    });
    return res.status(200).json({
      received: true,
      processed: false,
      status: "error",
      message: error.message || "Erro ao processar webhook do WhatsApp.",
      details: error.details || null,
    });
  }
});

app.use((error, req, res, next) => {
  if (error instanceof SyntaxError && error?.status === 400 && "body" in error) {
    if (req.path === "/webhook/whatsapp") {
      recordWebhookEvent({
        status: "ignored",
        reason: "invalidPayloadSyntax",
        details: {
          path: req.path,
          method: req.method,
          message: error.message || "JSON invalido.",
        },
      });
      return res.status(200).json({
        received: true,
        ignored: true,
        reason: "invalidPayloadSyntax",
      });
    }

    return res.status(400).json({
      status: "error",
      message: "JSON invalido na requisicao.",
    });
  }

  return next(error);
});

app.listen(port, () => {
  console.log(`Backend online em http://localhost:${port}`);
});

