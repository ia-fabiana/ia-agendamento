import express from "express";
import dotenv from "dotenv";
import { GoogleGenAI, Type } from "@google/genai";
import Database from "better-sqlite3";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const envPath = path.join(__dirname, "..", ".env");
dotenv.config({ path: envPath });

const app = express();
const port = Number(process.env.PORT || 3001);
const KNOWLEDGE_FILE_PATH = path.join(__dirname, "salonKnowledge.json");
const DATA_DIR_PATH = path.join(__dirname, "data");
const PUBLIC_UPLOADS_DIR_PATH = path.join(DATA_DIR_PATH, "uploads");
const MARKETING_UPLOADS_DIR_PATH = path.join(PUBLIC_UPLOADS_DIR_PATH, "marketing");
const SQLITE_DB_PATH = process.env.IA_DB_PATH?.trim() || path.join(DATA_DIR_PATH, "ia_agendamento.sqlite");
const MARKETING_UPLOAD_MAX_BYTES = Math.max(
  256 * 1024,
  Number(process.env.MARKETING_UPLOAD_MAX_BYTES || 5 * 1024 * 1024),
);
const MAX_WHATSAPP_HISTORY_MESSAGES = 20;
const BOOKING_CONFIRMATION_TTL_MS = 20 * 60 * 1000;
const HUMAN_HANDOFF_TTL_MS = 12 * 60 * 60 * 1000;
const BOT_AUTOCLOSE_TTL_MS = Math.max(
  10 * 60 * 1000,
  Number(process.env.BOT_AUTOCLOSE_TTL_MS || 12 * 60 * 60 * 1000),
);
const BOOKING_MAX_DAYS_AHEAD = Math.max(1, Number(process.env.BOOKING_MAX_DAYS_AHEAD || 60));
const BOOKING_MAX_DAYS_AHEAD_TENANTS = new Set(
  String(process.env.BOOKING_MAX_DAYS_AHEAD_TENANTS || "jacques-janine-leo,essencia")
    .split(/[,;\s]+/g)
    .map((item) => normalizeTenantCode(item))
    .filter(Boolean),
);
const BOOKING_MAX_DAYS_AHEAD_ESTABLISHMENTS = new Set(
  String(process.env.BOOKING_MAX_DAYS_AHEAD_ESTABLISHMENT_IDS || "62260,62217")
    .split(/[,;\s]+/g)
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item) && item > 0),
);
const MARKETING_ACTION_SESSION_TTL_MS = Math.max(
  10 * 60 * 1000,
  Number(process.env.MARKETING_ACTION_SESSION_TTL_MS || 6 * 60 * 60 * 1000),
);
const TRINKS_MAX_RETRIES = Math.max(1, Number(process.env.TRINKS_MAX_RETRIES || 4));
const TRINKS_RETRY_BASE_MS = Math.max(200, Number(process.env.TRINKS_RETRY_BASE_MS || 700));
const BOOKING_SEQUENCE_GAP_MS = Math.max(0, Number(process.env.BOOKING_SEQUENCE_GAP_MS || 450));
const TRINKS_STATUS_ID_CANCELLED = Math.max(1, Number(process.env.TRINKS_STATUS_ID_CANCELLED || 9));
const SCHEDULING_PROVIDER_TRINKS = "trinks";
const SCHEDULING_PROVIDER_GOOGLE_CALENDAR = "google_calendar";
const TENANT_SESSION_TTL_HOURS = Math.max(1, Number(process.env.TENANT_SESSION_TTL_HOURS || 24));
const TENANT_MIN_PASSWORD_LENGTH = Math.max(8, Number(process.env.TENANT_MIN_PASSWORD_LENGTH || 8));
const STRICT_TEST_BOOKING_GUARD_ENABLED =
  !["0", "false", "off", "no", "nao"].includes(
    String(process.env.STRICT_TEST_BOOKING_GUARD_ENABLED || "true").trim().toLowerCase(),
  );
const TEST_BOOKING_MARKER = toNonEmptyString(process.env.TEST_BOOKING_MARKER || "#TESTE");
const INTERNAL_TEST_PHONE_SET = new Set(
  parsePhoneList(process.env.INTERNAL_TEST_PHONES || process.env.TEST_INTERNAL_PHONES || ""),
);
const UNSUPPORTED_MESSAGE_REPLY =
  toNonEmptyString(process.env.WHATSAPP_UNSUPPORTED_MESSAGE_REPLY) ||
  "Recebi sua mensagem, mas no momento consigo ler apenas texto. Pode me escrever por texto, por favor?";
const whatsappConversations = new Map();
const recentWebhookMessages = new Map();
const pendingBookingConfirmations = new Map();
const humanHandoffSessions = new Map();
const botAutoClosedSessions = new Map();
const marketingActionSessions = new Map();
const WEBHOOK_DEDUPE_WINDOW_MS = 30_000;
const db = initDatabase();

function initDatabase() {
  mkdirSync(DATA_DIR_PATH, { recursive: true });
  mkdirSync(MARKETING_UPLOADS_DIR_PATH, { recursive: true });
  const database = new Database(SQLITE_DB_PATH);
  database.pragma("journal_mode = WAL");
  database.pragma("foreign_keys = ON");

  database.exec(`
    CREATE TABLE IF NOT EXISTS whatsapp_messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_code TEXT DEFAULT '',
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
      tenant_code TEXT DEFAULT '',
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
      tenant_code TEXT DEFAULT '',
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

    CREATE TABLE IF NOT EXISTS tenants (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      segment TEXT DEFAULT '',
      active INTEGER NOT NULL DEFAULT 1,
      default_provider TEXT DEFAULT 'trinks',
      establishment_id INTEGER,
      knowledge_json TEXT DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_tenants_code
      ON tenants(code);

    CREATE TABLE IF NOT EXISTS tenant_identifiers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL,
      kind TEXT NOT NULL,
      value TEXT NOT NULL,
      normalized_value TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(kind, normalized_value),
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_tenant_identifiers_tenant
      ON tenant_identifiers(tenant_id);

    CREATE INDEX IF NOT EXISTS idx_tenant_identifiers_kind
      ON tenant_identifiers(kind, normalized_value);

    CREATE TABLE IF NOT EXISTS tenant_provider_configs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL,
      provider TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      config_json TEXT DEFAULT '{}',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(tenant_id, provider),
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_tenant_provider_configs_tenant
      ON tenant_provider_configs(tenant_id);

    CREATE TABLE IF NOT EXISTS tenant_users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL,
      username TEXT NOT NULL,
      display_name TEXT DEFAULT '',
      password_hash TEXT NOT NULL,
      active INTEGER NOT NULL DEFAULT 1,
      last_login_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(tenant_id, username),
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_tenant_users_tenant
      ON tenant_users(tenant_id);

    CREATE TABLE IF NOT EXISTS tenant_user_sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_user_id INTEGER NOT NULL,
      token_hash TEXT NOT NULL UNIQUE,
      expires_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_seen_at TEXT,
      FOREIGN KEY(tenant_user_id) REFERENCES tenant_users(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_tenant_user_sessions_user
      ON tenant_user_sessions(tenant_user_id);

    CREATE INDEX IF NOT EXISTS idx_tenant_user_sessions_expires_at
      ON tenant_user_sessions(expires_at);

    CREATE TABLE IF NOT EXISTS tenant_crm_settings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL UNIQUE,
      config_json TEXT DEFAULT '{}',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS tenant_service_return_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL,
      service_key TEXT NOT NULL,
      service_name TEXT NOT NULL,
      category_key TEXT DEFAULT '',
      category_name TEXT DEFAULT '',
      active INTEGER NOT NULL DEFAULT 0,
      return_days INTEGER,
      use_default_flow INTEGER NOT NULL DEFAULT 1,
      step1_delay_days INTEGER,
      step1_message_template TEXT DEFAULT '',
      step2_delay_days INTEGER,
      step2_message_template TEXT DEFAULT '',
      step3_delay_days INTEGER,
      step3_message_template TEXT DEFAULT '',
      priority TEXT DEFAULT 'medium',
      service_name_aliases TEXT DEFAULT '',
      notes TEXT DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(tenant_id, service_key),
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_tenant_service_return_rules_tenant
      ON tenant_service_return_rules(tenant_id, category_key, service_name);

    CREATE TABLE IF NOT EXISTS tenant_category_opportunity_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL,
      category_key TEXT NOT NULL,
      category_name TEXT NOT NULL,
      opportunity_tracking_enabled INTEGER NOT NULL DEFAULT 1,
      opportunity_days_without_return INTEGER,
      opportunity_priority TEXT DEFAULT 'medium',
      allow_manual_campaign INTEGER NOT NULL DEFAULT 1,
      suggested_message_template TEXT DEFAULT '',
      notes TEXT DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(tenant_id, category_key),
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_tenant_category_opportunity_rules_tenant
      ON tenant_category_opportunity_rules(tenant_id, category_key, category_name);

    CREATE TABLE IF NOT EXISTS crm_client_blocks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL,
      client_id INTEGER,
      client_name TEXT DEFAULT '',
      phone TEXT NOT NULL,
      is_blocked INTEGER NOT NULL DEFAULT 1,
      block_reason TEXT DEFAULT '',
      block_notes TEXT DEFAULT '',
      blocked_at TEXT,
      blocked_by TEXT DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(tenant_id, phone),
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_crm_client_blocks_tenant
      ON crm_client_blocks(tenant_id, phone, is_blocked);

    CREATE TABLE IF NOT EXISTS crm_return_flows (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL,
      client_id INTEGER,
      client_name TEXT DEFAULT '',
      phone TEXT NOT NULL,
      origin_service_key TEXT DEFAULT '',
      origin_service_name TEXT DEFAULT '',
      origin_category_key TEXT DEFAULT '',
      origin_category_name TEXT DEFAULT '',
      last_visit_at TEXT DEFAULT '',
      last_professional_id INTEGER,
      last_professional_name TEXT DEFAULT '',
      last_professional_active INTEGER,
      flow_status TEXT NOT NULL DEFAULT 'eligible',
      current_step INTEGER NOT NULL DEFAULT 0,
      entered_flow_at TEXT,
      last_message_sent_at TEXT,
      next_scheduled_send_at TEXT,
      stop_reason TEXT DEFAULT '',
      converted_appointment_id INTEGER,
      converted_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_crm_return_flows_tenant
      ON crm_return_flows(tenant_id, flow_status, updated_at DESC);

    CREATE TABLE IF NOT EXISTS crm_flow_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      flow_id INTEGER,
      tenant_id INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      step INTEGER,
      message_preview TEXT DEFAULT '',
      message_sent TEXT DEFAULT '',
      reply_summary TEXT DEFAULT '',
      booking_id INTEGER,
      metadata_json TEXT DEFAULT '{}',
      created_at TEXT NOT NULL,
      FOREIGN KEY(flow_id) REFERENCES crm_return_flows(id) ON DELETE CASCADE,
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_crm_flow_events_flow
      ON crm_flow_events(flow_id, created_at DESC);

    CREATE TABLE IF NOT EXISTS crm_category_opportunities (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id INTEGER NOT NULL,
      client_id INTEGER,
      client_name TEXT DEFAULT '',
      phone TEXT DEFAULT '',
      category_key TEXT NOT NULL,
      category_name TEXT NOT NULL,
      source_service_key TEXT DEFAULT '',
      source_service_name TEXT DEFAULT '',
      last_relevant_visit_at TEXT DEFAULT '',
      days_without_return INTEGER,
      last_professional_id INTEGER,
      last_professional_name TEXT DEFAULT '',
      last_professional_active INTEGER,
      opportunity_status TEXT NOT NULL DEFAULT 'open',
      priority TEXT DEFAULT 'medium',
      owner TEXT DEFAULT '',
      notes TEXT DEFAULT '',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_crm_category_opportunities_tenant
      ON crm_category_opportunities(tenant_id, opportunity_status, category_key, updated_at DESC);
  `);

  ensureTenantIsolationColumns(database);
  backfillLegacyTenantCodes(database);

  return database;
}

function tableHasColumn(database, tableName, columnName) {
  const rows = database.prepare(`PRAGMA table_info(${tableName})`).all();
  return rows.some((row) => String(row?.name || "").toLowerCase() === String(columnName || "").toLowerCase());
}

function ensureTenantIsolationColumns(database) {
  if (!tableHasColumn(database, "whatsapp_messages", "tenant_code")) {
    database.exec("ALTER TABLE whatsapp_messages ADD COLUMN tenant_code TEXT DEFAULT ''");
  }
  if (!tableHasColumn(database, "appointment_audit", "tenant_code")) {
    database.exec("ALTER TABLE appointment_audit ADD COLUMN tenant_code TEXT DEFAULT ''");
  }
  if (!tableHasColumn(database, "webhook_events", "tenant_code")) {
    database.exec("ALTER TABLE webhook_events ADD COLUMN tenant_code TEXT DEFAULT ''");
  }
  if (!tableHasColumn(database, "tenant_service_return_rules", "service_name_aliases")) {
    database.exec("ALTER TABLE tenant_service_return_rules ADD COLUMN service_name_aliases TEXT DEFAULT ''");
  }

  database.exec(`
    CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_tenant_phone_at
      ON whatsapp_messages(tenant_code, phone, at DESC);
    CREATE INDEX IF NOT EXISTS idx_appointment_audit_tenant_created_at
      ON appointment_audit(tenant_code, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_webhook_events_tenant_received_at
      ON webhook_events(tenant_code, received_at DESC);
  `);
}

function backfillLegacyTenantCodes(database) {
  // Legacy rows (before multi-tenant tracking) are associated with the first tenant.
  const firstTenant = database.prepare("SELECT code FROM tenants ORDER BY id ASC LIMIT 1").get();
  const legacyTenantCode = normalizeTenantCode(firstTenant?.code || "");
  if (!legacyTenantCode) {
    return;
  }

  database.prepare(
    `
      UPDATE whatsapp_messages
      SET tenant_code = ?
      WHERE COALESCE(TRIM(tenant_code), '') = ''
    `,
  ).run(legacyTenantCode);

  database.prepare(
    `
      UPDATE appointment_audit
      SET tenant_code = ?
      WHERE COALESCE(TRIM(tenant_code), '') = ''
    `,
  ).run(legacyTenantCode);

  database.prepare(
    `
      UPDATE webhook_events
      SET tenant_code = ?
      WHERE COALESCE(TRIM(tenant_code), '') = ''
    `,
  ).run(legacyTenantCode);
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
  tenantCode = "",
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
          tenant_code, event, instance_name, sender_raw, sender_number, sender_name,
          message_id, message_type, message_text, status, reason, details, received_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    ).run(
      normalizeTenantScopeCode(tenantCode),
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

const jsonBodyParser = express.json({ limit: "12mb" });
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
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Api-Key, X-Admin-Token");
  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }
  next();
});
app.use("/uploads", express.static(PUBLIC_UPLOADS_DIR_PATH));

function ensureEnv(name) {
  const value = process.env[name];
  if (!value || !value.trim()) {
    throw new Error(`Variavel obrigatoria ausente: ${name}`);
  }
  return value.trim();
}

function isKnowledgeWriteAuthorized(req) {
  return Boolean(resolveAdminPrincipal(req));
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

function parsePhoneList(value) {
  const raw = toNonEmptyString(value);
  if (!raw) {
    return [];
  }
  return [...new Set(
    raw
      .split(/[,;\s]+/g)
      .map((item) => normalizePhone(item))
      .filter(Boolean),
  )];
}

function isInternalTestPhone(phone) {
  const normalized = normalizePhone(phone);
  if (!normalized) {
    return false;
  }
  return INTERNAL_TEST_PHONE_SET.has(normalized);
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

function detectTestSignal({ message = "", internalTester = false, explicitTestMode = false } = {}) {
  const normalizedMessage = normalizeForMatch(message);
  const normalizedMarker = normalizeForMatch(TEST_BOOKING_MARKER || "#TESTE");
  const hasMarker = normalizedMarker ? normalizedMessage.includes(normalizedMarker) : false;
  const hasTestKeyword = /\b(teste|testar|simulacao|homolog|mock|fantasma)\b/.test(normalizedMessage);
  const inferredTest = Boolean(explicitTestMode) || hasMarker || (Boolean(internalTester) && hasTestKeyword);
  return {
    inferredTest,
    hasMarker,
    hasTestKeyword,
    internalTester: Boolean(internalTester),
    explicitTestMode: Boolean(explicitTestMode),
  };
}

function normalizeTestAuthorization(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }
  const approvedBy = firstNonEmpty([input.approvedBy, input.authorizedBy, input.aprovadoPor]);
  const reason = firstNonEmpty([input.reason, input.motivo, input.justification]);
  const approvedAt = firstNonEmpty([input.approvedAt, input.authorizedAt, input.aprovadoEm]) || new Date().toISOString();
  return {
    approved: Boolean(input.approved),
    approvedBy,
    reason,
    approvedAt,
    principalRole: toNonEmptyString(input.principalRole),
    principalName: toNonEmptyString(input.principalName),
  };
}

function hasValidTestAuthorization(authorization) {
  const normalized = normalizeTestAuthorization(authorization);
  if (!normalized) {
    return false;
  }
  return Boolean(normalized.approved && normalized.approvedBy && normalized.reason);
}

function buildTestAuthorizationBlock({ signal }) {
  return {
    status: "blocked_requires_admin_authorization",
    message:
      "Detectei tentativa de agendamento em modo teste. Para proteger a agenda real, so permito teste com autorizacao explicita de admin.",
    required: {
      marker: TEST_BOOKING_MARKER || "#TESTE",
      testAuthorization: {
        approved: true,
        approvedBy: "nome do admin responsavel",
        reason: "motivo e escopo do teste",
      },
      credential:
        "Enviar X-Admin-Token valido (ou sessao de tenant autenticada) junto da requisicao de teste.",
    },
    signal: signal || null,
  };
}

function detectDirectBookingTestSignal(body = {}) {
  const explicitTestMode = Boolean(body?.testMode || body?.isTest || body?.mode === "test");
  const internalTester = isInternalTestPhone(body?.clientPhone);
  const probeText = [
    body?.message,
    body?.notes,
    body?.observacoes,
    body?.observation,
    body?.clientName,
    body?.service,
  ]
    .map((item) => toNonEmptyString(item))
    .filter(Boolean)
    .join(" ");
  return detectTestSignal({
    message: probeText,
    internalTester,
    explicitTestMode,
  });
}

function marketingUploadMimeToExtension(mime) {
  const normalized = toNonEmptyString(mime).toLowerCase();
  if (normalized === "image/jpeg" || normalized === "image/jpg") {
    return "jpg";
  }
  if (normalized === "image/png") {
    return "png";
  }
  if (normalized === "image/webp") {
    return "webp";
  }
  if (normalized === "image/gif") {
    return "gif";
  }
  return "";
}

function parseImageDataUrl(dataUrl) {
  const raw = toNonEmptyString(dataUrl);
  if (!raw) {
    return null;
  }

  const match = raw.match(/^data:(image\/(?:png|jpe?g|webp|gif));base64,([A-Za-z0-9+/=\s]+)$/i);
  if (!match) {
    return null;
  }

  return {
    mime: toNonEmptyString(match[1]).toLowerCase(),
    base64: String(match[2] || "").replace(/\s+/g, ""),
  };
}

function sanitizeUploadFileStem(value) {
  const raw = toNonEmptyString(value).replace(/\.[A-Za-z0-9]+$/, "");
  const normalized = raw
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Za-z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 72);
  return normalized || "imagem";
}

function getPublicBackendBaseUrl(req) {
  const envBase = toNonEmptyString(
    process.env.BACKEND_PUBLIC_BASE_URL
    || process.env.PUBLIC_BACKEND_URL
    || process.env.WEBHOOK_PUBLIC_BASE_URL,
  );
  if (envBase) {
    return envBase.replace(/\/+$/g, "");
  }

  const forwardedProto = toNonEmptyString(req?.headers?.["x-forwarded-proto"]).split(",")[0];
  const forwardedHost = toNonEmptyString(req?.headers?.["x-forwarded-host"]).split(",")[0];
  const host = forwardedHost || toNonEmptyString(req?.headers?.host);
  const protocol = forwardedProto || "https";

  if (!host) {
    return "";
  }
  return `${protocol}://${host}`;
}

function getConfiguredAdminToken() {
  return String(process.env.KNOWLEDGE_ADMIN_TOKEN || "").trim();
}

function getAdminTokenFromRequest(req) {
  return toNonEmptyString(req?.headers?.["x-admin-token"] || "");
}

function normalizeTenantUsername(value) {
  const normalized = toNonEmptyString(value)
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, "");
  return normalized.slice(0, 64);
}

function hashSha256(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex");
}

function hashTenantPassword(password, salt = "") {
  const normalizedPassword = String(password || "");
  const effectiveSalt = toNonEmptyString(salt) || crypto.randomBytes(16).toString("hex");
  const digest = crypto.scryptSync(normalizedPassword, effectiveSalt, 64).toString("hex");
  return `${effectiveSalt}:${digest}`;
}

function verifyTenantPassword(password, storedHash) {
  const rawHash = toNonEmptyString(storedHash);
  const [salt, expectedHex] = rawHash.split(":");
  if (!salt || !expectedHex) {
    return false;
  }

  try {
    const expected = Buffer.from(expectedHex, "hex");
    const actual = crypto.scryptSync(String(password || ""), salt, 64);
    if (!expected.length || expected.length !== actual.length) {
      return false;
    }
    return crypto.timingSafeEqual(expected, actual);
  } catch {
    return false;
  }
}

function cleanupExpiredTenantUserSessions() {
  db.prepare(
    `
      DELETE FROM tenant_user_sessions
      WHERE datetime(expires_at) <= datetime('now')
    `,
  ).run();
}

function isIsoDateExpired(isoDate) {
  const timestamp = Date.parse(String(isoDate || ""));
  if (!Number.isFinite(timestamp)) {
    return true;
  }
  return timestamp <= Date.now();
}

function mapTenantUserRow(row) {
  if (!row) {
    return null;
  }

  return {
    id: Number(row.id),
    tenantId: Number(row.tenantId),
    tenantCode: toNonEmptyString(row.tenantCode),
    tenantName: toNonEmptyString(row.tenantName),
    username: toNonEmptyString(row.username),
    displayName: toNonEmptyString(row.displayName),
    active: Number(row.active) !== 0,
    lastLoginAt: toNonEmptyString(row.lastLoginAt),
    createdAt: toNonEmptyString(row.createdAt),
    updatedAt: toNonEmptyString(row.updatedAt),
  };
}

function listTenantUsersByCode(code) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return [];
  }

  const rows = db.prepare(
    `
      SELECT u.id,
             u.tenant_id AS tenantId,
             t.code AS tenantCode,
             t.name AS tenantName,
             u.username,
             u.display_name AS displayName,
             u.active,
             u.last_login_at AS lastLoginAt,
             u.created_at AS createdAt,
             u.updated_at AS updatedAt
      FROM tenant_users u
      JOIN tenants t ON t.id = u.tenant_id
      WHERE t.id = ?
      ORDER BY u.username COLLATE NOCASE ASC, u.id ASC
    `,
  ).all(tenant.id);

  return rows.map(mapTenantUserRow).filter(Boolean);
}

function createTenantUserByCode(
  code,
  { username = "", displayName = "", password = "", active = true } = {},
) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const normalizedUsername = normalizeTenantUsername(username);
  if (!normalizedUsername) {
    const error = new Error("Campo obrigatorio: username");
    error.status = 400;
    throw error;
  }

  const rawPassword = String(password || "");
  if (rawPassword.length < TENANT_MIN_PASSWORD_LENGTH) {
    const error = new Error(`Senha deve ter pelo menos ${TENANT_MIN_PASSWORD_LENGTH} caracteres.`);
    error.status = 400;
    throw error;
  }

  const now = new Date().toISOString();
  const passwordHash = hashTenantPassword(rawPassword);

  db.prepare(
    `
      INSERT INTO tenant_users (
        tenant_id, username, display_name, password_hash, active, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `,
  ).run(
    tenant.id,
    normalizedUsername,
    toNonEmptyString(displayName),
    passwordHash,
    active ? 1 : 0,
    now,
    now,
  );

  return listTenantUsersByCode(tenant.code);
}

function updateTenantUserByCodeAndId(
  code,
  userId,
  { displayName, password, active } = {},
) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const parsedUserId = Number(userId);
  if (!Number.isFinite(parsedUserId) || parsedUserId <= 0) {
    const error = new Error("userId invalido.");
    error.status = 400;
    throw error;
  }

  const current = db.prepare(
    `
      SELECT id,
             tenant_id AS tenantId,
             username,
             display_name AS displayName,
             active
      FROM tenant_users
      WHERE id = ?
        AND tenant_id = ?
      LIMIT 1
    `,
  ).get(parsedUserId, tenant.id);

  if (!current) {
    const error = new Error("Usuario do tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const nextDisplayName = displayName == null ? current.displayName : toNonEmptyString(displayName);
  const nextActive = active == null ? (Number(current.active) !== 0) : Boolean(active);
  const now = new Date().toISOString();

  let nextPasswordHash = null;
  if (password != null) {
    const rawPassword = String(password || "");
    if (rawPassword.length < TENANT_MIN_PASSWORD_LENGTH) {
      const error = new Error(`Senha deve ter pelo menos ${TENANT_MIN_PASSWORD_LENGTH} caracteres.`);
      error.status = 400;
      throw error;
    }
    nextPasswordHash = hashTenantPassword(rawPassword);
  }

  db.prepare(
    `
      UPDATE tenant_users
      SET display_name = ?,
          active = ?,
          password_hash = CASE WHEN ? IS NULL THEN password_hash ELSE ? END,
          updated_at = ?
      WHERE id = ?
        AND tenant_id = ?
    `,
  ).run(
    nextDisplayName,
    nextActive ? 1 : 0,
    nextPasswordHash,
    nextPasswordHash,
    now,
    parsedUserId,
    tenant.id,
  );

  if (!nextActive) {
    db.prepare(
      `
        DELETE FROM tenant_user_sessions
        WHERE tenant_user_id = ?
      `,
    ).run(parsedUserId);
  }

  return listTenantUsersByCode(tenant.code);
}

function findTenantUserForLogin({ tenantCode = "", username = "" } = {}) {
  const normalizedTenantCode = normalizeTenantCode(tenantCode);
  const normalizedUsername = normalizeTenantUsername(username);
  if (!normalizedTenantCode || !normalizedUsername) {
    return null;
  }

  const row = db.prepare(
    `
      SELECT u.id,
             u.tenant_id AS tenantId,
             t.code AS tenantCode,
             t.name AS tenantName,
             t.active AS tenantActive,
             u.username,
             u.display_name AS displayName,
             u.password_hash AS passwordHash,
             u.active AS userActive
      FROM tenant_users u
      JOIN tenants t ON t.id = u.tenant_id
      WHERE t.code = ?
        AND u.username = ?
      LIMIT 1
    `,
  ).get(normalizedTenantCode, normalizedUsername);

  if (!row) {
    return null;
  }

  return {
    id: Number(row.id),
    tenantId: Number(row.tenantId),
    tenantCode: toNonEmptyString(row.tenantCode),
    tenantName: toNonEmptyString(row.tenantName),
    tenantActive: Number(row.tenantActive) !== 0,
    username: toNonEmptyString(row.username),
    displayName: toNonEmptyString(row.displayName),
    passwordHash: toNonEmptyString(row.passwordHash),
    userActive: Number(row.userActive) !== 0,
  };
}

function createTenantUserSession({ tenantUserId }) {
  const parsedUserId = Number(tenantUserId);
  if (!Number.isFinite(parsedUserId) || parsedUserId <= 0) {
    const error = new Error("tenantUserId invalido.");
    error.status = 400;
    throw error;
  }

  cleanupExpiredTenantUserSessions();

  const token = crypto.randomBytes(48).toString("hex");
  const tokenHash = hashSha256(token);
  const nowDate = new Date();
  const nowIso = nowDate.toISOString();
  const expiresAt = new Date(nowDate.getTime() + TENANT_SESSION_TTL_HOURS * 60 * 60 * 1000).toISOString();

  db.prepare(
    `
      INSERT INTO tenant_user_sessions (
        tenant_user_id, token_hash, expires_at, created_at, updated_at, last_seen_at
      )
      VALUES (?, ?, ?, ?, ?, ?)
    `,
  ).run(parsedUserId, tokenHash, expiresAt, nowIso, nowIso, nowIso);

  return {
    token,
    expiresAt,
  };
}

function revokeTenantSessionByToken(token) {
  const normalized = toNonEmptyString(token);
  if (!normalized) {
    return;
  }
  db.prepare(
    `
      DELETE FROM tenant_user_sessions
      WHERE token_hash = ?
    `,
  ).run(hashSha256(normalized));
}

function revokeTenantSessionsByUserId(userId) {
  const parsedUserId = Number(userId);
  if (!Number.isFinite(parsedUserId) || parsedUserId <= 0) {
    return;
  }
  db.prepare(
    `
      DELETE FROM tenant_user_sessions
      WHERE tenant_user_id = ?
    `,
  ).run(parsedUserId);
}

function resolveTenantPrincipalByToken(token) {
  const normalizedToken = toNonEmptyString(token);
  if (!normalizedToken) {
    return null;
  }

  cleanupExpiredTenantUserSessions();

  const row = db.prepare(
    `
      SELECT s.id AS sessionId,
             s.tenant_user_id AS tenantUserId,
             s.expires_at AS expiresAt,
             u.username,
             u.display_name AS displayName,
             u.active AS userActive,
             t.id AS tenantId,
             t.code AS tenantCode,
             t.name AS tenantName,
             t.active AS tenantActive
      FROM tenant_user_sessions s
      JOIN tenant_users u ON u.id = s.tenant_user_id
      JOIN tenants t ON t.id = u.tenant_id
      WHERE s.token_hash = ?
      LIMIT 1
    `,
  ).get(hashSha256(normalizedToken));

  if (!row) {
    return null;
  }

  if (
    isIsoDateExpired(row.expiresAt)
    || Number(row.userActive) === 0
    || Number(row.tenantActive) === 0
  ) {
    revokeTenantSessionByToken(normalizedToken);
    return null;
  }

  db.prepare(
    `
      UPDATE tenant_user_sessions
      SET last_seen_at = ?,
          updated_at = ?
      WHERE id = ?
    `,
  ).run(new Date().toISOString(), new Date().toISOString(), Number(row.sessionId));

  return {
    role: "tenant",
    tokenType: "tenant_session",
    sessionId: Number(row.sessionId),
    tenantUserId: Number(row.tenantUserId),
    tenantId: Number(row.tenantId),
    tenantCode: toNonEmptyString(row.tenantCode),
    tenantName: toNonEmptyString(row.tenantName),
    username: toNonEmptyString(row.username),
    displayName: toNonEmptyString(row.displayName),
    expiresAt: toNonEmptyString(row.expiresAt),
  };
}

function resolveAdminPrincipal(req) {
  const configuredAdminToken = getConfiguredAdminToken();
  const providedToken = getAdminTokenFromRequest(req);

  if (!configuredAdminToken) {
    if (!providedToken) {
      return {
        role: "superadmin",
        tokenType: "open_mode",
      };
    }
    return {
      role: "superadmin",
      tokenType: "x_admin_token",
    };
  }

  if (providedToken && providedToken === configuredAdminToken) {
    return {
      role: "superadmin",
      tokenType: "x_admin_token",
    };
  }

  if (!providedToken) {
    return null;
  }

  return resolveTenantPrincipalByToken(providedToken);
}

function requireAdminPrincipal(req, res) {
  const principal = resolveAdminPrincipal(req);
  if (!principal) {
    res.status(401).json({ message: "Nao autorizado." });
    return null;
  }
  return principal;
}

function resolveTrustedTestAuthorizationFromRequest(req, candidate) {
  const normalized = normalizeTestAuthorization(candidate);
  if (!normalized) {
    return null;
  }

  const principal = resolveAdminPrincipal(req);
  if (!principal) {
    return {
      ...normalized,
      approved: false,
      principalRole: "",
      principalName: "",
    };
  }

  const configuredAdminToken = getConfiguredAdminToken();
  const explicitToken = firstNonEmpty([
    getAdminTokenFromRequest(req),
    candidate?.adminToken,
    candidate?.token,
  ]);
  const hasTrustedCredential = configuredAdminToken
    ? explicitToken === configuredAdminToken || principal.role === "tenant"
    : Boolean(explicitToken) || principal.role === "tenant";

  const principalName = firstNonEmpty([
    principal.displayName,
    principal.username,
    normalized.approvedBy,
    principal.role,
  ]);

  return {
    ...normalized,
    approved: Boolean(normalized.approved && hasTrustedCredential),
    approvedBy: normalized.approvedBy || principalName,
    principalRole: toNonEmptyString(principal.role),
    principalName,
  };
}

function ensureSuperAdminPrincipal(principal) {
  if (!principal || principal.role !== "superadmin") {
    const error = new Error("Somente administrador global pode executar esta acao.");
    error.status = 403;
    throw error;
  }
}

function principalCanAccessTenant(principal, tenantCode) {
  if (!principal) {
    return false;
  }
  if (principal.role === "superadmin") {
    return true;
  }
  return normalizeTenantCode(principal.tenantCode) === normalizeTenantCode(tenantCode);
}

function isAdminWriteAuthorized(req) {
  return Boolean(resolveAdminPrincipal(req));
}

function normalizeTenantCode(value) {
  const base = toNonEmptyString(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return base.slice(0, 64);
}

function normalizeTenantIdentifierKind(value) {
  const kind = toNonEmptyString(value)
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  if (!kind) {
    return "";
  }
  return kind;
}

function normalizeTenantIdentifierValue(kind, value) {
  const normalizedKind = normalizeTenantIdentifierKind(kind);
  const raw = toNonEmptyString(value);
  if (!raw || !normalizedKind) {
    return "";
  }

  if (normalizedKind === "evolution_number") {
    return normalizePhone(raw);
  }

  if (normalizedKind === "domain") {
    return raw
      .replace(/^https?:\/\//i, "")
      .split("/")[0]
      .toLowerCase();
  }

  if (normalizedKind === "api_key") {
    return raw;
  }

  return raw.toLowerCase();
}

function isSupportedTenantIdentifierKind(kind) {
  const normalized = normalizeTenantIdentifierKind(kind);
  return [
    "evolution_instance",
    "evolution_number",
    "domain",
    "api_key",
    "custom",
  ].includes(normalized);
}

function parseJsonObjectLoose(value, fallback = {}) {
  const parsed = tryParseJsonLoose(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return fallback;
  }
  return parsed;
}

function getActiveTenantCount() {
  try {
    const row = db.prepare("SELECT COUNT(1) AS total FROM tenants WHERE active = 1").get();
    const parsed = Number(row?.total);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
  } catch {
    return 0;
  }
}

function isMultiTenantModeActive() {
  return getActiveTenantCount() > 1;
}

function resolveConversationTenantFallbackEstablishmentId() {
  const configured = getConfiguredEstablishmentId();
  if (!configured) {
    return null;
  }
  return isMultiTenantModeActive() ? null : configured;
}

function mapTenantRow(row) {
  if (!row) {
    return null;
  }
  return {
    id: Number(row.id),
    code: toNonEmptyString(row.code),
    name: toNonEmptyString(row.name),
    segment: toNonEmptyString(row.segment),
    active: Number(row.active) !== 0,
    defaultProvider: resolveSchedulingProvider(row.defaultProvider),
    establishmentId: Number.isFinite(Number(row.establishmentId)) ? Number(row.establishmentId) : null,
    knowledge: parseJsonObjectLoose(row.knowledgeJson, {}),
    createdAt: toNonEmptyString(row.createdAt),
    updatedAt: toNonEmptyString(row.updatedAt),
  };
}

function listTenants({ includeInactive = false } = {}) {
  const rows = includeInactive
    ? db.prepare(
      `
        SELECT id, code, name, segment, active, default_provider AS defaultProvider,
               establishment_id AS establishmentId, knowledge_json AS knowledgeJson,
               created_at AS createdAt, updated_at AS updatedAt
        FROM tenants
        ORDER BY name COLLATE NOCASE ASC, id ASC
      `,
    ).all()
    : db.prepare(
      `
        SELECT id, code, name, segment, active, default_provider AS defaultProvider,
               establishment_id AS establishmentId, knowledge_json AS knowledgeJson,
               created_at AS createdAt, updated_at AS updatedAt
        FROM tenants
        WHERE active = 1
        ORDER BY name COLLATE NOCASE ASC, id ASC
      `,
    ).all();

  return rows.map(mapTenantRow);
}

function getTenantByCode(code) {
  const normalizedCode = normalizeTenantCode(code);
  if (!normalizedCode) {
    return null;
  }

  const row = db.prepare(
    `
      SELECT id, code, name, segment, active, default_provider AS defaultProvider,
             establishment_id AS establishmentId, knowledge_json AS knowledgeJson,
             created_at AS createdAt, updated_at AS updatedAt
      FROM tenants
      WHERE code = ?
      LIMIT 1
    `,
  ).get(normalizedCode);

  return mapTenantRow(row);
}

function resolveTenantByIdentifier({ kind, value }) {
  const normalizedKind = normalizeTenantIdentifierKind(kind);
  const normalizedValue = normalizeTenantIdentifierValue(normalizedKind, value);
  if (!normalizedKind || !normalizedValue) {
    return null;
  }

  const row = db.prepare(
    `
      SELECT t.id, t.code, t.name, t.segment, t.active,
             t.default_provider AS defaultProvider,
             t.establishment_id AS establishmentId,
             t.knowledge_json AS knowledgeJson,
             t.created_at AS createdAt,
             t.updated_at AS updatedAt
      FROM tenant_identifiers i
      JOIN tenants t ON t.id = i.tenant_id
      WHERE i.kind = ?
        AND i.normalized_value = ?
      LIMIT 1
    `,
  ).get(normalizedKind, normalizedValue);

  return mapTenantRow(row);
}

function getActiveTenantByEstablishmentId(establishmentId) {
  const parsedId = Number(establishmentId);
  if (!Number.isFinite(parsedId) || parsedId <= 0) {
    return null;
  }

  const row = db.prepare(
    `
      SELECT id, code, name, segment, active, default_provider AS defaultProvider,
             establishment_id AS establishmentId, knowledge_json AS knowledgeJson,
             created_at AS createdAt, updated_at AS updatedAt
      FROM tenants
      WHERE active = 1
        AND establishment_id = ?
      ORDER BY datetime(updated_at) DESC, id DESC
      LIMIT 1
    `,
  ).get(parsedId);

  return mapTenantRow(row);
}

function resolveConversationTenantContext({
  tenantCode = "",
  tenantAlias = "",
  instanceName = "",
  establishmentId = null,
} = {}) {
  const normalizedTenantCode = normalizeTenantCode(tenantCode || tenantAlias);
  let tenant = normalizedTenantCode ? getTenantByCode(normalizedTenantCode) : null;

  if (tenant && !tenant.active) {
    tenant = null;
  }

  const normalizedInstance = toNonEmptyString(instanceName);
  if (!tenant && normalizedInstance) {
    tenant = resolveTenantByIdentifier({
      kind: "evolution_instance",
      value: normalizedInstance,
    });
    if (tenant && !tenant.active) {
      tenant = null;
    }
  }

  const parsedEstablishmentId = Number(establishmentId);
  if (!tenant && Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0) {
    tenant = getActiveTenantByEstablishmentId(parsedEstablishmentId);
  }

  const tenantEstablishmentId = Number(tenant?.establishmentId);
  const hasTenantEstablishmentId = Number.isFinite(tenantEstablishmentId) && tenantEstablishmentId > 0;
  const hasParsedEstablishmentId = Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0;
  const resolvedEstablishmentId = hasTenantEstablishmentId
    ? tenantEstablishmentId
    : (hasParsedEstablishmentId ? parsedEstablishmentId : null);

  const tenantKnowledge = tenant?.knowledge && typeof tenant.knowledge === "object" && !Array.isArray(tenant.knowledge)
    ? tenant.knowledge
    : null;

  return {
    tenant,
    tenantCode: toNonEmptyString(tenant?.code) || normalizedTenantCode || "",
    establishmentId: resolvedEstablishmentId,
    knowledge: tenantKnowledge,
  };
}

function getTenantIdentifiersByCode(code) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return [];
  }

  return db.prepare(
    `
      SELECT id, kind, value, normalized_value AS normalizedValue,
             created_at AS createdAt, updated_at AS updatedAt
      FROM tenant_identifiers
      WHERE tenant_id = ?
      ORDER BY kind ASC, id ASC
    `,
  ).all(tenant.id);
}

function getTenantProviderConfigsByCode(code) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return [];
  }

  const rows = db.prepare(
    `
      SELECT id, provider, enabled, config_json AS configJson,
             created_at AS createdAt, updated_at AS updatedAt
      FROM tenant_provider_configs
      WHERE tenant_id = ?
      ORDER BY provider ASC, id ASC
    `,
  ).all(tenant.id);

  return rows.map((row) => ({
    id: Number(row.id),
    provider: resolveSchedulingProvider(row.provider),
    enabled: Number(row.enabled) !== 0,
    config: parseJsonObjectLoose(row.configJson, {}),
    createdAt: toNonEmptyString(row.createdAt),
    updatedAt: toNonEmptyString(row.updatedAt),
  }));
}

function getTenantProviderConfigByCode(code, provider) {
  const normalizedCode = normalizeTenantCode(code);
  if (!normalizedCode) {
    return null;
  }

  const normalizedProvider = resolveSchedulingProvider(provider);
  if (!normalizedProvider) {
    return null;
  }

  return (
    getTenantProviderConfigsByCode(normalizedCode).find(
      (item) => resolveSchedulingProvider(item?.provider) === normalizedProvider,
    ) || null
  );
}

function getTenantIdentifierValueByCode(code, kind) {
  const normalizedCode = normalizeTenantCode(code);
  const normalizedKind = normalizeTenantIdentifierKind(kind);
  if (!normalizedCode || !normalizedKind) {
    return "";
  }

  const match = getTenantIdentifiersByCode(normalizedCode).find(
    (item) => normalizeTenantIdentifierKind(item?.kind) === normalizedKind,
  );
  return toNonEmptyString(match?.value);
}

function readProviderConfigValue(config, candidates = []) {
  const source = config && typeof config === "object" && !Array.isArray(config) ? config : {};

  for (const candidate of candidates) {
    const pathSegments = String(candidate || "")
      .split(".")
      .map((segment) => segment.trim())
      .filter(Boolean);
    if (!pathSegments.length) {
      continue;
    }

    let current = source;
    let found = true;
    for (const segment of pathSegments) {
      if (!current || typeof current !== "object" || Array.isArray(current) || !(segment in current)) {
        found = false;
        break;
      }
      current = current[segment];
    }

    const resolved = toNonEmptyString(current);
    if (found && resolved) {
      return resolved;
    }
  }

  return "";
}

function resolveTrinksRuntimeConfig({ tenantCode = "", establishmentId = null } = {}) {
  const context = resolveConversationTenantContext({
    tenantCode,
    establishmentId,
  });
  const resolvedTenantCode = normalizeTenantScopeCode(context.tenantCode || tenantCode);
  const providerConfig = resolvedTenantCode
    ? getTenantProviderConfigByCode(resolvedTenantCode, SCHEDULING_PROVIDER_TRINKS)
    : null;
  const config = providerConfig?.config && typeof providerConfig.config === "object" && !Array.isArray(providerConfig.config)
    ? providerConfig.config
    : {};

  const baseUrl = firstNonEmpty([
    readProviderConfigValue(config, [
      "baseUrl",
      "apiBaseUrl",
      "api_base_url",
      "trinksBaseUrl",
      "trinksApiBaseUrl",
      "credentials.baseUrl",
    ]),
    process.env.TRINKS_API_BASE_URL,
  ]);
  if (!baseUrl) {
    throw new Error("Variavel obrigatoria ausente: TRINKS_API_BASE_URL.");
  }

  const apiKey = firstNonEmpty([
    readProviderConfigValue(config, [
      "apiKey",
      "api_key",
      "trinksApiKey",
      "trinks_api_key",
      "xApiKey",
      "credentials.apiKey",
      "credentials.api_key",
    ]),
    process.env.TRINKS_API_KEY,
  ]);
  if (!apiKey) {
    throw new Error("Variavel obrigatoria ausente: TRINKS_API_KEY.");
  }

  return {
    tenantCode: resolvedTenantCode,
    baseUrl: String(baseUrl).replace(/\/$/, ""),
    apiKey,
    source: providerConfig ? "tenant" : "env",
  };
}

function inferEvolutionInstanceFromPath(path = "") {
  const normalizedPath = toNonEmptyString(path);
  if (!normalizedPath) {
    return "";
  }

  const queryMatch = normalizedPath.match(/[?&]instance=([^&]+)/i);
  if (queryMatch?.[1]) {
    try {
      return decodeURIComponent(queryMatch[1]);
    } catch {
      return queryMatch[1];
    }
  }

  const pathMatch = normalizedPath.match(
    /\/(?:message\/sendText|webhook\/set|webhook\/find|instance\/(?:connect|qrcode|qr|logout|disconnect|close|create))\/([^/?]+)/i,
  );
  if (pathMatch?.[1]) {
    try {
      return decodeURIComponent(pathMatch[1]);
    } catch {
      return pathMatch[1];
    }
  }

  return "";
}

function resolveEvolutionRuntimeConfig({ tenantCode = "", instanceName = "" } = {}) {
  let resolvedTenantCode = normalizeTenantScopeCode(tenantCode);
  if (!resolvedTenantCode && toNonEmptyString(instanceName)) {
    const context = resolveConversationTenantContext({ instanceName });
    resolvedTenantCode = normalizeTenantScopeCode(context.tenantCode);
  }
  const providerConfig = resolvedTenantCode
    ? getTenantProviderConfigByCode(resolvedTenantCode, "evolution")
    : null;
  const config = providerConfig?.config && typeof providerConfig.config === "object" && !Array.isArray(providerConfig.config)
    ? providerConfig.config
    : {};

  const baseUrl = firstNonEmpty([
    readProviderConfigValue(config, [
      "baseUrl",
      "apiBaseUrl",
      "api_base_url",
      "url",
      "evolutionBaseUrl",
      "evolutionApiBaseUrl",
      "credentials.baseUrl",
    ]),
    process.env.EVOLUTION_API_BASE_URL,
    process.env.EVOLUTION_URL,
    process.env._EVOLUTION_URL,
  ]);
  if (!baseUrl) {
    throw new Error(
      "Variavel obrigatoria ausente: EVOLUTION_API_BASE_URL (ou EVOLUTION_URL/_EVOLUTION_URL).",
    );
  }

  const apiKey = firstNonEmpty([
    readProviderConfigValue(config, [
      "apiKey",
      "api_key",
      "apikey",
      "evolutionApiKey",
      "evolution_api_key",
      "credentials.apiKey",
      "credentials.api_key",
      "credentials.apikey",
    ]),
    process.env.EVOLUTION_API_KEY,
  ]);
  if (!apiKey) {
    throw new Error("Variavel obrigatoria ausente: EVOLUTION_API_KEY.");
  }

  const resolvedInstance = firstNonEmpty([
    instanceName,
    readProviderConfigValue(config, [
      "instance",
      "instanceName",
      "evolutionInstance",
      "credentials.instance",
      "credentials.instanceName",
    ]),
    getTenantIdentifierValueByCode(resolvedTenantCode, "evolution_instance"),
    process.env.EVOLUTION_INSTANCE,
  ]);

  return {
    tenantCode: resolvedTenantCode,
    baseUrl: String(baseUrl).replace(/\/$/, ""),
    apiKey,
    instance: resolvedInstance,
    source: providerConfig ? "tenant" : "env",
  };
}

function createTenant({
  code = "",
  name = "",
  segment = "",
  active = true,
  defaultProvider = "",
  establishmentId = null,
  knowledge = {},
}) {
  const normalizedName = toNonEmptyString(name);
  if (!normalizedName) {
    const error = new Error("Campo obrigatorio: name");
    error.status = 400;
    throw error;
  }

  const normalizedCode = normalizeTenantCode(code || normalizedName);
  if (!normalizedCode) {
    const error = new Error("Campo obrigatorio: code");
    error.status = 400;
    throw error;
  }

  const provider = resolveSchedulingProvider(defaultProvider);
  const parsedEstablishmentId = Number(establishmentId);
  const now = new Date().toISOString();

  db.prepare(
    `
      INSERT INTO tenants (
        code, name, segment, active, default_provider, establishment_id,
        knowledge_json, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
  ).run(
    normalizedCode,
    normalizedName,
    toNonEmptyString(segment),
    active ? 1 : 0,
    provider,
    Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0 ? parsedEstablishmentId : null,
    safeJsonStringify(knowledge || {}),
    now,
    now,
  );

  return getTenantByCode(normalizedCode);
}

function updateTenantByCode(
  code,
  {
    name,
    segment,
    active,
    defaultProvider,
    establishmentId,
    knowledge,
  } = {},
) {
  const current = getTenantByCode(code);
  if (!current) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const nextName = name == null ? current.name : toNonEmptyString(name);
  if (!nextName) {
    const error = new Error("Campo invalido: name");
    error.status = 400;
    throw error;
  }

  const nextProvider = defaultProvider == null
    ? current.defaultProvider
    : resolveSchedulingProvider(defaultProvider);
  const now = new Date().toISOString();

  const parsedEstablishmentId = establishmentId == null
    ? current.establishmentId
    : Number(establishmentId);

  const nextKnowledge = knowledge == null ? current.knowledge : knowledge;
  db.prepare(
    `
      UPDATE tenants
      SET name = ?,
          segment = ?,
          active = ?,
          default_provider = ?,
          establishment_id = ?,
          knowledge_json = ?,
          updated_at = ?
      WHERE code = ?
    `,
  ).run(
    nextName,
    segment == null ? current.segment : toNonEmptyString(segment),
    active == null ? (current.active ? 1 : 0) : (Boolean(active) ? 1 : 0),
    nextProvider,
    Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0 ? parsedEstablishmentId : null,
    safeJsonStringify(nextKnowledge || {}),
    now,
    current.code,
  );

  return getTenantByCode(current.code);
}

function upsertTenantIdentifierByCode(code, { kind, value }) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const normalizedKind = normalizeTenantIdentifierKind(kind);
  const normalizedValue = normalizeTenantIdentifierValue(normalizedKind, value);
  if (!isSupportedTenantIdentifierKind(normalizedKind)) {
    const error = new Error("kind de identificador nao suportado.");
    error.status = 400;
    error.details = {
      supportedKinds: ["evolution_instance", "evolution_number", "domain", "api_key", "custom"],
    };
    throw error;
  }

  if (!normalizedValue) {
    const error = new Error("Campo obrigatorio: value");
    error.status = 400;
    throw error;
  }

  const now = new Date().toISOString();
  db.prepare(
    `
      INSERT INTO tenant_identifiers (tenant_id, kind, value, normalized_value, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(kind, normalized_value) DO UPDATE SET
        tenant_id = excluded.tenant_id,
        value = excluded.value,
        updated_at = excluded.updated_at
    `,
  ).run(
    tenant.id,
    normalizedKind,
    toNonEmptyString(value),
    normalizedValue,
    now,
    now,
  );

  return getTenantIdentifiersByCode(tenant.code);
}

function upsertTenantProviderConfigByCode(code, provider, { enabled = true, config = {} } = {}) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const normalizedProvider = resolveSchedulingProvider(provider);
  const now = new Date().toISOString();

  db.prepare(
    `
      INSERT INTO tenant_provider_configs (tenant_id, provider, enabled, config_json, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(tenant_id, provider) DO UPDATE SET
        enabled = excluded.enabled,
        config_json = excluded.config_json,
        updated_at = excluded.updated_at
    `,
  ).run(
    tenant.id,
    normalizedProvider,
    enabled ? 1 : 0,
    safeJsonStringify(config || {}),
    now,
    now,
  );

  return getTenantProviderConfigsByCode(tenant.code);
}

function getDefaultCrmSettings() {
  return {
    crmReturnEnabled: false,
    crmMode: "beta",
    bookingMaxDaysAhead: BOOKING_MAX_DAYS_AHEAD,
    messageSendingWindowStart: "09:00",
    messageSendingWindowEnd: "19:00",
    messageDailyLimit: 20,
    stopFlowOnAnyFutureBooking: true,
    maxSteps: 3,
    humanHandoffEnabled: true,
    humanHandoffClientNumber: "",
    humanHandoffInternalNumber: "",
    humanHandoffMessageTemplate:
      "Se preferir, nosso atendimento humano segue com voce pelo numero {{human_number}}.",
    humanHandoffSendInternalSummary: true,
    humanHandoffPauseAi: true,
    opportunityTrackingEnabled: true,
    allowOnlyWhitelistedPhonesInBeta: false,
    betaTestPhones: [],
  };
}

function normalizeCrmMode(value) {
  const normalized = toNonEmptyString(value).toLowerCase();
  if (["beta", "manual", "automatic"].includes(normalized)) {
    return normalized;
  }
  return "beta";
}

function normalizePriority(value, fallback = "medium") {
  const normalized = toNonEmptyString(value).toLowerCase();
  if (["low", "medium", "high"].includes(normalized)) {
    return normalized;
  }
  return fallback;
}

function normalizeTimeWindowValue(value, fallback = "") {
  const normalized = normalizeTimeValue(value);
  return normalized || fallback;
}

function sanitizeCrmSettings(input = {}) {
  const source = input && typeof input === "object" && !Array.isArray(input) ? input : {};
  const defaults = getDefaultCrmSettings();
  const bookingMaxDaysAhead = Number(source.bookingMaxDaysAhead);
  const messageDailyLimit = Number(source.messageDailyLimit);
  const maxSteps = Number(source.maxSteps);
  const betaPhones = Array.isArray(source.betaTestPhones)
    ? source.betaTestPhones.map((item) => normalizePhone(item)).filter(Boolean)
    : parsePhoneList(source.betaTestPhones || "");

  return {
    ...defaults,
    crmReturnEnabled: source.crmReturnEnabled == null
      ? defaults.crmReturnEnabled
      : Boolean(source.crmReturnEnabled),
    crmMode: normalizeCrmMode(source.crmMode || defaults.crmMode),
    bookingMaxDaysAhead: Number.isFinite(bookingMaxDaysAhead) && bookingMaxDaysAhead > 0
      ? Math.min(365, Math.max(1, Math.trunc(bookingMaxDaysAhead)))
      : defaults.bookingMaxDaysAhead,
    messageSendingWindowStart: normalizeTimeWindowValue(
      source.messageSendingWindowStart,
      defaults.messageSendingWindowStart,
    ),
    messageSendingWindowEnd: normalizeTimeWindowValue(
      source.messageSendingWindowEnd,
      defaults.messageSendingWindowEnd,
    ),
    messageDailyLimit: Number.isFinite(messageDailyLimit) && messageDailyLimit > 0
      ? Math.min(1000, Math.max(1, Math.trunc(messageDailyLimit)))
      : defaults.messageDailyLimit,
    stopFlowOnAnyFutureBooking: source.stopFlowOnAnyFutureBooking == null
      ? defaults.stopFlowOnAnyFutureBooking
      : Boolean(source.stopFlowOnAnyFutureBooking),
    maxSteps: Number.isFinite(maxSteps) && maxSteps > 0
      ? Math.min(3, Math.max(1, Math.trunc(maxSteps)))
      : defaults.maxSteps,
    humanHandoffEnabled: source.humanHandoffEnabled == null
      ? defaults.humanHandoffEnabled
      : Boolean(source.humanHandoffEnabled),
    humanHandoffClientNumber: normalizePhone(source.humanHandoffClientNumber || source.humanNumber || ""),
    humanHandoffInternalNumber: normalizePhone(source.humanHandoffInternalNumber || source.internalNumber || ""),
    humanHandoffMessageTemplate: toNonEmptyString(source.humanHandoffMessageTemplate)
      || defaults.humanHandoffMessageTemplate,
    humanHandoffSendInternalSummary: source.humanHandoffSendInternalSummary == null
      ? defaults.humanHandoffSendInternalSummary
      : Boolean(source.humanHandoffSendInternalSummary),
    humanHandoffPauseAi: source.humanHandoffPauseAi == null
      ? defaults.humanHandoffPauseAi
      : Boolean(source.humanHandoffPauseAi),
    opportunityTrackingEnabled: source.opportunityTrackingEnabled == null
      ? defaults.opportunityTrackingEnabled
      : Boolean(source.opportunityTrackingEnabled),
    allowOnlyWhitelistedPhonesInBeta: source.allowOnlyWhitelistedPhonesInBeta == null
      ? defaults.allowOnlyWhitelistedPhonesInBeta
      : Boolean(source.allowOnlyWhitelistedPhonesInBeta),
    betaTestPhones: [...new Set(betaPhones)],
  };
}

function getTenantCrmSettingsByCode(code) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return null;
  }

  const row = db.prepare(
    `
      SELECT id, config_json AS configJson, created_at AS createdAt, updated_at AS updatedAt
      FROM tenant_crm_settings
      WHERE tenant_id = ?
      LIMIT 1
    `,
  ).get(tenant.id);

  const config = sanitizeCrmSettings(parseJsonObjectLoose(row?.configJson, {}));
  return {
    id: Number(row?.id || 0) || null,
    tenantId: tenant.id,
    tenantCode: tenant.code,
    config,
    createdAt: toNonEmptyString(row?.createdAt),
    updatedAt: toNonEmptyString(row?.updatedAt),
  };
}

function upsertTenantCrmSettingsByCode(code, payload = {}) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const current = getTenantCrmSettingsByCode(tenant.code);
  const nextConfig = sanitizeCrmSettings({
    ...(current?.config || getDefaultCrmSettings()),
    ...(payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {}),
  });
  const now = new Date().toISOString();

  db.prepare(
    `
      INSERT INTO tenant_crm_settings (tenant_id, config_json, created_at, updated_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(tenant_id) DO UPDATE SET
        config_json = excluded.config_json,
        updated_at = excluded.updated_at
    `,
  ).run(
    tenant.id,
    safeJsonStringify(nextConfig),
    current?.createdAt || now,
    now,
  );

  return getTenantCrmSettingsByCode(tenant.code);
}

function sanitizeServiceRuleInput(input = {}) {
  const source = input && typeof input === "object" && !Array.isArray(input) ? input : {};
  const serviceKey = firstNonEmpty([
    toNonEmptyString(source.serviceKey),
    toNonEmptyString(source.serviceId),
    toNonEmptyString(source.id),
  ]);
  const serviceName = firstNonEmpty([
    toNonEmptyString(source.serviceName),
    toNonEmptyString(source.name),
  ]);
  const categoryKey = firstNonEmpty([
    toNonEmptyString(source.categoryKey),
    toNonEmptyString(source.categoryId),
    toNonEmptyString(source.categoryName),
  ]);
  const categoryName = firstNonEmpty([
    toNonEmptyString(source.categoryName),
    toNonEmptyString(source.category),
  ]);
  const returnDays = Number(source.returnDays);
  const step1DelayDays = Number(source.step1DelayDays);
  const step2DelayDays = Number(source.step2DelayDays);
  const step3DelayDays = Number(source.step3DelayDays);
  const serviceNameAliases = toNonEmptyString(source.serviceNameAliases || source.service_name_aliases);

  return {
    serviceKey,
    serviceName,
    categoryKey,
    categoryName,
    active: Boolean(source.active),
    returnDays: Number.isFinite(returnDays) && returnDays > 0 ? Math.min(365, Math.trunc(returnDays)) : null,
    useDefaultFlow: source.useDefaultFlow == null ? true : Boolean(source.useDefaultFlow),
    step1DelayDays: Number.isFinite(step1DelayDays) && step1DelayDays >= 0 ? Math.min(365, Math.trunc(step1DelayDays)) : null,
    step1MessageTemplate: toNonEmptyString(source.step1MessageTemplate),
    step2DelayDays: Number.isFinite(step2DelayDays) && step2DelayDays >= 0 ? Math.min(365, Math.trunc(step2DelayDays)) : null,
    step2MessageTemplate: toNonEmptyString(source.step2MessageTemplate),
    step3DelayDays: Number.isFinite(step3DelayDays) && step3DelayDays >= 0 ? Math.min(365, Math.trunc(step3DelayDays)) : null,
    step3MessageTemplate: toNonEmptyString(source.step3MessageTemplate),
    priority: normalizePriority(source.priority, "medium"),
    serviceNameAliases,
    notes: toNonEmptyString(source.notes),
  };
}

function listTenantServiceReturnRulesByCode(code) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return [];
  }

  return db.prepare(
    `
      SELECT id, service_key AS serviceKey, service_name AS serviceName,
             category_key AS categoryKey, category_name AS categoryName,
             active, return_days AS returnDays, use_default_flow AS useDefaultFlow,
             step1_delay_days AS step1DelayDays, step1_message_template AS step1MessageTemplate,
             step2_delay_days AS step2DelayDays, step2_message_template AS step2MessageTemplate,
             step3_delay_days AS step3DelayDays, step3_message_template AS step3MessageTemplate,
              priority, notes, service_name_aliases AS serviceNameAliases,
              created_at AS createdAt, updated_at AS updatedAt
      FROM tenant_service_return_rules
      WHERE tenant_id = ?
      ORDER BY category_name COLLATE NOCASE ASC, service_name COLLATE NOCASE ASC, id ASC
    `,
  ).all(tenant.id).map((row) => ({
    id: Number(row.id),
    serviceKey: toNonEmptyString(row.serviceKey),
    serviceName: toNonEmptyString(row.serviceName),
    categoryKey: toNonEmptyString(row.categoryKey),
    categoryName: toNonEmptyString(row.categoryName),
    active: Number(row.active) !== 0,
    returnDays: Number.isFinite(Number(row.returnDays)) ? Number(row.returnDays) : null,
    useDefaultFlow: Number(row.useDefaultFlow) !== 0,
    step1DelayDays: Number.isFinite(Number(row.step1DelayDays)) ? Number(row.step1DelayDays) : null,
    step1MessageTemplate: toNonEmptyString(row.step1MessageTemplate),
    step2DelayDays: Number.isFinite(Number(row.step2DelayDays)) ? Number(row.step2DelayDays) : null,
    step2MessageTemplate: toNonEmptyString(row.step2MessageTemplate),
    step3DelayDays: Number.isFinite(Number(row.step3DelayDays)) ? Number(row.step3DelayDays) : null,
    step3MessageTemplate: toNonEmptyString(row.step3MessageTemplate),
    priority: normalizePriority(row.priority, "medium"),
    serviceNameAliases: toNonEmptyString(row.serviceNameAliases),
    notes: toNonEmptyString(row.notes),
    createdAt: toNonEmptyString(row.createdAt),
    updatedAt: toNonEmptyString(row.updatedAt),
  }));
}

function upsertTenantServiceReturnRulesByCode(code, rules = []) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const list = Array.isArray(rules) ? rules : [];
  const now = new Date().toISOString();
  const stmt = db.prepare(
    `
      INSERT INTO tenant_service_return_rules (
        tenant_id, service_key, service_name, category_key, category_name,
        active, return_days, use_default_flow,
        step1_delay_days, step1_message_template,
        step2_delay_days, step2_message_template,
        step3_delay_days, step3_message_template,
        priority, service_name_aliases, notes, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(tenant_id, service_key) DO UPDATE SET
        service_name = excluded.service_name,
        category_key = excluded.category_key,
        category_name = excluded.category_name,
        active = excluded.active,
        return_days = excluded.return_days,
        use_default_flow = excluded.use_default_flow,
        step1_delay_days = excluded.step1_delay_days,
        step1_message_template = excluded.step1_message_template,
        step2_delay_days = excluded.step2_delay_days,
        step2_message_template = excluded.step2_message_template,
        step3_delay_days = excluded.step3_delay_days,
        step3_message_template = excluded.step3_message_template,
        priority = excluded.priority,
        service_name_aliases = excluded.service_name_aliases,
        notes = excluded.notes,
        updated_at = excluded.updated_at
    `,
  );

  const transaction = db.transaction((items) => {
    for (const item of items) {
      const sanitized = sanitizeServiceRuleInput(item);
      if (!sanitized.serviceKey || !sanitized.serviceName) {
        continue;
      }
      stmt.run(
        tenant.id,
        sanitized.serviceKey,
        sanitized.serviceName,
        sanitized.categoryKey,
        sanitized.categoryName,
        sanitized.active ? 1 : 0,
        sanitized.returnDays,
        sanitized.useDefaultFlow ? 1 : 0,
        sanitized.step1DelayDays,
        sanitized.step1MessageTemplate,
        sanitized.step2DelayDays,
        sanitized.step2MessageTemplate,
        sanitized.step3DelayDays,
        sanitized.step3MessageTemplate,
        sanitized.priority,
        sanitized.serviceNameAliases,
        sanitized.notes,
        now,
        now,
      );
    }
  });

  transaction(list);
  return listTenantServiceReturnRulesByCode(tenant.code);
}

function sanitizeCategoryOpportunityRuleInput(input = {}) {
  const source = input && typeof input === "object" && !Array.isArray(input) ? input : {};
  const categoryKey = firstNonEmpty([
    toNonEmptyString(source.categoryKey),
    toNonEmptyString(source.categoryId),
    toNonEmptyString(source.categoryName),
  ]);
  const categoryName = firstNonEmpty([
    toNonEmptyString(source.categoryName),
    toNonEmptyString(source.name),
  ]);
  const opportunityDaysWithoutReturn = Number(source.opportunityDaysWithoutReturn);
  return {
    categoryKey,
    categoryName,
    opportunityTrackingEnabled: source.opportunityTrackingEnabled == null
      ? true
      : Boolean(source.opportunityTrackingEnabled),
    opportunityDaysWithoutReturn:
      Number.isFinite(opportunityDaysWithoutReturn) && opportunityDaysWithoutReturn > 0
        ? Math.min(365, Math.trunc(opportunityDaysWithoutReturn))
        : null,
    opportunityPriority: normalizePriority(source.opportunityPriority, "medium"),
    allowManualCampaign: source.allowManualCampaign == null ? true : Boolean(source.allowManualCampaign),
    suggestedMessageTemplate: toNonEmptyString(source.suggestedMessageTemplate),
    notes: toNonEmptyString(source.notes),
  };
}

function listTenantCategoryOpportunityRulesByCode(code) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return [];
  }

  return db.prepare(
    `
      SELECT id, category_key AS categoryKey, category_name AS categoryName,
             opportunity_tracking_enabled AS opportunityTrackingEnabled,
             opportunity_days_without_return AS opportunityDaysWithoutReturn,
             opportunity_priority AS opportunityPriority,
             allow_manual_campaign AS allowManualCampaign,
             suggested_message_template AS suggestedMessageTemplate,
             notes, created_at AS createdAt, updated_at AS updatedAt
      FROM tenant_category_opportunity_rules
      WHERE tenant_id = ?
      ORDER BY category_name COLLATE NOCASE ASC, id ASC
    `,
  ).all(tenant.id).map((row) => ({
    id: Number(row.id),
    categoryKey: toNonEmptyString(row.categoryKey),
    categoryName: toNonEmptyString(row.categoryName),
    opportunityTrackingEnabled: Number(row.opportunityTrackingEnabled) !== 0,
    opportunityDaysWithoutReturn: Number.isFinite(Number(row.opportunityDaysWithoutReturn))
      ? Number(row.opportunityDaysWithoutReturn)
      : null,
    opportunityPriority: normalizePriority(row.opportunityPriority, "medium"),
    allowManualCampaign: Number(row.allowManualCampaign) !== 0,
    suggestedMessageTemplate: toNonEmptyString(row.suggestedMessageTemplate),
    notes: toNonEmptyString(row.notes),
    createdAt: toNonEmptyString(row.createdAt),
    updatedAt: toNonEmptyString(row.updatedAt),
  }));
}

function upsertTenantCategoryOpportunityRulesByCode(code, rules = []) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const list = Array.isArray(rules) ? rules : [];
  const now = new Date().toISOString();
  const stmt = db.prepare(
    `
      INSERT INTO tenant_category_opportunity_rules (
        tenant_id, category_key, category_name,
        opportunity_tracking_enabled, opportunity_days_without_return,
        opportunity_priority, allow_manual_campaign,
        suggested_message_template, notes, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(tenant_id, category_key) DO UPDATE SET
        category_name = excluded.category_name,
        opportunity_tracking_enabled = excluded.opportunity_tracking_enabled,
        opportunity_days_without_return = excluded.opportunity_days_without_return,
        opportunity_priority = excluded.opportunity_priority,
        allow_manual_campaign = excluded.allow_manual_campaign,
        suggested_message_template = excluded.suggested_message_template,
        notes = excluded.notes,
        updated_at = excluded.updated_at
    `,
  );

  const transaction = db.transaction((items) => {
    for (const item of items) {
      const sanitized = sanitizeCategoryOpportunityRuleInput(item);
      if (!sanitized.categoryKey || !sanitized.categoryName) {
        continue;
      }
      stmt.run(
        tenant.id,
        sanitized.categoryKey,
        sanitized.categoryName,
        sanitized.opportunityTrackingEnabled ? 1 : 0,
        sanitized.opportunityDaysWithoutReturn,
        sanitized.opportunityPriority,
        sanitized.allowManualCampaign ? 1 : 0,
        sanitized.suggestedMessageTemplate,
        sanitized.notes,
        now,
        now,
      );
    }
  });

  transaction(list);
  return listTenantCategoryOpportunityRulesByCode(tenant.code);
}

function listCrmClientBlocksByCode(code, { phone = "" } = {}) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return [];
  }

  let query = `
    SELECT id, client_id AS clientId, client_name AS clientName, phone,
           is_blocked AS isBlocked, block_reason AS blockReason, block_notes AS blockNotes,
           blocked_at AS blockedAt, blocked_by AS blockedBy,
           created_at AS createdAt, updated_at AS updatedAt
    FROM crm_client_blocks
    WHERE tenant_id = ?
  `;
  const params = [tenant.id];
  const normalizedPhone = normalizePhone(phone);
  if (normalizedPhone) {
    query += " AND phone = ?";
    params.push(normalizedPhone);
  }
  query += " ORDER BY is_blocked DESC, datetime(updated_at) DESC, id DESC";

  return db.prepare(query).all(...params).map((row) => ({
    id: Number(row.id),
    clientId: Number.isFinite(Number(row.clientId)) ? Number(row.clientId) : null,
    clientName: toNonEmptyString(row.clientName),
    phone: toNonEmptyString(row.phone),
    isBlocked: Number(row.isBlocked) !== 0,
    blockReason: toNonEmptyString(row.blockReason),
    blockNotes: toNonEmptyString(row.blockNotes),
    blockedAt: toNonEmptyString(row.blockedAt),
    blockedBy: toNonEmptyString(row.blockedBy),
    createdAt: toNonEmptyString(row.createdAt),
    updatedAt: toNonEmptyString(row.updatedAt),
  }));
}

function upsertCrmClientBlockByCode(code, payload = {}) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const phone = normalizePhone(payload.phone || payload.clientPhone || "");
  if (!phone) {
    const error = new Error("Campo obrigatorio: phone");
    error.status = 400;
    throw error;
  }

  const now = new Date().toISOString();
  const isBlocked = payload.isBlocked == null ? true : Boolean(payload.isBlocked);
  db.prepare(
    `
      INSERT INTO crm_client_blocks (
        tenant_id, client_id, client_name, phone, is_blocked,
        block_reason, block_notes, blocked_at, blocked_by, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(tenant_id, phone) DO UPDATE SET
        client_id = excluded.client_id,
        client_name = excluded.client_name,
        is_blocked = excluded.is_blocked,
        block_reason = excluded.block_reason,
        block_notes = excluded.block_notes,
        blocked_at = excluded.blocked_at,
        blocked_by = excluded.blocked_by,
        updated_at = excluded.updated_at
    `,
  ).run(
    tenant.id,
    Number.isFinite(Number(payload.clientId)) ? Number(payload.clientId) : null,
    toNonEmptyString(payload.clientName),
    phone,
    isBlocked ? 1 : 0,
    toNonEmptyString(payload.blockReason),
    toNonEmptyString(payload.blockNotes || payload.notes),
    isBlocked ? (toNonEmptyString(payload.blockedAt) || now) : null,
    toNonEmptyString(payload.blockedBy),
    now,
    now,
  );

  return listCrmClientBlocksByCode(tenant.code, { phone })[0] || null;
}

function listCrmReturnFlowsByCode(code, { phone = "", status = "", limit = 200 } = {}) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return [];
  }

  let query = `
    SELECT id, client_id AS clientId, client_name AS clientName, phone,
           origin_service_key AS originServiceKey, origin_service_name AS originServiceName,
           origin_category_key AS originCategoryKey, origin_category_name AS originCategoryName,
           last_visit_at AS lastVisitAt, last_professional_id AS lastProfessionalId,
           last_professional_name AS lastProfessionalName, last_professional_active AS lastProfessionalActive,
           flow_status AS flowStatus, current_step AS currentStep,
           entered_flow_at AS enteredFlowAt, last_message_sent_at AS lastMessageSentAt,
           next_scheduled_send_at AS nextScheduledSendAt, stop_reason AS stopReason,
           converted_appointment_id AS convertedAppointmentId, converted_at AS convertedAt,
           created_at AS createdAt, updated_at AS updatedAt
    FROM crm_return_flows
    WHERE tenant_id = ?
  `;
  const params = [tenant.id];
  const normalizedPhone = normalizePhone(phone);
  const normalizedStatus = toNonEmptyString(status);
  if (normalizedPhone) {
    query += " AND phone = ?";
    params.push(normalizedPhone);
  }
  if (normalizedStatus) {
    query += " AND flow_status = ?";
    params.push(normalizedStatus);
  }
  query += " ORDER BY datetime(updated_at) DESC, id DESC LIMIT ?";
  params.push(Math.min(Math.max(Number(limit || 200), 1), 1000));

  return db.prepare(query).all(...params).map((row) => ({
    id: Number(row.id),
    clientId: Number.isFinite(Number(row.clientId)) ? Number(row.clientId) : null,
    clientName: toNonEmptyString(row.clientName),
    phone: toNonEmptyString(row.phone),
    originServiceKey: toNonEmptyString(row.originServiceKey),
    originServiceName: toNonEmptyString(row.originServiceName),
    originCategoryKey: toNonEmptyString(row.originCategoryKey),
    originCategoryName: toNonEmptyString(row.originCategoryName),
    lastVisitAt: toNonEmptyString(row.lastVisitAt),
    lastProfessionalId: Number.isFinite(Number(row.lastProfessionalId)) ? Number(row.lastProfessionalId) : null,
    lastProfessionalName: toNonEmptyString(row.lastProfessionalName),
    lastProfessionalActive: row.lastProfessionalActive == null
      ? null
      : Number(row.lastProfessionalActive) !== 0,
    flowStatus: toNonEmptyString(row.flowStatus),
    currentStep: Number.isFinite(Number(row.currentStep)) ? Number(row.currentStep) : 0,
    enteredFlowAt: toNonEmptyString(row.enteredFlowAt),
    lastMessageSentAt: toNonEmptyString(row.lastMessageSentAt),
    nextScheduledSendAt: toNonEmptyString(row.nextScheduledSendAt),
    stopReason: toNonEmptyString(row.stopReason),
    convertedAppointmentId: Number.isFinite(Number(row.convertedAppointmentId)) ? Number(row.convertedAppointmentId) : null,
    convertedAt: toNonEmptyString(row.convertedAt),
    createdAt: toNonEmptyString(row.createdAt),
    updatedAt: toNonEmptyString(row.updatedAt),
  }));
}

function listCrmCategoryOpportunitiesByCode(code, { status = "", limit = 200 } = {}) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    return [];
  }

  let query = `
    SELECT id, client_id AS clientId, client_name AS clientName, phone,
           category_key AS categoryKey, category_name AS categoryName,
           source_service_key AS sourceServiceKey, source_service_name AS sourceServiceName,
           last_relevant_visit_at AS lastRelevantVisitAt, days_without_return AS daysWithoutReturn,
           last_professional_id AS lastProfessionalId, last_professional_name AS lastProfessionalName,
           last_professional_active AS lastProfessionalActive,
           opportunity_status AS opportunityStatus, priority, owner, notes,
           created_at AS createdAt, updated_at AS updatedAt
    FROM crm_category_opportunities
    WHERE tenant_id = ?
  `;
  const params = [tenant.id];
  const normalizedStatus = toNonEmptyString(status);
  if (normalizedStatus) {
    query += " AND opportunity_status = ?";
    params.push(normalizedStatus);
  }
  query += " ORDER BY datetime(updated_at) DESC, id DESC LIMIT ?";
  params.push(Math.min(Math.max(Number(limit || 200), 1), 1000));

  return db.prepare(query).all(...params).map((row) => ({
    id: Number(row.id),
    clientId: Number.isFinite(Number(row.clientId)) ? Number(row.clientId) : null,
    clientName: toNonEmptyString(row.clientName),
    phone: toNonEmptyString(row.phone),
    categoryKey: toNonEmptyString(row.categoryKey),
    categoryName: toNonEmptyString(row.categoryName),
    sourceServiceKey: toNonEmptyString(row.sourceServiceKey),
    sourceServiceName: toNonEmptyString(row.sourceServiceName),
    lastRelevantVisitAt: toNonEmptyString(row.lastRelevantVisitAt),
    daysWithoutReturn: Number.isFinite(Number(row.daysWithoutReturn)) ? Number(row.daysWithoutReturn) : null,
    lastProfessionalId: Number.isFinite(Number(row.lastProfessionalId)) ? Number(row.lastProfessionalId) : null,
    lastProfessionalName: toNonEmptyString(row.lastProfessionalName),
    lastProfessionalActive: row.lastProfessionalActive == null
      ? null
      : Number(row.lastProfessionalActive) !== 0,
    opportunityStatus: toNonEmptyString(row.opportunityStatus),
    priority: normalizePriority(row.priority, "medium"),
    owner: toNonEmptyString(row.owner),
    notes: toNonEmptyString(row.notes),
    createdAt: toNonEmptyString(row.createdAt),
    updatedAt: toNonEmptyString(row.updatedAt),
  }));
}

function resolveServiceKeyFromCatalogItem(item) {
  const idCandidate = firstNonEmpty([
    toNonEmptyString(item?.id),
    toNonEmptyString(item?.servicoId),
    toNonEmptyString(item?.serviceId),
    toNonEmptyString(item?.codigo),
    toNonEmptyString(item?.code),
  ]);
  if (idCandidate) {
    return idCandidate;
  }
  const nameCandidate = toNonEmptyString(item?.nome || item?.name || item?.servicoNome);
  return normalizeTenantCode(nameCandidate).replace(/-/g, "_");
}

function resolveCategoryKeyFromCatalogItem(item) {
  return firstNonEmpty([
    toNonEmptyString(item?.categoriaId),
    toNonEmptyString(item?.categoria?.id),
    toNonEmptyString(item?.categoryId),
    toNonEmptyString(item?.category?.id),
    normalizeTenantCode(item?.categoriaNome || item?.categoria || item?.category || item?.categoryName).replace(/-/g, "_"),
  ]);
}

function resolveCategoryNameFromCatalogItem(item) {
  return firstNonEmpty([
    toNonEmptyString(item?.categoriaNome),
    toNonEmptyString(item?.categoria?.nome),
    toNonEmptyString(item?.categoria),
    toNonEmptyString(item?.categoryName),
    toNonEmptyString(item?.category?.name),
    toNonEmptyString(item?.category),
  ]);
}

function mapTrinksCatalogServiceItem(item) {
  const serviceKey = resolveServiceKeyFromCatalogItem(item);
  const serviceName = toNonEmptyString(item?.nome || item?.name || item?.servicoNome);
  if (!serviceKey || !serviceName) {
    return null;
  }

  const duration = Number(item?.duracaoEmMinutos || item?.duracao || item?.duracaoMinutos);
  const price = Number(item?.valor || item?.preco);
  const categoryKey = resolveCategoryKeyFromCatalogItem(item);
  const categoryName = resolveCategoryNameFromCatalogItem(item);

  return {
    serviceKey,
    serviceName,
    categoryKey,
    categoryName,
    serviceId: Number.isFinite(Number(item?.id || item?.servicoId || item?.serviceId))
      ? Number(item?.id || item?.servicoId || item?.serviceId)
      : null,
    durationMinutes: Number.isFinite(duration) && duration > 0 ? duration : null,
    price: Number.isFinite(price) ? price : null,
    active: item?.ativo == null ? true : Boolean(item.ativo),
    visibleToClient: item?.visivelCliente == null ? null : Boolean(item.visivelCliente),
    raw: item,
  };
}

async function listTenantServiceCatalogByCode(code) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }
  if (!tenant.establishmentId) {
    const error = new Error("Tenant sem establishmentId configurado.");
    error.status = 400;
    throw error;
  }

  const collected = [];
  const seen = new Set();
  for (let page = 1; page <= 10; page += 1) {
    const payload = await trinksRequest("/servicos", {
      method: "GET",
      estabelecimentoId: tenant.establishmentId,
      tenantCode: tenant.code,
      query: {
        page,
        pageSize: 100,
      },
    });
    const items = extractItems(payload);
    for (const item of items) {
      const mapped = mapTrinksCatalogServiceItem(item);
      if (!mapped) {
        continue;
      }
      if (seen.has(mapped.serviceKey)) {
        continue;
      }
      seen.add(mapped.serviceKey);
      collected.push(mapped);
    }
    if (items.length < 100) {
      break;
    }
  }

  return collected.sort((a, b) =>
    `${a.categoryName} ${a.serviceName}`.localeCompare(`${b.categoryName} ${b.serviceName}`, "pt-BR", {
      sensitivity: "base",
    }),
  );
}

async function buildTenantCrmServiceCatalogWithRules(code) {
  const [catalog, rules] = await Promise.all([
    listTenantServiceCatalogByCode(code),
    Promise.resolve(listTenantServiceReturnRulesByCode(code)),
  ]);
  const ruleMap = new Map(rules.map((item) => [String(item.serviceKey), item]));

  return catalog.map((item) => ({
    ...item,
    rule: ruleMap.get(String(item.serviceKey)) || null,
  }));
}

function buildTenantCrmDashboard(code) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const serviceRules = listTenantServiceReturnRulesByCode(tenant.code);
  const categoryRules = listTenantCategoryOpportunityRulesByCode(tenant.code);
  const blocks = listCrmClientBlocksByCode(tenant.code);
  const flows = listCrmReturnFlowsByCode(tenant.code, { limit: 1000 });
  const opportunities = listCrmCategoryOpportunitiesByCode(tenant.code, { limit: 1000 });
  const settings = getTenantCrmSettingsByCode(tenant.code);

  const auditSummary = db.prepare(
    `
      SELECT
        COUNT(1) AS total,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successCount,
        SUM(CASE WHEN event_type = 'create' THEN 1 ELSE 0 END) AS createCount
      FROM appointment_audit
      WHERE tenant_code = ?
    `,
  ).get(tenant.code);

  const webhookSummary = db.prepare(
    `
      SELECT
        COUNT(1) AS total,
        SUM(CASE WHEN reason = 'handoffActivated' THEN 1 ELSE 0 END) AS handoffActivatedCount,
        SUM(CASE WHEN reason = 'handoffResumed' THEN 1 ELSE 0 END) AS handoffResumedCount
      FROM webhook_events
      WHERE tenant_code = ?
    `,
  ).get(tenant.code);

  const flowsByStatus = {};
  for (const item of flows) {
    const key = toNonEmptyString(item.flowStatus) || "unknown";
    flowsByStatus[key] = Number(flowsByStatus[key] || 0) + 1;
  }

  const opportunitiesByStatus = {};
  for (const item of opportunities) {
    const key = toNonEmptyString(item.opportunityStatus) || "unknown";
    opportunitiesByStatus[key] = Number(opportunitiesByStatus[key] || 0) + 1;
  }

  const activeRules = serviceRules.filter((item) => item.active && Number(item.returnDays) > 0);
  return {
    settings: settings?.config || getDefaultCrmSettings(),
    totals: {
      configuredServices: serviceRules.length,
      activeServiceRules: activeRules.length,
      configuredCategories: categoryRules.length,
      blockedClients: blocks.filter((item) => item.isBlocked).length,
      flowsTotal: flows.length,
      opportunitiesTotal: opportunities.length,
      auditAppointmentsTotal: Number(auditSummary?.total || 0),
      auditAppointmentsSuccess: Number(auditSummary?.successCount || 0),
      auditAppointmentsCreated: Number(auditSummary?.createCount || 0),
      webhookEventsTotal: Number(webhookSummary?.total || 0),
      handoffActivatedCount: Number(webhookSummary?.handoffActivatedCount || 0),
      handoffResumedCount: Number(webhookSummary?.handoffResumedCount || 0),
    },
    flowsByStatus,
    opportunitiesByStatus,
    topServices: activeRules
      .slice()
      .sort((a, b) => (Number(a.returnDays || 0) - Number(b.returnDays || 0)))
      .slice(0, 8),
    topCategories: categoryRules
      .slice()
      .sort((a, b) => (Number(a.opportunityDaysWithoutReturn || 0) - Number(b.opportunityDaysWithoutReturn || 0)))
      .slice(0, 8),
    recentBlocks: blocks.slice(0, 8),
    recentFlows: flows.slice(0, 8),
    recentOpportunities: opportunities.slice(0, 8),
  };
}

function daysBetweenIsoDates(fromIsoDate = "", toIsoDate = "") {
  const start = Date.parse(`${String(fromIsoDate || "").trim()}T00:00:00Z`);
  const end = Date.parse(`${String(toIsoDate || "").trim()}T00:00:00Z`);
  if (!Number.isFinite(start) || !Number.isFinite(end)) {
    return null;
  }
  return Math.floor((end - start) / (24 * 60 * 60 * 1000));
}

function findMatchingServiceRuleForName(serviceName = "", rules = []) {
  const target = toNonEmptyString(serviceName);
  if (!target || !Array.isArray(rules) || !rules.length) {
    return null;
  }

  const candidates = rules.map((rule) => {
    const aliases = toNonEmptyString(rule.serviceNameAliases || rule.service_name_aliases || "")
      .split("|")
      .map((a) => toNonEmptyString(a))
      .filter(Boolean);
    return {
      nome: rule.serviceName,
      rule,
      aliases,
    };
  });
  const matched = findBestServiceMatch(target, candidates);
  return matched?.rule || null;
}

function queryRecentTenantAppointmentAudit(tenantCode, { limit = 2000 } = {}) {
  return db.prepare(
    `
      SELECT id, tenant_code AS tenantCode, event_type AS eventType, status,
             establishment_id AS establishmentId, appointment_id AS appointmentId,
             confirmation_code AS confirmationCode, client_phone AS clientPhone,
             client_name AS clientName, service_name AS serviceName,
             professional_name AS professionalName, appointment_date AS appointmentDate,
             appointment_time AS appointmentTime, request_payload AS requestPayload,
             response_payload AS responsePayload, created_at AS createdAt
      FROM appointment_audit
      WHERE tenant_code = ?
        AND status = 'success'
        AND event_type IN ('create', 'reschedule')
        AND COALESCE(client_phone, '') <> ''
        AND COALESCE(service_name, '') <> ''
      ORDER BY appointment_date DESC, appointment_time DESC, id DESC
      LIMIT ?
    `,
  ).all(normalizeTenantScopeCode(tenantCode), Math.min(Math.max(Number(limit || 2000), 1), 5000)).map((row) => ({
    ...row,
    requestPayload: row.requestPayload ? safeJsonParse(row.requestPayload) : null,
    responsePayload: row.responsePayload ? safeJsonParse(row.responsePayload) : null,
  }));
}

async function queryRecentTenantAppointmentsFromTrinks(tenant, { lookbackDays = 365, limit = 4000, serviceRules = [] } = {}) {
  if (!tenant?.establishmentId) {
    return [];
  }

  const todayIso = getSaoPauloDateContext().isoToday;
  const cutoffIso = addDaysToIsoDate(todayIso, -Math.abs(Number(lookbackDays || 365)));
  const maxRows = Math.min(Math.max(Number(limit || 4000), 1), 5000);
  const maxPages = Math.min(160, Math.max(10, Math.ceil(maxRows / 25)));
  const appointmentRows = [];
  const relevantClientIds = new Set();
  const seenAppointmentIds = new Set();

  for (let page = 1; page <= maxPages; page += 1) {
    let payload = null;
    try {
      payload = await trinksRequest("/agendamentos", {
        method: "GET",
        estabelecimentoId: tenant.establishmentId,
        tenantCode: tenant.code,
        query: {
          page,
          pageSize: 50,
        },
      });
    } catch {
      break;
    }

    const items = extractItems(payload);
    if (!items.length) {
      break;
    }

    let pageOldestDate = "";
    for (const item of items) {
      const normalized = normalizeAppointmentItem(item);
      if (!normalized?.date || appointmentLooksCanceled(normalized.raw)) {
        continue;
      }

      if (!pageOldestDate || normalized.date < pageOldestDate) {
        pageOldestDate = normalized.date;
      }

      if (normalized.date > todayIso) {
        continue;
      }
      if (cutoffIso && normalized.date < cutoffIso) {
        continue;
      }

      const appointmentId = Number(normalized.id);
      const appointmentKey = Number.isFinite(appointmentId) && appointmentId > 0
        ? `id:${appointmentId}`
        : `${normalized.date}|${normalized.time}|${normalized.client}|${normalized.professional}|${normalized.services.join("|")}`;
      if (seenAppointmentIds.has(appointmentKey)) {
        continue;
      }
      seenAppointmentIds.add(appointmentKey);

      const serviceName = toNonEmptyString(normalized.services[0] || "");
      if (!serviceName) {
        continue;
      }
      if (Array.isArray(serviceRules) && serviceRules.length && !findMatchingServiceRuleForName(serviceName, serviceRules)) {
        continue;
      }

      const clientId = Number(normalized.raw?.cliente?.id ?? normalized.raw?.clienteId ?? normalized.raw?.clientId);
      if (Number.isFinite(clientId) && clientId > 0) {
        relevantClientIds.add(clientId);
      }

      appointmentRows.push({
        id: appointmentId || null,
        tenantCode: tenant.code,
        establishmentId: tenant.establishmentId,
        clientId: Number.isFinite(clientId) && clientId > 0 ? clientId : null,
        clientName: toNonEmptyString(normalized.client),
        clientPhone: "",
        serviceName,
        professionalName: toNonEmptyString(normalized.professional),
        appointmentDate: normalized.date,
        appointmentTime: toNonEmptyString(normalized.time),
        createdAt: toNonEmptyString(normalized.dateTime),
        source: "trinks_history",
      });

      if (appointmentRows.length >= maxRows) {
        break;
      }
    }

    if (appointmentRows.length >= maxRows) {
      break;
    }
    if (pageOldestDate && cutoffIso && pageOldestDate < cutoffIso) {
      break;
    }
  }

  if (!appointmentRows.length || !relevantClientIds.size) {
    return appointmentRows;
  }

  const clientCache = new Map();
  const clientIds = [...relevantClientIds];
  for (let offset = 0; offset < clientIds.length; offset += 10) {
    const batch = clientIds.slice(offset, offset + 10);
    const results = await Promise.all(batch.map(async (clientId) => {
      try {
        const client = await getClientById(tenant.establishmentId, clientId);
        const phone = extractPreferredPhoneFromClient(client);
        const clientName = toNonEmptyString(
          clientDisplayNameFrom(client) || appointmentRows.find((row) => row.clientId === clientId)?.clientName,
        );
        return { clientId, phone, clientName };
      } catch {
        return { clientId, phone: "", clientName: "" };
      }
    }));
    for (const item of results) {
      clientCache.set(item.clientId, { phone: item.phone, clientName: item.clientName });
      if (item.phone) {
        upsertClientPhoneMap(item.phone, item.clientId, item.clientName);
      }
    }
  }

  return appointmentRows
    .map((row) => {
      const resolved = clientCache.get(row.clientId) || null;
      return {
        ...row,
        clientPhone: toNonEmptyString(resolved?.phone),
        clientName: toNonEmptyString(resolved?.clientName || row.clientName),
      };
    })
    .filter((row) => normalizePhone(row.clientPhone));
}

async function detectFutureBookingForPhone(tenant, phone, cache, todayIso) {
  const normalizedPhone = normalizePhone(phone);
  if (!tenant?.establishmentId || !normalizedPhone) {
    return {
      phone: normalizedPhone,
      hasFutureBooking: false,
      clientId: null,
      clientName: "",
      firstFutureBooking: null,
      lookupStatus: "missing_phone",
    };
  }

  if (cache.has(normalizedPhone)) {
    return cache.get(normalizedPhone);
  }

  let result = {
    phone: normalizedPhone,
    hasFutureBooking: false,
    clientId: null,
    clientName: "",
    firstFutureBooking: null,
    lookupStatus: "not_found",
  };

  try {
    const client = await findExistingClientByPhone(tenant.establishmentId, normalizedPhone);
    const clientId = Number(clientIdFrom(client));
    if (Number.isFinite(clientId) && clientId > 0) {
      const dateToIso = addDaysToIsoDate(todayIso, 120);
      const futureItems = await listAppointmentsByClientId(
        tenant.establishmentId,
        clientId,
        { dateFrom: todayIso, dateTo: dateToIso },
      );
      const normalizedAppointments = futureItems
        .map(normalizeAppointmentItem)
        .filter(Boolean)
        .filter((item) => !appointmentLooksCanceled(item.raw))
        .filter((item) => toNonEmptyString(item.date) >= todayIso)
        .filter((item) => {
          const date = toNonEmptyString(item.date);
          return !dateToIso || (date && date <= dateToIso);
        })
        .filter((item) => {
          const apptClientId = Number(appointmentClientIdFrom(item?.raw || item));
          if (Number.isFinite(apptClientId) && apptClientId > 0) {
            return apptClientId === clientId;
          }
          return true;
        })
        .sort((left, right) => appointmentSortDateTime(left).localeCompare(appointmentSortDateTime(right)));

      result = {
        phone: normalizedPhone,
        hasFutureBooking: normalizedAppointments.length > 0,
        clientId,
        clientName: clientDisplayNameFrom(client),
        firstFutureBooking: normalizedAppointments[0] || null,
        lookupStatus: "ok",
      };
    }
  } catch (error) {
    result = {
      ...result,
      lookupStatus: "error",
      errorMessage: error?.message || "Erro ao consultar agendamentos futuros da cliente.",
    };
  }

  cache.set(normalizedPhone, result);
  return result;
}

function findOpenCrmReturnFlowRow(tenantId, phone, originServiceKey) {
  return db.prepare(
    `
      SELECT id
      FROM crm_return_flows
      WHERE tenant_id = ?
        AND phone = ?
        AND origin_service_key = ?
        AND flow_status NOT IN ('converted', 'stopped', 'expired')
      ORDER BY datetime(updated_at) DESC, id DESC
      LIMIT 1
    `,
  ).get(tenantId, normalizePhone(phone), toNonEmptyString(originServiceKey));
}

function findOpenCrmCategoryOpportunityRow(tenantId, phone, categoryKey) {
  return db.prepare(
    `
      SELECT id
      FROM crm_category_opportunities
      WHERE tenant_id = ?
        AND phone = ?
        AND category_key = ?
        AND opportunity_status NOT IN ('converted', 'dismissed')
      ORDER BY datetime(updated_at) DESC, id DESC
      LIMIT 1
    `,
  ).get(tenantId, normalizePhone(phone), toNonEmptyString(categoryKey));
}

function recordCrmFlowEvent({ flowId = null, tenantId, eventType, step = null, metadata = {}, messagePreview = "", messageSent = "", replySummary = "", bookingId = null }) {
  if (!tenantId || !eventType) {
    return;
  }
  db.prepare(
    `
      INSERT INTO crm_flow_events (
        flow_id, tenant_id, event_type, step, message_preview, message_sent, reply_summary, booking_id, metadata_json, created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
  ).run(
    flowId,
    tenantId,
    toNonEmptyString(eventType),
    Number.isFinite(Number(step)) ? Number(step) : null,
    toNonEmptyString(messagePreview),
    toNonEmptyString(messageSent),
    toNonEmptyString(replySummary),
    bookingId != null ? Number(bookingId) || null : null,
    safeJsonStringify(metadata || {}),
    new Date().toISOString(),
  );
}

// ---------- CRM Phase 6 helpers ----------

function getActiveCrmFlowForPhone(tenantId, phone) {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone || !tenantId) return null;
  return db.prepare(
    `SELECT * FROM crm_return_flows
     WHERE tenant_id = ? AND phone = ?
       AND flow_status NOT IN ('converted', 'stopped', 'expired', 'opted_out')
     ORDER BY datetime(created_at) DESC LIMIT 1`,
  ).get(tenantId, normalizedPhone) || null;
}

function updateCrmFlowStatus(flowId, tenantId, updates = {}) {
  const now = new Date().toISOString();
  const sets = ["updated_at = ?"];
  const params = [now];
  if (updates.flow_status !== undefined) { sets.push("flow_status = ?"); params.push(updates.flow_status); }
  if (updates.stop_reason !== undefined) { sets.push("stop_reason = ?"); params.push(updates.stop_reason); }
  if (updates.current_step !== undefined) { sets.push("current_step = ?"); params.push(Number(updates.current_step)); }
  if (updates.entered_flow_at !== undefined) { sets.push("entered_flow_at = ?"); params.push(updates.entered_flow_at); }
  if (updates.last_message_sent_at !== undefined) { sets.push("last_message_sent_at = ?"); params.push(updates.last_message_sent_at); }
  if (updates.next_scheduled_send_at !== undefined) { sets.push("next_scheduled_send_at = ?"); params.push(updates.next_scheduled_send_at); }
  if (updates.converted_at !== undefined) { sets.push("converted_at = ?"); params.push(updates.converted_at); }
  if (updates.converted_appointment_id !== undefined) { sets.push("converted_appointment_id = ?"); params.push(updates.converted_appointment_id != null ? Number(updates.converted_appointment_id) || null : null); }
  params.push(flowId, tenantId);
  db.prepare(`UPDATE crm_return_flows SET ${sets.join(", ")} WHERE id = ? AND tenant_id = ?`).run(...params);
}

function getCrmFlowEventsByFlowId(flowId, tenantId) {
  return db.prepare(
    `SELECT id, flow_id AS flowId, event_type AS eventType, step,
            message_preview AS messagePreview, message_sent AS messageSent,
            reply_summary AS replySummary, booking_id AS bookingId,
            metadata_json AS metadataJson, created_at AS createdAt
     FROM crm_flow_events
     WHERE flow_id = ? AND tenant_id = ?
     ORDER BY created_at ASC`,
  ).all(flowId, tenantId).map((row) => ({
    id: Number(row.id),
    flowId: Number(row.flowId),
    eventType: toNonEmptyString(row.eventType),
    step: row.step != null ? Number(row.step) : null,
    messagePreview: toNonEmptyString(row.messagePreview),
    messageSent: toNonEmptyString(row.messageSent),
    replySummary: toNonEmptyString(row.replySummary),
    bookingId: row.bookingId != null ? Number(row.bookingId) : null,
    metadata: parseJsonObjectLoose(row.metadataJson, {}),
    createdAt: toNonEmptyString(row.createdAt),
  }));
}

function getCrmFlowById(flowId, tenantId) {
  return db.prepare(`SELECT * FROM crm_return_flows WHERE id = ? AND tenant_id = ? LIMIT 1`).get(flowId, tenantId) || null;
}

function isCrmOptOutText(text) {
  const n = normalizeForMatch(text);
  return /\b(nao\s*quero\s*(mais\s*)?(receber|mensagens?)|para\s*de\s*mandar|cancela[rm]?\s*mensagens?|nao\s*me\s*mande|nao\s*quero\s*mais\s*mensagens?|parar\s*mensagens?|remov[ae][rm]?.*(lista|contato)|sair\s*da\s*lista|opt-?out)\b/.test(n);
}

function formatCrmStepMessage(template, context = {}) {
  const { clientName = "", serviceName = "", lastVisitAt = "", humanNumber = "" } = context;
  return String(template || "")
    .replace(/\{\{client_name\}\}/gi, clientName)
    .replace(/\{\{service_name\}\}/gi, serviceName)
    .replace(/\{\{last_visit_at\}\}/gi, lastVisitAt ? isoToBrDate(lastVisitAt) || lastVisitAt : "sua ultima visita")
    .replace(/\{\{human_number\}\}/gi, humanNumber);
}

function findVeryRecentBookingAuditForPhone(tenantCode, phone, withinSeconds = 120) {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone || !tenantCode) return null;
  const cutoff = new Date(Date.now() - withinSeconds * 1000).toISOString();
  return db.prepare(
    `SELECT id, appointment_id AS appointmentId, confirmation_code AS confirmationCode
     FROM appointment_audit
     WHERE tenant_code = ? AND client_phone = ? AND status = 'success' AND event_type = 'create'
       AND created_at >= ?
     ORDER BY created_at DESC LIMIT 1`,
  ).get(normalizeTenantScopeCode(tenantCode), normalizedPhone, cutoff) || null;
}

function isCrmPhoneBlockedForTenantCode(code, phone) {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone || !code) {
    return false;
  }
  return listCrmClientBlocksByCode(code, { phone: normalizedPhone }).some((item) => item.isBlocked);
}

function getSaoPauloClockTime(at = new Date()) {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(at);
}

function isTimeWithinCrmWindow(currentTime = "", start = "", end = "") {
  const normalizedCurrent = normalizeTimeValue(currentTime);
  const normalizedStart = normalizeTimeValue(start);
  const normalizedEnd = normalizeTimeValue(end);
  if (!normalizedCurrent || !normalizedStart || !normalizedEnd) {
    return true;
  }
  if (normalizedStart <= normalizedEnd) {
    return normalizedCurrent >= normalizedStart && normalizedCurrent <= normalizedEnd;
  }
  return normalizedCurrent >= normalizedStart || normalizedCurrent <= normalizedEnd;
}

function getSaoPauloUtcRangeForIsoDate(isoDate = "") {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(isoDate || ""))) {
    return { start: "", end: "" };
  }
  const start = new Date(`${isoDate}T00:00:00-03:00`).toISOString();
  const end = new Date(`${addDaysToIsoDate(isoDate, 1)}T00:00:00-03:00`).toISOString();
  return { start, end };
}

function countTenantCrmStepSentOnIsoDate(tenantId, isoDate = "") {
  const { start, end } = getSaoPauloUtcRangeForIsoDate(isoDate);
  if (!tenantId || !start || !end) {
    return 0;
  }
  const row = db.prepare(
    `
      SELECT COUNT(1) AS total
      FROM crm_flow_events
      WHERE tenant_id = ?
        AND event_type = 'step_sent'
        AND created_at >= ?
        AND created_at < ?
    `,
  ).get(tenantId, start, end);
  return Number.isFinite(Number(row?.total)) ? Number(row.total) : 0;
}

function stopCrmFlowForSystemReason(flowId, tenantId, reason, metadata = {}) {
  updateCrmFlowStatus(flowId, tenantId, {
    flow_status: reason === "opt_out" ? "opted_out" : "stopped",
    stop_reason: toNonEmptyString(reason),
    next_scheduled_send_at: "",
  });
  recordCrmFlowEvent({
    flowId,
    tenantId,
    eventType: reason === "future_booking" ? "future_booking_detected" : "stopped",
    metadata,
  });
}

async function sendCrmFlowStepNow({
  tenant,
  flow,
  crmSettings = {},
  source = "manual",
} = {}) {
  if (!tenant?.id || !tenant?.code || !flow?.id) {
    const error = new Error("Fluxo CRM invalido para envio.");
    error.status = 400;
    throw error;
  }

  const settings = crmSettings && typeof crmSettings === "object"
    ? crmSettings
    : (getTenantCrmSettingsByCode(tenant.code)?.config || getDefaultCrmSettings());
  const status = toNonEmptyString(flow.flow_status || flow.flowStatus);
  let stepNumber = 0;
  if (["pending_approval", "scheduled_step_1", "eligible"].includes(status)) {
    stepNumber = 1;
  } else if (status === "scheduled_step_2") {
    stepNumber = 2;
  } else if (status === "scheduled_step_3") {
    stepNumber = 3;
  } else {
    const error = new Error(`Fluxo nao permite envio manual no status: ${status || "desconhecido"}`);
    error.status = 400;
    throw error;
  }

  if (isCrmPhoneBlockedForTenantCode(tenant.code, flow.phone)) {
    stopCrmFlowForSystemReason(flow.id, tenant.id, "client_blocked", {
      source,
      phone: normalizePhone(flow.phone),
    });
    const error = new Error("Esta cliente esta bloqueada no CRM. O fluxo foi encerrado.");
    error.status = 409;
    throw error;
  }

  if (settings.stopFlowOnAnyFutureBooking) {
    const futureBooking = await detectFutureBookingForPhone(
      tenant,
      flow.phone,
      new Map(),
      getSaoPauloDateContext().isoToday,
    );
    if (futureBooking?.hasFutureBooking) {
      stopCrmFlowForSystemReason(flow.id, tenant.id, "future_booking", {
        source,
        phone: normalizePhone(flow.phone),
        firstFutureBooking: futureBooking.firstFutureBooking || null,
      });
      const error = new Error("A cliente ja possui agendamento futuro. O fluxo foi encerrado.");
      error.status = 409;
      throw error;
    }
  }

  const serviceRule = db.prepare(
    `SELECT * FROM tenant_service_return_rules WHERE tenant_id = ? AND service_key = ? LIMIT 1`,
  ).get(tenant.id, flow.origin_service_key);
  const template = toNonEmptyString(
    stepNumber === 1
      ? serviceRule?.step1_message_template
      : stepNumber === 2
        ? serviceRule?.step2_message_template
        : serviceRule?.step3_message_template,
  );
  const fallbackTemplate = stepNumber === 1
    ? "Ola {{client_name}}! Faz um tempo que voce nao nos visita para {{service_name}}. Que tal agendar um horario?"
    : "Ola {{client_name}}! Passamos para lembrar que voce pode agendar seu {{service_name}} conosco.";
  const messageToSend = formatCrmStepMessage(template || fallbackTemplate, {
    clientName: toNonEmptyString(flow.client_name || flow.clientName),
    serviceName: toNonEmptyString(flow.origin_service_name || flow.originServiceName),
    lastVisitAt: toNonEmptyString(flow.last_visit_at || flow.lastVisitAt),
    humanNumber: toNonEmptyString(settings.humanHandoffClientNumber),
  });

  const instance = resolveEvolutionInstance(null, { tenantCode: tenant.code });
  if (!instance) {
    const error = new Error("Instancia Evolution nao configurada para este tenant.");
    error.status = 500;
    throw error;
  }

  await evolutionRequest(`/message/sendText/${instance}`, {
    method: "POST",
    body: { number: flow.phone, text: messageToSend },
  });

  const now = new Date().toISOString();
  const maxSteps = Number(settings.maxSteps || 3);
  const step2DelayDays = Number(serviceRule?.step2_delay_days || 7);
  const step3DelayDays = Number(serviceRule?.step3_delay_days || 14);
  let nextStatus = "expired";
  let nextSendAt = "";
  if (stepNumber < maxSteps) {
    if (stepNumber === 1) {
      nextStatus = "scheduled_step_2";
      nextSendAt = `${addDaysToIsoDate(now.slice(0, 10), step2DelayDays)}T09:00:00`;
    } else if (stepNumber === 2) {
      nextStatus = "scheduled_step_3";
      nextSendAt = `${addDaysToIsoDate(now.slice(0, 10), step3DelayDays)}T09:00:00`;
    }
  }

  updateCrmFlowStatus(flow.id, tenant.id, {
    flow_status: nextStatus,
    current_step: stepNumber,
    entered_flow_at: toNonEmptyString(flow.entered_flow_at || flow.enteredFlowAt) || now,
    last_message_sent_at: now,
    next_scheduled_send_at: nextSendAt,
    stop_reason: nextStatus === "expired" ? "exhausted" : "",
  });
  recordCrmFlowEvent({
    flowId: flow.id,
    tenantId: tenant.id,
    eventType: "step_sent",
    step: stepNumber,
    messageSent: messageToSend,
    messagePreview: messageToSend.slice(0, 200),
    metadata: { source },
  });

  return {
    stepNumber,
    nextStatus,
    nextSendAt,
    messageSent: messageToSend,
  };
}

function materializeCrmFlowCandidate(tenant, candidate, crmMode = "beta") {
  const phone = normalizePhone(candidate.phone);
  const now = new Date().toISOString();
  const existing = findOpenCrmReturnFlowRow(tenant.id, phone, candidate.originServiceKey);
  const nextStatus = crmMode === "automatic" ? "scheduled_step_1" : "pending_approval";

  if (existing?.id) {
    db.prepare(
      `
        UPDATE crm_return_flows
        SET client_id = ?,
            client_name = ?,
            origin_service_name = ?,
            origin_category_key = ?,
            origin_category_name = ?,
            last_visit_at = ?,
            last_professional_name = ?,
            last_professional_active = ?,
            flow_status = ?,
            current_step = 0,
            stop_reason = '',
            updated_at = ?
        WHERE id = ?
      `,
    ).run(
      Number.isFinite(Number(candidate.clientId)) ? Number(candidate.clientId) : null,
      toNonEmptyString(candidate.clientName),
      toNonEmptyString(candidate.originServiceName),
      toNonEmptyString(candidate.originCategoryKey),
      toNonEmptyString(candidate.originCategoryName),
      toNonEmptyString(candidate.lastVisitAt),
      toNonEmptyString(candidate.lastProfessionalName),
      candidate.lastProfessionalActive == null ? null : (candidate.lastProfessionalActive ? 1 : 0),
      nextStatus,
      now,
      existing.id,
    );
    recordCrmFlowEvent({
      flowId: existing.id,
      tenantId: tenant.id,
      eventType: "reentered_flow",
      metadata: candidate,
    });
    return { id: existing.id, action: "updated" };
  }

  const result = db.prepare(
    `
      INSERT INTO crm_return_flows (
        tenant_id, client_id, client_name, phone,
        origin_service_key, origin_service_name, origin_category_key, origin_category_name,
        last_visit_at, last_professional_name, last_professional_active,
        flow_status, current_step, entered_flow_at, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
    `,
  ).run(
    tenant.id,
    Number.isFinite(Number(candidate.clientId)) ? Number(candidate.clientId) : null,
    toNonEmptyString(candidate.clientName),
    phone,
    toNonEmptyString(candidate.originServiceKey),
    toNonEmptyString(candidate.originServiceName),
    toNonEmptyString(candidate.originCategoryKey),
    toNonEmptyString(candidate.originCategoryName),
    toNonEmptyString(candidate.lastVisitAt),
    toNonEmptyString(candidate.lastProfessionalName),
    candidate.lastProfessionalActive == null ? null : (candidate.lastProfessionalActive ? 1 : 0),
    nextStatus,
    now,
    now,
    now,
  );
  recordCrmFlowEvent({
    flowId: Number(result.lastInsertRowid),
    tenantId: tenant.id,
    eventType: "entered_flow",
    metadata: candidate,
  });
  return { id: Number(result.lastInsertRowid), action: "created" };
}

function materializeCrmOpportunityCandidate(tenant, candidate) {
  const phone = normalizePhone(candidate.phone);
  const now = new Date().toISOString();
  const existing = findOpenCrmCategoryOpportunityRow(tenant.id, phone, candidate.categoryKey);

  if (existing?.id) {
    db.prepare(
      `
        UPDATE crm_category_opportunities
        SET client_id = ?,
            client_name = ?,
            source_service_key = ?,
            source_service_name = ?,
            last_relevant_visit_at = ?,
            days_without_return = ?,
            last_professional_name = ?,
            last_professional_active = ?,
            opportunity_status = ?,
            priority = ?,
            notes = ?,
            updated_at = ?
        WHERE id = ?
      `,
    ).run(
      Number.isFinite(Number(candidate.clientId)) ? Number(candidate.clientId) : null,
      toNonEmptyString(candidate.clientName),
      toNonEmptyString(candidate.sourceServiceKey),
      toNonEmptyString(candidate.sourceServiceName),
      toNonEmptyString(candidate.lastRelevantVisitAt),
      Number.isFinite(Number(candidate.daysWithoutReturn)) ? Number(candidate.daysWithoutReturn) : null,
      toNonEmptyString(candidate.lastProfessionalName),
      candidate.lastProfessionalActive == null ? null : (candidate.lastProfessionalActive ? 1 : 0),
      "open",
      normalizePriority(candidate.priority, "medium"),
      toNonEmptyString(candidate.notes),
      now,
      existing.id,
    );
    return { id: existing.id, action: "updated" };
  }

  const result = db.prepare(
    `
      INSERT INTO crm_category_opportunities (
        tenant_id, client_id, client_name, phone,
        category_key, category_name, source_service_key, source_service_name,
        last_relevant_visit_at, days_without_return,
        last_professional_name, last_professional_active,
        opportunity_status, priority, notes, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?)
    `,
  ).run(
    tenant.id,
    Number.isFinite(Number(candidate.clientId)) ? Number(candidate.clientId) : null,
    toNonEmptyString(candidate.clientName),
    phone,
    toNonEmptyString(candidate.categoryKey),
    toNonEmptyString(candidate.categoryName),
    toNonEmptyString(candidate.sourceServiceKey),
    toNonEmptyString(candidate.sourceServiceName),
    toNonEmptyString(candidate.lastRelevantVisitAt),
    Number.isFinite(Number(candidate.daysWithoutReturn)) ? Number(candidate.daysWithoutReturn) : null,
    toNonEmptyString(candidate.lastProfessionalName),
    candidate.lastProfessionalActive == null ? null : (candidate.lastProfessionalActive ? 1 : 0),
    normalizePriority(candidate.priority, "medium"),
    toNonEmptyString(candidate.notes),
    now,
    now,
  );
  return { id: Number(result.lastInsertRowid), action: "created" };
}

async function runTenantCrmPreview(code, { lookbackDays = 365, materialize = false, limit = 250 } = {}) {
  const tenant = getTenantByCode(code);
  if (!tenant) {
    const error = new Error("Tenant nao encontrado.");
    error.status = 404;
    throw error;
  }

  const settings = getTenantCrmSettingsByCode(tenant.code)?.config || getDefaultCrmSettings();
  const serviceRules = listTenantServiceReturnRulesByCode(tenant.code).filter(
    (item) => item.active && Number(item.returnDays) > 0,
  );
  const categoryRules = listTenantCategoryOpportunityRulesByCode(tenant.code).filter(
    (item) => item.opportunityTrackingEnabled && Number(item.opportunityDaysWithoutReturn) > 0,
  );
  const todayIso = getSaoPauloDateContext().isoToday;
  const cutoffIso = addDaysToIsoDate(todayIso, -Math.abs(Number(lookbackDays || 365)));
  const localAuditRows = queryRecentTenantAppointmentAudit(tenant.code, { limit: 4000 })
    .filter((row) => toNonEmptyString(row.appointmentDate) && row.appointmentDate <= todayIso)
    .filter((row) => !cutoffIso || row.appointmentDate >= cutoffIso);
  const trinksRows = await queryRecentTenantAppointmentsFromTrinks(tenant, {
    lookbackDays,
    limit: 4000,
    serviceRules,
  });
  const seenHistoryKeys = new Set();
  const auditRows = [...trinksRows, ...localAuditRows].filter((row) => {
    const key = [
      normalizePhone(row.clientPhone),
      toNonEmptyString(row.serviceName),
      toNonEmptyString(row.appointmentDate),
      toNonEmptyString(row.appointmentTime),
    ].join("|");
    if (!key || seenHistoryKeys.has(key)) {
      return false;
    }
    seenHistoryKeys.add(key);
    return true;
  });

  const latestByPhoneService = new Map();
  const latestByPhoneCategory = new Map();
  const latestByPhoneOverall = new Map();
  for (const row of auditRows) {
    const phone = normalizePhone(row.clientPhone);
    if (!phone) continue;
    const serviceRule = findMatchingServiceRuleForName(row.serviceName, serviceRules);
    const overallKey = phone;
    if (!latestByPhoneOverall.has(overallKey)) {
      latestByPhoneOverall.set(overallKey, row);
    }
    if (serviceRule) {
      const serviceKey = `${phone}:${serviceRule.serviceKey}`;
      if (!latestByPhoneService.has(serviceKey)) {
        latestByPhoneService.set(serviceKey, { row, rule: serviceRule });
      }
    }
  }

  for (const row of auditRows) {
    const phone = normalizePhone(row.clientPhone);
    if (!phone) continue;
    const serviceRule = findMatchingServiceRuleForName(row.serviceName, serviceRules);
    const categoryKey = toNonEmptyString(serviceRule?.categoryKey);
    const categoryName = toNonEmptyString(serviceRule?.categoryName);
    if (!categoryKey || !categoryName) continue;
    const categoryRule = categoryRules.find((item) => item.categoryKey === categoryKey);
    if (!categoryRule) continue;
    const key = `${phone}:${categoryKey}`;
    if (!latestByPhoneCategory.has(key)) {
      latestByPhoneCategory.set(key, { row, serviceRule, categoryRule });
    }
  }

  const blockedPhones = new Set(
    listCrmClientBlocksByCode(tenant.code)
      .filter((item) => item.isBlocked)
      .map((item) => normalizePhone(item.phone)),
  );
  const futureBookingCache = new Map();
  const flowCandidates = [];
  const opportunityCandidates = [];
  const skipped = [];

  for (const { row, rule } of latestByPhoneService.values()) {
    if (flowCandidates.length >= limit) {
      break;
    }
    const phone = normalizePhone(row.clientPhone);
    const daysSinceLastVisit = daysBetweenIsoDates(row.appointmentDate, todayIso);
    if (!phone || daysSinceLastVisit == null) {
      continue;
    }
    if (daysSinceLastVisit < Number(rule.returnDays || 0)) {
      continue;
    }
    if (blockedPhones.has(phone)) {
      skipped.push({
        type: "flow",
        phone,
        clientName: row.clientName,
        reason: "client_blocked",
        serviceName: rule.serviceName,
      });
      continue;
    }

    const futureBooking = await detectFutureBookingForPhone(tenant, phone, futureBookingCache, todayIso);
    let resolvedClientName = toNonEmptyString(futureBooking.clientName || row.clientName);
    if (!resolvedClientName && tenant.establishmentId) {
      try {
        const knownClient = await findExistingClientByPhone(tenant.establishmentId, phone);
        resolvedClientName = toNonEmptyString(clientDisplayNameFrom(knownClient));
      } catch {
        resolvedClientName = "";
      }
    }
    if (futureBooking.hasFutureBooking) {
      skipped.push({
        type: "flow",
        phone,
        clientName: resolvedClientName,
        reason: "future_booking",
        serviceName: rule.serviceName,
        firstFutureBooking: futureBooking.firstFutureBooking,
      });
      continue;
    }

    const existingFlow = findOpenCrmReturnFlowRow(tenant.id, phone, rule.serviceKey);
    if (existingFlow?.id) {
      skipped.push({
        type: "flow",
        phone,
        clientName: row.clientName,
        reason: "already_open",
        serviceName: rule.serviceName,
      });
      continue;
    }

    const candidate = {
      phone,
      clientId: futureBooking.clientId,
      clientName: resolvedClientName,
      originServiceKey: rule.serviceKey,
      originServiceName: rule.serviceName,
      originCategoryKey: rule.categoryKey,
      originCategoryName: rule.categoryName,
      lastVisitAt: row.appointmentDate,
      lastProfessionalName: row.professionalName,
      lastProfessionalActive: null,
      daysSinceLastVisit,
      returnDays: rule.returnDays,
      stepMessages: {
        step1: rule.step1MessageTemplate || "",
        step2: rule.step2MessageTemplate || "",
        step3: rule.step3MessageTemplate || "",
      },
      statusIfCreated: settings.crmMode === "automatic" ? "scheduled_step_1" : "pending_approval",
    };
    flowCandidates.push(candidate);
  }

  for (const { row, serviceRule, categoryRule } of latestByPhoneCategory.values()) {
    if (opportunityCandidates.length >= limit) {
      break;
    }
    const phone = normalizePhone(row.clientPhone);
    const latestOverall = latestByPhoneOverall.get(phone);
    if (!phone || !latestOverall || toNonEmptyString(latestOverall.appointmentDate) <= toNonEmptyString(row.appointmentDate)) {
      continue;
    }
    const daysWithoutReturn = daysBetweenIsoDates(row.appointmentDate, todayIso);
    if (daysWithoutReturn == null || daysWithoutReturn < Number(categoryRule.opportunityDaysWithoutReturn || 0)) {
      continue;
    }

    const existingOpportunity = findOpenCrmCategoryOpportunityRow(tenant.id, phone, categoryRule.categoryKey);
    if (existingOpportunity?.id) {
      continue;
    }

    opportunityCandidates.push({
      phone,
      clientId: null,
      clientName: row.clientName,
      categoryKey: categoryRule.categoryKey,
      categoryName: categoryRule.categoryName,
      sourceServiceKey: serviceRule.serviceKey,
      sourceServiceName: serviceRule.serviceName,
      lastRelevantVisitAt: row.appointmentDate,
      daysWithoutReturn,
      lastProfessionalName: row.professionalName,
      lastProfessionalActive: null,
      priority: categoryRule.opportunityPriority,
      notes: "Cliente voltou por outro atendimento, mas esta categoria segue sem retorno no ciclo esperado.",
    });
  }

  const materialized = {
    flowsCreated: 0,
    flowsUpdated: 0,
    opportunitiesCreated: 0,
    opportunitiesUpdated: 0,
  };

  if (materialize) {
    for (const candidate of flowCandidates) {
      const result = materializeCrmFlowCandidate(tenant, candidate, settings.crmMode);
      if (result.action === "created") materialized.flowsCreated += 1;
      if (result.action === "updated") materialized.flowsUpdated += 1;
    }
    for (const candidate of opportunityCandidates) {
      const result = materializeCrmOpportunityCandidate(tenant, candidate);
      if (result.action === "created") materialized.opportunitiesCreated += 1;
      if (result.action === "updated") materialized.opportunitiesUpdated += 1;
    }
  }

  return {
    generatedAt: new Date().toISOString(),
    materialize,
    crmMode: settings.crmMode || "beta",
    summary: {
      auditedRows: auditRows.length,
      flowCandidates: flowCandidates.length,
      opportunityCandidates: opportunityCandidates.length,
      skipped: skipped.length,
      ...materialized,
    },
    flowCandidates,
    opportunityCandidates,
    skipped: skipped.slice(0, 200),
  };
}

function normalizeSchedulingProvider(value) {
  const normalized = toNonEmptyString(value)
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  if (!normalized) {
    return "";
  }

  if (normalized === "trinks") {
    return SCHEDULING_PROVIDER_TRINKS;
  }

  if (
    normalized === "google" ||
    normalized === "google_calendar" ||
    normalized === "googlecalendar" ||
    normalized === "calendar"
  ) {
    return SCHEDULING_PROVIDER_GOOGLE_CALENDAR;
  }

  return normalized;
}

function parseBooleanEnv(value, fallback = false) {
  const normalized = toNonEmptyString(value).toLowerCase();
  if (!normalized) {
    return fallback;
  }
  if (["1", "true", "yes", "on", "sim"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off", "nao"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function resolveSchedulingProvider(preferred = "") {
  const explicit = normalizeSchedulingProvider(preferred);
  if (explicit) {
    return explicit;
  }

  const envProvider = normalizeSchedulingProvider(
    firstNonEmpty([process.env.SCHEDULING_PROVIDER, process.env.SCHEDULING_DEFAULT_PROVIDER]),
  );
  if (envProvider) {
    return envProvider;
  }

  return SCHEDULING_PROVIDER_TRINKS;
}

function resolveTenantDefaultSchedulingProvider(tenantCode = "") {
  const tenant = getTenantByCode(tenantCode);
  if (!tenant || !tenant.active) {
    return "";
  }
  return resolveSchedulingProvider(tenant.defaultProvider);
}

function resolveSchedulingProviderForTenant({ preferredProvider = "", tenantCode = "" } = {}) {
  const explicit = normalizeSchedulingProvider(preferredProvider);
  if (explicit) {
    return explicit;
  }

  const tenantProvider = resolveTenantDefaultSchedulingProvider(tenantCode);
  if (tenantProvider) {
    return tenantProvider;
  }

  return resolveSchedulingProvider("");
}

function resolveSchedulingRequestContext({ tenantCode = "", tenantAlias = "", establishmentId = null } = {}) {
  const normalizedTenantCode = normalizeTenantCode(tenantCode || tenantAlias);
  const tenant = normalizedTenantCode ? getTenantByCode(normalizedTenantCode) : null;

  const parsedEstablishmentId = Number(establishmentId);
  const tenantEstablishmentId = Number(tenant?.establishmentId);
  const hasTenantEstablishmentId = Number.isFinite(tenantEstablishmentId) && tenantEstablishmentId > 0;
  const hasParsedEstablishmentId = Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0;
  const resolvedEstablishmentId = hasTenantEstablishmentId
    ? tenantEstablishmentId
    : (hasParsedEstablishmentId ? parsedEstablishmentId : null);

  return {
    tenantCode: normalizedTenantCode || "",
    tenant,
    establishmentId: resolvedEstablishmentId,
  };
}

function getSchedulingProviderCapabilities(provider) {
  if (provider === SCHEDULING_PROVIDER_TRINKS) {
    return {
      availability: true,
      createAppointment: true,
      listAppointmentsDay: true,
      listProfessionals: true,
      rescheduleAppointment: true,
      cancelAppointment: true,
      supportsProfessionalAgenda: true,
      supportsMultiServiceAppointment: true,
    };
  }

  if (provider === SCHEDULING_PROVIDER_GOOGLE_CALENDAR) {
    return {
      availability: true,
      createAppointment: true,
      listAppointmentsDay: true,
      listProfessionals: false,
      rescheduleAppointment: true,
      cancelAppointment: true,
      supportsProfessionalAgenda: false,
      supportsMultiServiceAppointment: false,
    };
  }

  return {
    availability: false,
    createAppointment: false,
    listAppointmentsDay: false,
    listProfessionals: false,
    rescheduleAppointment: false,
    cancelAppointment: false,
  };
}

function listSchedulingProviders({ tenantCode = "" } = {}) {
  const defaultProvider = resolveSchedulingProviderForTenant({ tenantCode });
  const googleConfigured = parseBooleanEnv(process.env.GOOGLE_CALENDAR_ENABLED, false)
    || Boolean(
      firstNonEmpty([
        process.env.GOOGLE_CLIENT_ID,
        process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
        process.env.GOOGLE_CALENDAR_ID,
      ]),
    );

  return [
    {
      provider: SCHEDULING_PROVIDER_TRINKS,
      label: "Trinks",
      ready: true,
      default: defaultProvider === SCHEDULING_PROVIDER_TRINKS,
      capabilities: getSchedulingProviderCapabilities(SCHEDULING_PROVIDER_TRINKS),
    },
    {
      provider: SCHEDULING_PROVIDER_GOOGLE_CALENDAR,
      label: "Google Calendar",
      ready: googleConfigured,
      default: defaultProvider === SCHEDULING_PROVIDER_GOOGLE_CALENDAR,
      capabilities: getSchedulingProviderCapabilities(SCHEDULING_PROVIDER_GOOGLE_CALENDAR),
    },
  ];
}

function createSchedulingProviderNotReadyError(provider) {
  const normalized = normalizeSchedulingProvider(provider) || String(provider || "");
  const error = new Error(
    `Provider de agenda '${normalized}' ainda nao foi configurado neste backend.`,
  );
  error.status = 501;
  error.details = {
    provider: normalized,
    action: "configure_provider_credentials",
  };
  return error;
}

function getSchedulingAdapter(preferredProviderOrOptions = "") {
  const options = typeof preferredProviderOrOptions === "object" && preferredProviderOrOptions !== null
    ? preferredProviderOrOptions
    : { provider: preferredProviderOrOptions };
  const provider = resolveSchedulingProviderForTenant({
    preferredProvider: options.provider,
    tenantCode: options.tenantCode,
  });

  if (provider === SCHEDULING_PROVIDER_TRINKS) {
    return {
      provider,
      capabilities: getSchedulingProviderCapabilities(provider),
      async getAvailability({ establishmentId, service, date, professionalName = "", preferredTime = "" }) {
        return getAvailability(establishmentId, service, date, {
          professionalName: professionalName ? String(professionalName) : "",
          preferredTime: preferredTime ? String(preferredTime) : "",
        });
      },
      async createAppointment({
        establishmentId,
        tenantCode = "",
        service,
        date,
        time,
        professionalName = "",
        clientName,
        clientPhone,
      }) {
        return createAppointment({
          establishmentId,
          tenantCode: tenantCode ? String(tenantCode) : "",
          service,
          date,
          time,
          professionalName: professionalName ? String(professionalName) : "",
          clientName,
          clientPhone,
        });
      },
      async getAppointmentsDay({ establishmentId, date }) {
        const response = await getAppointmentsForDate(Number(establishmentId), String(date));
        return {
          source: response.source,
          appointments: response.items.map(normalizeAppointmentItem).filter(Boolean),
          raw: response.raw,
        };
      },
      async getProfessionals({ establishmentId, date, serviceId }) {
        const professionals = await getProfessionals({
          establishmentId: Number(establishmentId),
          date: String(date),
          serviceId: serviceId ? Number(serviceId) : undefined,
        });
        return {
          professionals: professionals
            .filter(professionalHasOpenSchedule)
            .map((item) => ({
              id: item.id,
              name: professionalDisplayName(item.name),
              availableTimes: item.availableTimes,
            })),
        };
      },
      async rescheduleAppointment({ establishmentId, tenantCode = "", confirmationCode, appointmentId, date, time }) {
        return rescheduleAppointment({
          establishmentId: Number(establishmentId),
          tenantCode: tenantCode ? String(tenantCode) : "",
          confirmationCode: confirmationCode ? String(confirmationCode) : "",
          appointmentId: appointmentId ? String(appointmentId) : "",
          date: String(date),
          time: String(time),
        });
      },
      async cancelAppointment({
        establishmentId,
        tenantCode = "",
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
        return cancelAppointment({
          establishmentId: Number(establishmentId),
          tenantCode: tenantCode ? String(tenantCode) : "",
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
      },
    };
  }

  if (provider === SCHEDULING_PROVIDER_GOOGLE_CALENDAR) {
    const notReady = async () => {
      throw createSchedulingProviderNotReadyError(provider);
    };

    return {
      provider,
      capabilities: getSchedulingProviderCapabilities(provider),
      getAvailability: notReady,
      createAppointment: notReady,
      getAppointmentsDay: notReady,
      getProfessionals: notReady,
      rescheduleAppointment: notReady,
      cancelAppointment: notReady,
    };
  }

  const error = new Error(`Provider de agenda nao suportado: ${provider}`);
  error.status = 400;
  error.details = {
    provider,
    supportedProviders: [SCHEDULING_PROVIDER_TRINKS, SCHEDULING_PROVIDER_GOOGLE_CALENDAR],
  };
  throw error;
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

function normalizeTenantScopeCode(value = "") {
  return normalizeTenantCode(value);
}

function buildWhatsappConversationCacheKey(phone, tenantCode = "") {
  const normalizedPhone = normalizePhone(phone || "");
  const normalizedTenantCode = normalizeTenantScopeCode(tenantCode);
  if (!normalizedPhone) {
    return "";
  }
  return normalizedTenantCode ? `${normalizedTenantCode}:${normalizedPhone}` : normalizedPhone;
}

function parseWhatsappConversationCacheKey(key = "") {
  const raw = toNonEmptyString(key);
  if (!raw) {
    return { tenantCode: "", phone: "" };
  }

  const separator = raw.indexOf(":");
  if (separator <= 0) {
    return { tenantCode: "", phone: normalizePhone(raw) };
  }

  return {
    tenantCode: normalizeTenantScopeCode(raw.slice(0, separator)),
    phone: normalizePhone(raw.slice(separator + 1)),
  };
}

function persistWhatsappMessage({
  phone,
  role,
  content,
  at,
  senderName = "",
  source = "runtime",
  tenantCode = "",
}) {
  if (!phone || !role || !content) {
    return;
  }

  db.prepare(
    `
      INSERT INTO whatsapp_messages (tenant_code, phone, role, content, sender_name, at, source)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `,
  ).run(
    normalizeTenantScopeCode(tenantCode),
    String(phone),
    String(role),
    String(content),
    senderName ? String(senderName) : "",
    at ? String(at) : new Date().toISOString(),
    source ? String(source) : "runtime",
  );
}

function loadWhatsappMessagesFromDb(phone, limit = MAX_WHATSAPP_HISTORY_MESSAGES, options = {}) {
  if (!phone) {
    return [];
  }

  const normalizedTenantCode = normalizeTenantScopeCode(options?.tenantCode || "");
  const rows = normalizedTenantCode
    ? db.prepare(
      `
        SELECT role, content, at, sender_name AS senderName
        FROM whatsapp_messages
        WHERE phone = ?
          AND tenant_code = ?
        ORDER BY datetime(at) DESC, id DESC
        LIMIT ?
      `,
    ).all(String(phone), normalizedTenantCode, Number(limit))
    : db.prepare(
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

function getWhatsappHistory(phone, options = {}) {
  if (!phone) {
    return [];
  }

  const cacheKey = buildWhatsappConversationCacheKey(phone, options?.tenantCode || "");
  const current = Array.isArray(whatsappConversations.get(cacheKey))
    ? whatsappConversations.get(cacheKey)
    : [];

  const normalized = current.map(normalizeWhatsappMessage).filter(Boolean);
  if (normalized.length) {
    return normalized;
  }

  const fromDb = loadWhatsappMessagesFromDb(phone, MAX_WHATSAPP_HISTORY_MESSAGES, options);
  if (fromDb.length) {
    whatsappConversations.set(cacheKey, fromDb);
  }
  return fromDb;
}

function pushWhatsappHistory(phone, role, content, senderName = "", options = {}) {
  if (!phone || !role || !content) {
    return;
  }

  const normalizedTenantCode = normalizeTenantScopeCode(options?.tenantCode || "");
  const cacheKey = buildWhatsappConversationCacheKey(phone, normalizedTenantCode);
  const current = getWhatsappHistory(phone, { tenantCode: normalizedTenantCode });
  const entry = {
    role,
    content: String(content),
    at: new Date().toISOString(),
    senderName: senderName ? String(senderName) : "",
  };
  const updated = [...current, entry].slice(-MAX_WHATSAPP_HISTORY_MESSAGES);
  whatsappConversations.set(cacheKey, updated);
  persistWhatsappMessage({
    phone,
    role,
    content,
    at: entry.at,
    senderName: entry.senderName,
    source: "runtime",
    tenantCode: normalizedTenantCode,
  });
}

function cleanupWebhookDedupeCache(now = Date.now()) {
  for (const [key, value] of recentWebhookMessages.entries()) {
    if (!value || now - Number(value.at || 0) > WEBHOOK_DEDUPE_WINDOW_MS) {
      recentWebhookMessages.delete(key);
    }
  }
}

function isDuplicateIncomingWhatsapp(incoming, options = {}) {
  const now = Date.now();
  cleanupWebhookDedupeCache(now);

  const sender = normalizePhone(incoming?.senderNumber || "");
  const messageId = toNonEmptyString(incoming?.messageId);
  const text = toNonEmptyString(incoming?.messageText).toLowerCase();
  const tenantScope = normalizeTenantScopeCode(options?.tenantCode || "");
  const instanceScope = toNonEmptyString(options?.instanceName || incoming?.instanceName).toLowerCase();
  const scope = tenantScope || (instanceScope ? `instance:${instanceScope}` : "global");
  const withMessageId = sender && messageId ? `id:${scope}:${sender}:${messageId}` : "";
  const withoutMessageId = sender && text ? `txt:${scope}:${sender}:${text}` : "";

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

function summarizeWhatsappConversations(options = {}) {
  const normalizedTenantCode = normalizeTenantScopeCode(options?.tenantCode || "");
  const rows = normalizedTenantCode
    ? db.prepare(
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
                   AND u.tenant_code = m.tenant_code
                   AND u.role = 'user'
                   AND COALESCE(u.sender_name, '') <> ''
                 ORDER BY datetime(u.at) DESC, u.id DESC
                 LIMIT 1
               ) AS userSenderName,
               (
                 SELECT COUNT(*)
                 FROM whatsapp_messages c
                 WHERE c.phone = m.phone
                   AND c.tenant_code = m.tenant_code
               ) AS count
        FROM whatsapp_messages m
        JOIN (
          SELECT tenant_code, phone, MAX(id) AS max_id
          FROM whatsapp_messages
          WHERE tenant_code = ?
          GROUP BY tenant_code, phone
        ) latest ON latest.max_id = m.id
        ORDER BY datetime(m.at) DESC, m.id DESC
      `,
    ).all(normalizedTenantCode)
    : db.prepare(
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
                   AND u.tenant_code = m.tenant_code
                   AND u.role = 'user'
                   AND COALESCE(u.sender_name, '') <> ''
                 ORDER BY datetime(u.at) DESC, u.id DESC
                 LIMIT 1
               ) AS userSenderName,
               (
                 SELECT COUNT(*)
                 FROM whatsapp_messages c
                 WHERE c.phone = m.phone
                   AND c.tenant_code = m.tenant_code
               ) AS count
        FROM whatsapp_messages m
        JOIN (
          SELECT tenant_code, phone, MAX(id) AS max_id
          FROM whatsapp_messages
          GROUP BY tenant_code, phone
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

  for (const [cacheKey, messages] of whatsappConversations.entries()) {
    const parsed = parseWhatsappConversationCacheKey(cacheKey);
    const phone = parsed.phone;
    const tenantCode = parsed.tenantCode;
    if (!phone) {
      continue;
    }
    if (normalizedTenantCode && normalizedTenantCode !== tenantCode) {
      continue;
    }
    if (!normalizedTenantCode && tenantCode) {
      continue;
    }

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
  const tenantCode = normalizeTenantCode(fallback?.tenantCode || "");
  const scopePrefix = tenantCode
    ? `${tenantCode}:${Number(establishmentId)}`
    : String(Number(establishmentId));

  const phone = normalizePhone(customerContext?.phone || fallback.clientPhone || "");
  if (phone) {
    return `${scopePrefix}:phone:${phone}`;
  }

  const name = normalizeForMatch(customerContext?.name || fallback.clientName || "").trim();
  if (name) {
    return `${scopePrefix}:name:${name}`;
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

function buildHumanHandoffSessionKey(phone, tenantCode = "") {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) {
    return "";
  }

  const normalizedTenantCode = normalizeTenantScopeCode(tenantCode);
  return normalizedTenantCode ? `${normalizedTenantCode}:${normalizedPhone}` : `global:${normalizedPhone}`;
}

function cleanupHumanHandoffSessions(now = Date.now()) {
  for (const [key, value] of humanHandoffSessions.entries()) {
    if (!value || now > Number(value.expiresAt || 0)) {
      humanHandoffSessions.delete(key);
    }
  }
}

function getHumanHandoffSession(phone, tenantCode = "") {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) {
    return null;
  }

  cleanupHumanHandoffSessions();
  const scopedKey = buildHumanHandoffSessionKey(normalizedPhone, tenantCode);
  if (tenantCode) {
    return humanHandoffSessions.get(scopedKey) || null;
  }

  const matches = [];
  for (const value of humanHandoffSessions.values()) {
    if (normalizePhone(value?.phone || "") === normalizedPhone) {
      matches.push(value);
    }
  }

  if (!matches.length) {
    return humanHandoffSessions.get(scopedKey) || null;
  }

  matches.sort((left, right) => Number(right?.updatedAt || 0) - Number(left?.updatedAt || 0));
  return matches[0] || null;
}

function setHumanHandoffSession(phone, payload = {}) {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) {
    return null;
  }

  const normalizedTenantCode = normalizeTenantScopeCode(payload?.tenantCode || "");
  const sessionKey = buildHumanHandoffSessionKey(normalizedPhone, normalizedTenantCode);
  const now = Date.now();
  const value = {
    active: true,
    phone: normalizedPhone,
    tenantCode: normalizedTenantCode,
    createdAt: now,
    updatedAt: now,
    expiresAt: now + HUMAN_HANDOFF_TTL_MS,
    ...payload,
  };

  humanHandoffSessions.set(sessionKey, value);
  return value;
}

function clearHumanHandoffSession(phone, tenantCode = "") {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) {
    return false;
  }

  if (tenantCode) {
    return humanHandoffSessions.delete(buildHumanHandoffSessionKey(normalizedPhone, tenantCode));
  }

  let deleted = false;
  for (const key of [...humanHandoffSessions.keys()]) {
    if (key.endsWith(`:${normalizedPhone}`)) {
      humanHandoffSessions.delete(key);
      deleted = true;
    }
  }
  return deleted;
}

function buildBotSessionKey(phone, tenantCode = "") {
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) {
    return "";
  }
  const normalizedTenantCode = normalizeTenantScopeCode(tenantCode);
  return normalizedTenantCode ? `${normalizedTenantCode}:${normalizedPhone}` : normalizedPhone;
}

function cleanupBotAutoClosedSessions(now = Date.now()) {
  for (const [key, value] of botAutoClosedSessions.entries()) {
    if (!value || now > Number(value.expiresAt || 0)) {
      botAutoClosedSessions.delete(key);
    }
  }
}

function getBotAutoClosedSession(phone, tenantCode = "") {
  const key = buildBotSessionKey(phone, tenantCode);
  if (!key) {
    return null;
  }
  cleanupBotAutoClosedSessions();
  return botAutoClosedSessions.get(key) || null;
}

function setBotAutoClosedSession(phone, tenantCode = "", payload = {}) {
  const key = buildBotSessionKey(phone, tenantCode);
  if (!key) {
    return null;
  }

  const now = Date.now();
  const value = {
    active: true,
    phone: normalizePhone(phone),
    tenantCode: normalizeTenantScopeCode(tenantCode),
    createdAt: now,
    updatedAt: now,
    expiresAt: now + BOT_AUTOCLOSE_TTL_MS,
    ...payload,
  };
  botAutoClosedSessions.set(key, value);
  return value;
}

function clearBotAutoClosedSession(phone, tenantCode = "") {
  const key = buildBotSessionKey(phone, tenantCode);
  if (!key) {
    return false;
  }
  return botAutoClosedSessions.delete(key);
}

function detectInboundAutomationSignal(text = "") {
  const normalized = normalizeForMatch(text).replace(/\s+/g, " ").trim();
  if (!normalized) {
    return { score: 0, reasons: [] };
  }

  const reasons = [];
  let score = 0;
  const mark = (pattern, reason, weight = 1) => {
    if (pattern.test(normalized)) {
      reasons.push(reason);
      score += weight;
    }
  };

  mark(/\bj&t express\b/, "jt_express", 2);
  mark(/\btransportadora\b/, "transportadora", 2);
  mark(/\b(remessa|encomenda|logistica)\b/, "logistica", 1);
  mark(/\bpesquisa oficial\b/, "pesquisa_oficial", 2);
  mark(/\bexperiencia dos nossos clientes\b/, "pesquisa_experiencia", 1);
  mark(/\b(escreva|responda)\s+(sim|nao)\b/, "resposta_binaria", 2);
  mark(/\bclique nos botoes acima\b/, "botoes_acima", 2);
  mark(/\bagradecemos (o seu interesse|por compartilhar|sua colaboracao)\b/, "agradecimento_template", 1);
  mark(/\bparceira da empresa\b/, "template_parceira", 1);
  mark(/\bencaminhar o seu feedback\b/, "feedback_template", 1);

  return {
    score,
    reasons: [...new Set(reasons)],
  };
}

function countRecentAutomationHits(history = []) {
  if (!Array.isArray(history) || !history.length) {
    return 0;
  }
  return history
    .slice(-12)
    .filter((item) => String(item?.role || "").toLowerCase() === "user")
    .map((item) => detectInboundAutomationSignal(item?.content || item?.text || ""))
    .filter((signal) => Number(signal.score || 0) >= 2)
    .length;
}

function countRepeatedUserMessage(history = [], message = "") {
  const normalizedMessage = normalizeForMatch(message).replace(/\s+/g, " ").trim();
  if (!normalizedMessage || !Array.isArray(history)) {
    return 0;
  }

  return history
    .slice(-12)
    .filter((item) => String(item?.role || "").toLowerCase() === "user")
    .map((item) => normalizeForMatch(item?.content || item?.text || "").replace(/\s+/g, " ").trim())
    .filter((content) => content && content === normalizedMessage)
    .length;
}

function isExplicitHumanReopenMessage(text = "") {
  const normalized = normalizeForMatch(text);
  if (!normalized) {
    return false;
  }
  return (
    /\b(quero agendar|quero marcar|gostaria de agendar|sou cliente|sou uma cliente)\b/.test(normalized) ||
    /\b(escova|manicure|pedicure|depilacao|cabelo|corte|unha|sobrancelha)\b/.test(normalized) ||
    /\b(falar com atendente|falar com humano)\b/.test(normalized)
  );
}

function shouldAutoCloseBotConversation({
  message = "",
  history = [],
  knownClientName = "",
} = {}) {
  const signal = detectInboundAutomationSignal(message);
  const recentHits = countRecentAutomationHits(history);
  const repeatedCount = countRepeatedUserMessage(history, message);
  const hasKnownClient = Boolean(toNonEmptyString(knownClientName));

  const shouldClose = !hasKnownClient && (
    signal.score >= 3 ||
    (signal.score >= 2 && recentHits >= 1) ||
    recentHits >= 2 ||
    repeatedCount >= 1
  );

  return {
    shouldClose,
    signal,
    recentHits,
    repeatedCount,
    hasKnownClient,
  };
}

function buildBotAutoCloseMessage({ tenantName = "" } = {}) {
  const channelLabel = toNonEmptyString(tenantName)
    ? `do ${tenantName}`
    : "desta unidade";
  return [
    "Encerrando este atendimento automaticamente para evitar loop entre mensagens de robo.",
    "",
    `Motivo: identifiquei que este numero esta enviando mensagens automaticas de outro sistema (ex.: pesquisa/logistica), e este canal ${channelLabel} e exclusivo para atendimento de clientes.`,
    "",
    "Se voce for uma pessoa e quiser retomar, envie: QUERO AGENDAR.",
  ].join("\n");
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

function listCrmHumanHandoffTargets(crmSettings = {}) {
  const targets = [];
  const configured = normalizePhone(crmSettings?.humanHandoffInternalNumber || "");
  if (configured) {
    targets.push(configured);
  }
  for (const phone of listHumanAlertPhones()) {
    const normalized = normalizePhone(phone);
    if (normalized && !targets.includes(normalized)) {
      targets.push(normalized);
    }
  }
  return targets;
}

function buildCrmHumanHandoffClientMessage({ tenant, flow = null, crmSettings = {}, customerName = "" } = {}) {
  const humanNumber = toNonEmptyString(crmSettings?.humanHandoffClientNumber);
  const fallbackTemplate = humanNumber
    ? "Perfeito, {{client_name}}. Vou encaminhar seu atendimento para nossa equipe humana agora. Se preferir falar direto, este e o numero: {{human_number}}."
    : "Perfeito, {{client_name}}. Vou encaminhar seu atendimento para nossa equipe humana agora.";
  const template = toNonEmptyString(crmSettings?.humanHandoffMessageTemplate) || fallbackTemplate;
  const message = formatCrmStepMessage(template, {
    clientName: toNonEmptyString(customerName || flow?.client_name || flow?.clientName),
    serviceName: toNonEmptyString(flow?.origin_service_name || flow?.originServiceName),
    lastVisitAt: toNonEmptyString(flow?.last_visit_at || flow?.lastVisitAt),
    humanNumber,
  });
  return toNonEmptyString(message);
}

function buildCrmHumanHandoffInternalSummary({
  tenant,
  flow = null,
  customerPhone = "",
  customerName = "",
  customerMessage = "",
  triggerReason = "",
  initiatedBy = "",
} = {}) {
  const now = new Date();
  const tenantLabel = toNonEmptyString(tenant?.name || tenant?.code) || "Tenant sem nome";
  const clientLabel = toNonEmptyString(customerName || flow?.client_name || flow?.clientName) || "Cliente sem nome";
  const serviceLabel = toNonEmptyString(flow?.origin_service_name || flow?.originServiceName) || "-";
  const categoryLabel = toNonEmptyString(flow?.origin_category_name || flow?.originCategoryName) || "-";
  const lastVisitLabel = isoToBrDate(toNonEmptyString(flow?.last_visit_at || flow?.lastVisitAt))
    || toNonEmptyString(flow?.last_visit_at || flow?.lastVisitAt)
    || "-";
  const professionalName = toNonEmptyString(flow?.last_professional_name || flow?.lastProfessionalName) || "-";
  const professionalActive = flow?.last_professional_active == null && flow?.lastProfessionalActive == null
    ? null
    : (
      flow?.last_professional_active == null
        ? Boolean(flow?.lastProfessionalActive)
        : Number(flow?.last_professional_active) !== 0
    );
  const professionalStatus = professionalActive == null ? "nao validado" : (professionalActive ? "ativo" : "inativo");
  const flowStep = Number(flow?.current_step || flow?.currentStep || 0);
  const flowStatus = toNonEmptyString(flow?.flow_status || flow?.flowStatus) || "-";

  return [
    "CRM - HANDOFF HUMANO",
    `Tenant: ${tenantLabel}`,
    `Cliente: ${clientLabel}`,
    `WhatsApp: ${normalizePhone(customerPhone) || "-"}`,
    `Servico de origem: ${serviceLabel}`,
    `Categoria: ${categoryLabel}`,
    `Ultima visita: ${lastVisitLabel}`,
    `Ultimo profissional: ${professionalName} (${professionalStatus})`,
    `Etapa/status do fluxo: ${flowStep} / ${flowStatus}`,
    `Motivo: ${toNonEmptyString(triggerReason) || "Solicitacao de atendimento humano"}`,
    `Mensagem da cliente: ${toNonEmptyString(customerMessage) || "-"}`,
    `Acionado por: ${toNonEmptyString(initiatedBy) || "sistema"}`,
    `Horario: ${new Intl.DateTimeFormat("pt-BR", {
      timeZone: "America/Sao_Paulo",
      dateStyle: "short",
      timeStyle: "short",
    }).format(now)}`,
  ].join("\n");
}

async function notifyCrmHumanHandoffTargets({
  instance,
  tenantCode = "",
  targets = [],
  text = "",
} = {}) {
  const normalizedTargets = [...new Set((Array.isArray(targets) ? targets : []).map((item) => normalizePhone(item)).filter(Boolean))];
  if (!normalizedTargets.length || !toNonEmptyString(text)) {
    return { sent: 0, failed: 0, targets: normalizedTargets, details: { sent: [], failed: [] } };
  }

  const sent = [];
  const failed = [];
  for (const target of normalizedTargets) {
    try {
      const result = await evolutionRequest(`/message/sendText/${instance}`, {
        method: "POST",
        body: { number: target, text },
        tenantCode,
        instanceName: instance,
      });
      sent.push({ phone: target, result });
    } catch (error) {
      failed.push({
        phone: target,
        message: error?.message || "Erro ao enviar resumo interno.",
        status: error?.status || null,
      });
    }
  }

  return {
    sent: sent.length,
    failed: failed.length,
    targets: normalizedTargets,
    details: { sent, failed },
  };
}

async function activateCrmHumanHandoff({
  tenant,
  flow = null,
  crmSettings = {},
  instance = "",
  phone = "",
  customerName = "",
  customerMessage = "",
  triggerSource = "",
  triggerReason = "",
  initiatedBy = "",
} = {}) {
  if (!tenant?.id || !tenant?.code) {
    const error = new Error("Tenant invalido para handoff do CRM.");
    error.status = 400;
    throw error;
  }

  const normalizedPhone = normalizePhone(phone || flow?.phone || "");
  if (!normalizedPhone) {
    const error = new Error("Telefone da cliente ausente para handoff do CRM.");
    error.status = 400;
    throw error;
  }

  const resolvedInstance = toNonEmptyString(instance) || resolveEvolutionInstance(null, { tenantCode: tenant.code });
  if (!resolvedInstance) {
    const error = new Error("Instancia Evolution nao configurada para handoff do CRM.");
    error.status = 500;
    throw error;
  }

  const resolvedSettings = crmSettings && typeof crmSettings === "object"
    ? crmSettings
    : (getTenantCrmSettingsByCode(tenant.code)?.config || getDefaultCrmSettings());
  const shouldPauseAi = resolvedSettings.humanHandoffPauseAi == null
    ? true
    : Boolean(resolvedSettings.humanHandoffPauseAi);
  const resolvedCustomerName = toNonEmptyString(customerName || flow?.client_name || flow?.clientName);

  const clientMessage = buildCrmHumanHandoffClientMessage({
    tenant,
    flow,
    crmSettings: resolvedSettings,
    customerName: resolvedCustomerName,
  });
  if (clientMessage) {
    await evolutionRequest(`/message/sendText/${resolvedInstance}`, {
      method: "POST",
      body: { number: normalizedPhone, text: clientMessage },
      tenantCode: tenant.code,
      instanceName: resolvedInstance,
    });
    pushWhatsappHistory(normalizedPhone, "assistant", clientMessage, "", {
      tenantCode: tenant.code,
    });
  }

  const summaryText = buildCrmHumanHandoffInternalSummary({
    tenant,
    flow,
    customerPhone: normalizedPhone,
    customerName: resolvedCustomerName,
    customerMessage,
    triggerReason,
    initiatedBy,
  });
  const internalTargets = resolvedSettings.humanHandoffSendInternalSummary === false
    ? []
    : listCrmHumanHandoffTargets(resolvedSettings);
  const internalAlert = await notifyCrmHumanHandoffTargets({
    instance: resolvedInstance,
    tenantCode: tenant.code,
    targets: internalTargets,
    text: summaryText,
  });

  const session = shouldPauseAi
    ? setHumanHandoffSession(normalizedPhone, {
      source: triggerSource || "crm",
      reason: triggerReason || "Handoff CRM",
      establishmentId: tenant.establishmentId || null,
      customerName: resolvedCustomerName,
      messageId: "",
      flowId: Number.isFinite(Number(flow?.id)) ? Number(flow.id) : null,
      tenantCode: tenant.code,
    })
    : null;

  if (flow?.id) {
    if (shouldPauseAi) {
      stopCrmFlowForSystemReason(flow.id, tenant.id, "human_handoff", {
        source: triggerSource || "crm",
        initiatedBy: toNonEmptyString(initiatedBy),
        phone: normalizedPhone,
      });
    }
    recordCrmFlowEvent({
      flowId: flow.id,
      tenantId: tenant.id,
      eventType: "human_handoff",
      step: Number(flow.current_step || flow.currentStep || 0) || null,
      metadata: {
        source: toNonEmptyString(triggerSource) || "crm",
        reason: toNonEmptyString(triggerReason) || "Handoff CRM",
        initiatedBy: toNonEmptyString(initiatedBy),
        clientMessageSent: Boolean(clientMessage),
        internalTargets: internalAlert.targets || [],
        internalSummarySentCount: Number(internalAlert.sent || 0),
        pauseAi: shouldPauseAi,
      },
      messagePreview: clientMessage.slice(0, 200),
      messageSent: clientMessage,
      replySummary: toNonEmptyString(customerMessage).slice(0, 300),
    });
  }

  return {
    clientMessage,
    internalAlert,
    shouldPauseAi,
    session,
    instance: resolvedInstance,
  };
}

function detectConfirmationIntent(message) {
  const normalized = normalizeForMatch(message);
  if (!normalized) {
    return "none";
  }

  if (
    /\b(nao|nÃ£o|negativo|melhor nao|melhor nao|cancelar|cancela|desmarcar|desmarca|mudar|trocar|corrigir)\b/.test(
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

function historyHasRecentBookingConfirmationPrompt(history = []) {
  if (!Array.isArray(history) || !history.length) {
    return false;
  }

  const recent = history.slice(-8);
  for (let index = recent.length - 1; index >= 0; index -= 1) {
    const item = recent[index];
    const role = toNonEmptyString(item?.role).toLowerCase();
    if (role !== "assistant") {
      continue;
    }

    const content = normalizeForMatch(item?.content || item?.text || "");
    if (!content) {
      continue;
    }

    if (
      /\b(confirma|confirmar|podemos confirmar|posso confirmar|responda sim|responda "sim"|se estiver certo responda)\b/.test(
        content,
      ) &&
      /\b(agendamento|agendar|servico|horario|data)\b/.test(content)
    ) {
      return true;
    }
  }

  return false;
}

function textLooksLikeBookingConfirmationRequest(text = "") {
  const normalized = normalizeForMatch(text);
  if (!normalized) {
    return false;
  }
  if (
    /\b(cancel|cancelar|cancelamento|desmarc|reagend|remarcar|alterar|alteracao|mudar horario|trocar horario)\b/.test(
      normalized,
    )
  ) {
    return false;
  }
  return (
    /\b(podemos confirmar|posso confirmar|confirma estes|confirma este|se estiver certo responda|responda sim)\b/.test(
      normalized,
    ) &&
    /\b(agendamento|agendar|servic|horario|data)\b/.test(normalized)
  );
}

function historyHasRecentChangeConfirmationPrompt(history = []) {
  if (!Array.isArray(history) || !history.length) {
    return false;
  }

  const recent = history.slice(-8);
  for (let index = recent.length - 1; index >= 0; index -= 1) {
    const item = recent[index];
    const role = toNonEmptyString(item?.role).toLowerCase();
    if (role !== "assistant") {
      continue;
    }

    const content = normalizeForMatch(item?.content || item?.text || "");
    if (!content) {
      continue;
    }

    if (
      /\b(confirme|confirma|isso mesmo|responda sim|se sim|se estiver certo)\b/.test(content) &&
      /\b(cancel|cancelar|cancelamento|desmarc|reagend|remarcar|alterar)\b/.test(content)
    ) {
      return true;
    }
  }

  return false;
}

function extractIsoDateFromText(text, fallbackIsoDate = "") {
  const raw = toNonEmptyString(text);
  if (!raw) {
    return "";
  }

  const isoMatch = raw.match(/\b(20\d{2})-(\d{2})-(\d{2})\b/);
  if (isoMatch) {
    return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
  }

  const brMatch = raw.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/);
  if (!brMatch) {
    return "";
  }

  const day = Number(brMatch[1]);
  const month = Number(brMatch[2]);
  let year = Number(brMatch[3] || "");

  if (!Number.isFinite(year) || !year) {
    const fallbackYear = Number(toNonEmptyString(fallbackIsoDate).slice(0, 4));
    year = Number.isFinite(fallbackYear) && fallbackYear > 0
      ? fallbackYear
      : Number(getSaoPauloDateContext().isoToday.slice(0, 4));
  } else if (year < 100) {
    year += 2000;
  }

  if (!Number.isFinite(day) || !Number.isFinite(month) || day < 1 || day > 31 || month < 1 || month > 12) {
    return "";
  }

  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function normalizeRecoveredProfessionalName(value) {
  return professionalDisplayName(
    toNonEmptyString(value)
      .replace(/^[\-\u2022\*]+/, "")
      .replace(/\s+/g, " ")
      .trim(),
  );
}

function normalizeRecoveredServiceName(value) {
  return toNonEmptyString(value)
    .replace(/^[\-\u2022\*]+/, "")
    .replace(/\s+/g, " ")
    .trim();
}

function parseIsoDateFromFlexibleToken(token = "", fallbackIsoDate = "") {
  const raw = toNonEmptyString(token);
  if (!raw) {
    return "";
  }

  const directIso = extractIsoDateFromText(raw, fallbackIsoDate);
  if (directIso) {
    return directIso;
  }

  const normalized = normalizeForMatch(raw);
  const dayOnlyMatch = normalized.match(/\bdia\s*(\d{1,2})\b/);
  if (!dayOnlyMatch) {
    return "";
  }

  const day = Number(dayOnlyMatch[1]);
  if (!Number.isFinite(day) || day < 1 || day > 31) {
    return "";
  }

  const baseIso = /^\d{4}-\d{2}-\d{2}$/.test(toNonEmptyString(fallbackIsoDate))
    ? toNonEmptyString(fallbackIsoDate)
    : getSaoPauloDateContext().isoToday;
  const baseYear = Number(baseIso.slice(0, 4));
  const baseMonth = Number(baseIso.slice(5, 7));
  if (!Number.isFinite(baseYear) || !Number.isFinite(baseMonth) || baseMonth < 1 || baseMonth > 12) {
    return "";
  }

  return `${String(baseYear).padStart(4, "0")}-${String(baseMonth).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function extractRecoveredServiceAndDate(serviceLabel = "", fallbackIsoDate = "") {
  const rawService = normalizeRecoveredServiceName(serviceLabel);
  if (!rawService) {
    return {
      service: "",
      date: toNonEmptyString(fallbackIsoDate),
    };
  }

  let service = rawService.replace(/^servi[cç]o\b[:\-\s]*/i, "").trim();
  let resolvedDate = "";

  const datePattern = /\b(?:dia\s*)?\d{1,2}(?:[\/\-]\d{1,2}(?:[\/\-]\d{2,4})?)?\b/i;
  const dateMatch = service.match(datePattern);
  if (dateMatch?.[0]) {
    resolvedDate = parseIsoDateFromFlexibleToken(dateMatch[0], fallbackIsoDate);
    service = service.replace(dateMatch[0], " ");
  }

  service = service
    .replace(/\b(?:dia|data|horario|hora)\b/gi, " ")
    .replace(/[,:;]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!service) {
    service = rawService;
  }

  return {
    service,
    date: resolvedDate || toNonEmptyString(fallbackIsoDate),
  };
}

function looksLikeBookingServiceLabel(value = "") {
  const normalized = normalizeForMatch(value);
  if (!normalized) {
    return false;
  }
  return /\b(escova|corte|colora|tonaliz|reflex|manicure|pedicure|depil|hidrat|reconstr|maqui|penteado|sobrancel|unha|mao|pe)\b/.test(
    normalized,
  );
}

function recoverBookingDraftFromHistory(history = [], fallbackIsoDate = "") {
  if (!Array.isArray(history) || !history.length) {
    return { items: [], source: "" };
  }

  const recent = history.slice(-12);
  for (let index = recent.length - 1; index >= 0; index -= 1) {
    const item = recent[index];
    const content = toNonEmptyString(item?.content || item?.text || "");
    if (!content) {
      continue;
    }

    const lines = content
      .replace(/\r/g, "")
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);
    if (!lines.length) {
      continue;
    }

    const resolvedDate = extractIsoDateFromText(content, fallbackIsoDate) || toNonEmptyString(fallbackIsoDate);
    if (!resolvedDate) {
      continue;
    }

    const recovered = [];
    for (const line of lines) {
      const normalizedLine = normalizeForMatch(line).replace(/\*/g, " ").replace(/\s+/g, " ").trim();
      if (!normalizedLine.includes(" com ") || !/(?:\bas|\ba|\?s)\s+\d{1,2}(?::\d{2}|h\d{0,2})?\b/.test(normalizedLine)) {
        continue;
      }

      const serviceMatch = normalizedLine.match(
        /^(.+?)\s+com\s+(?:a|o)?\s*([a-z][a-z\s.'-]{1,50})(?:\s+em\s+\d{1,2}[\/\-]\d{1,2}(?:[\/\-]\d{2,4})?)?\s+(?:\bas|\ba|\?s)\s*(\d{1,2})(?::|h)?(\d{2})?/i,
      );
      if (!serviceMatch) {
        continue;
      }

      const recoveredService = extractRecoveredServiceAndDate(serviceMatch[1], resolvedDate);
      const rawService = normalizeRecoveredServiceName(recoveredService.service);
      const itemDate = toNonEmptyString(recoveredService.date) || resolvedDate;
      const rawProfessional = normalizeRecoveredProfessionalName(serviceMatch[2]);
      const hour = Number(serviceMatch[3]);
      const minute = Number(serviceMatch[4] || "0");
      const parsedTime = normalizeTimeValue(
        `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
      );

      if (!rawService || !looksLikeBookingServiceLabel(rawService) || !rawProfessional || !parsedTime || !itemDate) {
        continue;
      }

      recovered.push({
        service: rawService,
        date: itemDate,
        time: parsedTime,
        professionalName: rawProfessional,
      });
    }

    if (!recovered.length) {
      const merged = normalizeForMatch(content)
        .replace(/\*/g, " ")
        .replace(/\s+/g, " ")
        .trim();
      const globalPattern = /([a-z][a-z0-9\s\/-]{2,40}?)\s+com\s+(?:a|o)?\s*([a-z][a-z\s.'-]{1,50})\s+(?:\bas|\ba|\?s)\s*(\d{1,2})(?::|h)?(\d{2})?/gi;
      for (const match of merged.matchAll(globalPattern)) {
        const recoveredService = extractRecoveredServiceAndDate(match[1], resolvedDate);
        const rawService = normalizeRecoveredServiceName(recoveredService.service);
        const itemDate = toNonEmptyString(recoveredService.date) || resolvedDate;
        const rawProfessional = normalizeRecoveredProfessionalName(match[2]);
        const hour = Number(match[3]);
        const minute = Number(match[4] || "0");
        const parsedTime = normalizeTimeValue(
          `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
        );
        if (!rawService || !looksLikeBookingServiceLabel(rawService) || !rawProfessional || !parsedTime || !itemDate) {
          continue;
        }
        recovered.push({
          service: rawService,
          date: itemDate,
          time: parsedTime,
          professionalName: rawProfessional,
        });
      }
    }

    if (recovered.length) {
      const unique = new Map();
      for (const entry of recovered) {
        unique.set(
          `${normalizeForMatch(entry.service)}|${normalizeForMatch(entry.professionalName)}|${entry.date}|${entry.time}`,
          entry,
        );
      }
      return { items: [...unique.values()], source: content };
    }
  }

  return { items: [], source: "" };
}

function isBookingStatusInquiry(message) {
  const normalized = normalizeForMatch(message);
  if (!normalized) {
    return false;
  }

  return [
    "conseguiu",
    "deu certo",
    "confirmou",
    "ja confirmou",
    "ja agendou",
    "status",
    "e ai",
  ].some((token) => normalized.includes(token));
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

function shouldEnforceMaxDaysAheadBookingLimit({ tenantCode = "", establishmentId = null } = {}) {
  const normalizedTenant = normalizeTenantCode(tenantCode);
  if (normalizedTenant) {
    const crmSettings = getTenantCrmSettingsByCode(normalizedTenant);
    const configured = Number(crmSettings?.config?.bookingMaxDaysAhead);
    if (Number.isFinite(configured) && configured > 0) {
      return true;
    }
  }
  if (normalizedTenant && BOOKING_MAX_DAYS_AHEAD_TENANTS.has(normalizedTenant)) {
    return true;
  }

  const parsedEstablishmentId = Number(establishmentId);
  if (Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0) {
    if (BOOKING_MAX_DAYS_AHEAD_ESTABLISHMENTS.has(parsedEstablishmentId)) {
      return true;
    }
    const tenant = getActiveTenantByEstablishmentId(parsedEstablishmentId);
    const configured = Number(getTenantCrmSettingsByCode(tenant?.code || "")?.config?.bookingMaxDaysAhead);
    if (Number.isFinite(configured) && configured > 0) {
      return true;
    }
    if (BOOKING_MAX_DAYS_AHEAD_TENANTS.has(normalizeTenantCode(tenant?.code || ""))) {
      return true;
    }
  }

  return false;
}

function resolveBookingMaxDaysAhead({ tenantCode = "", establishmentId = null } = {}) {
  const normalizedTenant = normalizeTenantCode(tenantCode);
  if (normalizedTenant) {
    const crmSettings = getTenantCrmSettingsByCode(normalizedTenant);
    const configured = Number(crmSettings?.config?.bookingMaxDaysAhead);
    if (Number.isFinite(configured) && configured > 0) {
      return Math.min(365, Math.max(1, Math.trunc(configured)));
    }
  }

  const parsedEstablishmentId = Number(establishmentId);
  if (Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0) {
    const tenant = getActiveTenantByEstablishmentId(parsedEstablishmentId);
    if (tenant?.code) {
      return resolveBookingMaxDaysAhead({ tenantCode: tenant.code });
    }
  }

  return BOOKING_MAX_DAYS_AHEAD;
}

function assertBookingWithinMaxDaysAhead({
  date,
  tenantCode = "",
  establishmentId = null,
  maxDaysAhead = null,
} = {}) {
  const normalizedDate = normalizeBookingDate(date);
  if (!normalizedDate) {
    return;
  }
  if (!shouldEnforceMaxDaysAheadBookingLimit({ tenantCode, establishmentId })) {
    return;
  }

  const todayIso = getSaoPauloDateContext().isoToday;
  const effectiveMaxDaysAhead = Number.isFinite(Number(maxDaysAhead)) && Number(maxDaysAhead) > 0
    ? Number(maxDaysAhead)
    : resolveBookingMaxDaysAhead({ tenantCode, establishmentId });
  const maxAllowedIso = addDaysToIsoDate(todayIso, effectiveMaxDaysAhead);
  if (!maxAllowedIso) {
    return;
  }

  if (normalizedDate > maxAllowedIso) {
    const maxAllowedBr = isoToBrDate(maxAllowedIso) || maxAllowedIso;
    const error = new Error(
      `Para este atendimento, consigo agendar somente ate ${maxAllowedBr} (${maxDaysAhead} dias a partir de hoje).`,
    );
    error.status = 422;
    error.details = {
      code: "booking_window_exceeded",
      maxDaysAhead,
      requestedDate: normalizedDate,
      todayIso,
      maxAllowedDate: maxAllowedIso,
    };
    throw error;
  }
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
  tenantCode = "",
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
          tenant_code, event_type, status, establishment_id, appointment_id, confirmation_code,
          client_phone, client_name, service_name, professional_name,
          appointment_date, appointment_time, request_payload, response_payload,
          error_message, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    ).run(
      normalizeTenantScopeCode(tenantCode),
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

// DecompÃƒÂµe um telefone brasileiro em { ddi, ddd, numero } para criaÃƒÂ§ÃƒÂ£o de clientes na Trinks
function parseBrazilianPhone(phone) {
  const digits = normalizePhone(phone);
  if (!digits) return null;

  // Remove DDI 55 se presente no inÃƒÂ­cio (55 + 10 ou 11 dÃƒÂ­gitos = 12 ou 13 dÃƒÂ­gitos)
  let local = digits;
  if (local.length >= 12 && local.startsWith("55")) {
    local = local.slice(2);
  }

  // Extrai DDD (2 dÃƒÂ­gitos) + nÃƒÂºmero (8 ou 9 dÃƒÂ­gitos)
  if (local.length >= 10) {
    const ddd = local.slice(0, 2);
    const numero = local.slice(2);
    return { ddi: "55", ddd, numero };
  }

  // Sem DDD reconhecÃƒÂ­vel Ã¢â‚¬â€ retorna sÃƒÂ³ o nÃƒÂºmero
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

async function getClientById(estabelecimentoId, clientId) {
  const parsedClientId = Number(clientId);
  if (!estabelecimentoId || !Number.isFinite(parsedClientId) || parsedClientId <= 0) {
    return null;
  }

  return trinksRequest(`/clientes/${parsedClientId}`, {
    method: "GET",
    estabelecimentoId,
  });
}

function extractPreferredPhoneFromClient(item) {
  const candidates = [];
  if (Array.isArray(item?.telefone)) {
    candidates.push(...item.telefone);
  }
  if (Array.isArray(item?.telefones)) {
    candidates.push(...item.telefones);
  }
  if (Array.isArray(item?.phones)) {
    candidates.push(...item.phones);
  }
  if (item?.celular) {
    candidates.push(item.celular);
  }
  if (item?.phone) {
    candidates.push(item.phone);
  }

  for (const candidate of candidates) {
    const normalized = typeof candidate === "object"
      ? normalizeTrinksPhone(candidate)
      : normalizePhone(candidate);
    if (normalized) {
      return normalized;
    }
  }

  return "";
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

function joinHumanNames(names = []) {
  const cleaned = Array.isArray(names)
    ? names.map((name) => toNonEmptyString(name)).filter(Boolean)
    : [];
  if (!cleaned.length) {
    return "";
  }
  if (cleaned.length === 1) {
    return cleaned[0];
  }
  if (cleaned.length === 2) {
    return `${cleaned[0]} e ${cleaned[1]}`;
  }
  return `${cleaned.slice(0, -1).join(", ")} e ${cleaned[cleaned.length - 1]}`;
}

function resolveTenantDisplayName(knowledge, tenantCode = "") {
  const tenant = getTenantByCode(tenantCode);
  const fromTenant = toNonEmptyString(tenant?.name);
  if (fromTenant) {
    return fromTenant;
  }
  return firstNonEmpty([
    knowledge?.identity?.brandName,
    knowledge?.business?.name,
    knowledge?.identity?.name,
  ]);
}

function buildRestrictedProfessionalMessage({ allowedProfessionalNames = [] } = {}) {
  const displayNames = uniqueProfessionalDisplayNames(allowedProfessionalNames);
  if (!displayNames.length) {
    return "No momento, consigo ajudar apenas com agenda dos profissionais habilitados desta unidade. Sobre outros profissionais, nao tenho informacao para confirmar por este canal.";
  }
  return `No momento, consigo ajudar apenas com agenda de ${joinHumanNames(displayNames)}. Sobre outros profissionais, nao tenho informacao para confirmar por este canal.`;
}

function buildBookingSingleMessageRetryHint({ allowedProfessionalNames = [] } = {}) {
  const displayNames = uniqueProfessionalDisplayNames(allowedProfessionalNames);
  if (displayNames.length >= 2) {
    return `Perfeito. Podemos continuar por partes. Me diga so o que falta ajustar ou confirmar, como servico, data, horario ou profissional. Se quiser, voce pode citar algo como ${displayNames[0]} ou ${displayNames[1]}.`;
  }
  if (displayNames.length === 1) {
    return `Perfeito. Podemos continuar por partes. Me diga so o que falta ajustar ou confirmar, como servico, data, horario ou profissional. Se quiser, voce pode me informar a profissional ${displayNames[0]}.`;
  }
  return "Perfeito. Podemos continuar por partes. Me diga so o que falta ajustar ou confirmar: servico, data, horario ou profissional.";
}

function normalizeProfessionalConstraintName(value) {
  return normalizeForMatch(toNonEmptyString(value));
}

function extractAllowedProfessionalNames(knowledge) {
  if (!knowledge || typeof knowledge !== "object" || Array.isArray(knowledge)) {
    return [];
  }

  const candidateArrays = [
    knowledge?.allowedProfessionals,
    knowledge?.operations?.allowedProfessionals,
    knowledge?.business?.allowedProfessionals,
    knowledge?.rules?.allowedProfessionals,
  ];

  const flat = candidateArrays
    .flatMap((value) => (Array.isArray(value) ? value : []))
    .map((item) => toNonEmptyString(item))
    .filter(Boolean);

  return [...new Set(flat)];
}

function professionalMatchesAllowedList(name, allowedNames = []) {
  if (!Array.isArray(allowedNames) || !allowedNames.length) {
    return true;
  }

  const normalizedCandidate = normalizeProfessionalConstraintName(name);
  const candidateFirstName = clientFirstName(name);
  const normalizedCandidateFirstName = normalizeProfessionalConstraintName(candidateFirstName);
  if (!normalizedCandidate && !normalizedCandidateFirstName) {
    return false;
  }

  return allowedNames.some((allowed) => {
    const normalizedAllowed = normalizeProfessionalConstraintName(allowed);
    const allowedFirstName = clientFirstName(allowed);
    const normalizedAllowedFirstName = normalizeProfessionalConstraintName(allowedFirstName);

    if (!normalizedAllowed && !normalizedAllowedFirstName) {
      return false;
    }

    return (
      (normalizedAllowed && normalizedCandidate.includes(normalizedAllowed)) ||
      (normalizedAllowed && normalizedAllowed.includes(normalizedCandidate)) ||
      (normalizedAllowedFirstName && normalizedCandidateFirstName === normalizedAllowedFirstName) ||
      (normalizedAllowedFirstName && normalizedCandidate === normalizedAllowedFirstName)
    );
  });
}

function filterProfessionalsByAllowedList(professionals = [], allowedNames = []) {
  if (!Array.isArray(professionals)) {
    return [];
  }
  if (!Array.isArray(allowedNames) || !allowedNames.length) {
    return professionals;
  }
  return professionals.filter((item) =>
    professionalMatchesAllowedList(item?.name || professionalNameFrom(item), allowedNames));
}

function filterProfessionalNamesByAllowedList(names = [], allowedNames = []) {
  if (!Array.isArray(names)) {
    return [];
  }
  if (!Array.isArray(allowedNames) || !allowedNames.length) {
    return names;
  }
  return names.filter((name) => professionalMatchesAllowedList(name, allowedNames));
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

function normalizeMarketingActionType(value) {
  const normalized = normalizeForMatch(value).replace(/\s+/g, "_");
  if (!normalized) {
    return "custom";
  }

  if (normalized === "upsell") {
    return "upsell";
  }

  if (normalized === "cross_sell" || normalized === "crosssell") {
    return "cross_sell";
  }

  return "custom";
}

function normalizeMarketingActionTrigger(value) {
  const normalized = normalizeForMatch(value).replace(/\s+/g, "_");
  if (!normalized) {
    return "before_closing";
  }

  if (
    normalized === "before_closing"
    || normalized === "encerramento"
    || normalized === "finalizacao"
    || normalized === "before_farewell"
  ) {
    return "before_closing";
  }

  if (normalized === "always" || normalized === "all_messages" || normalized === "every_message") {
    return "always";
  }

  return "before_closing";
}

function normalizeMarketingActionEndDate(value) {
  const raw = toNonEmptyString(value);
  if (!raw) {
    return "";
  }

  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
  }

  const brMatch = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (brMatch) {
    return `${brMatch[3]}-${brMatch[2]}-${brMatch[1]}`;
  }

  return "";
}

function isMarketingActionWithinDate(action, todayIso = "") {
  const untilDate = toNonEmptyString(action?.endDate);
  if (!untilDate) {
    return true;
  }
  const today = toNonEmptyString(todayIso) || getSaoPauloDateContext().isoToday;
  if (!today) {
    return true;
  }
  return today <= untilDate;
}

function normalizeMarketingConfig(knowledge) {
  const source = knowledge?.marketing && typeof knowledge.marketing === "object"
    ? knowledge.marketing
    : {};
  const actions = Array.isArray(source?.actions) ? source.actions : [];

  const normalizedActions = actions
    .map((item, index) => {
      if (!item || typeof item !== "object") {
        return null;
      }

      const id = toNonEmptyString(item.id) || `marketing_action_${index + 1}`;
      const name = toNonEmptyString(item.name) || `Acao ${index + 1}`;
      const message = toNonEmptyString(item.message || item.text || item.offer);
      const mediaUrl = toNonEmptyString(item.mediaUrl || item.imageUrl || item.photoUrl);
      const mediaCaption = toNonEmptyString(item.mediaCaption || item.caption);
      const endDate = normalizeMarketingActionEndDate(
        item.endDate || item.validUntil || item.untilDate || item.dateUntil || item.ate,
      );
      if (!message) {
        return null;
      }

      return {
        id,
        name,
        enabled: item.enabled == null ? true : Boolean(item.enabled),
        type: normalizeMarketingActionType(item.type),
        trigger: normalizeMarketingActionTrigger(item.trigger),
        message,
        mediaUrl,
        mediaCaption,
        endDate,
      };
    })
    .filter(Boolean);

  return {
    enabled: source?.enabled == null ? false : Boolean(source.enabled),
    actions: normalizedActions,
  };
}

function cleanupMarketingActionSessions(now = Date.now()) {
  for (const [key, value] of marketingActionSessions.entries()) {
    if (!value) {
      marketingActionSessions.delete(key);
      continue;
    }

    const sentAt = Number(value.sentAt || 0);
    if (!sentAt || now - sentAt > MARKETING_ACTION_SESSION_TTL_MS) {
      marketingActionSessions.delete(key);
    }
  }
}

function textSuggestsConversationClosing(text) {
  const normalized = normalizeForMatch(text);
  if (!normalized) {
    return false;
  }

  return (
    /\b(mais alguma coisa|algo mais|se precisar|fico a disposicao|estou a disposicao)\b/.test(normalized)
    || /\b(ate logo|ate mais|tchau|obrigad|tenha um otimo dia|tenha um excelente dia)\b/.test(normalized)
    || /\b(encerrar|finalizar atendimento)\b/.test(normalized)
    || /\b(agendamento confirmado com sucesso)\b/.test(normalized)
  );
}

function pickMarketingActionForStage(knowledge, stage = "before_closing") {
  const config = normalizeMarketingConfig(knowledge);
  if (!config.enabled) {
    return null;
  }
  const todayIso = getSaoPauloDateContext().isoToday;

  return (
    config.actions.find((item) => item.enabled && item.trigger === stage && isMarketingActionWithinDate(item, todayIso))
    || config.actions.find((item) => item.enabled && item.trigger === "always" && isMarketingActionWithinDate(item, todayIso))
    || null
  );
}

function applyMarketingActionBeforeClosing({
  knowledge,
  customerMessage = "",
  assistantReply = "",
  sessionKey = "",
} = {}) {
  const replyText = toNonEmptyString(assistantReply);
  if (!replyText) {
    return {
      text: replyText,
      marketingMedia: null,
    };
  }

  const action = pickMarketingActionForStage(knowledge, "before_closing");
  if (!action) {
    return {
      text: replyText,
      marketingMedia: null,
    };
  }

  const customerWantsToClose = textSuggestsConversationClosing(customerMessage);
  const assistantIsClosing = textSuggestsConversationClosing(replyText);
  const shouldGateByClosingSignal = action.trigger !== "always";
  if (shouldGateByClosingSignal && !customerWantsToClose && !assistantIsClosing) {
    return {
      text: replyText,
      marketingMedia: null,
    };
  }

  const normalizedReply = normalizeForMatch(replyText);
  const normalizedOfferMessage = normalizeForMatch(action.message);
  if (normalizedOfferMessage && normalizedReply.includes(normalizedOfferMessage)) {
    return {
      text: replyText,
      marketingMedia: null,
    };
  }

  const actionSignature = [
    toNonEmptyString(action.id),
    toNonEmptyString(action.message),
    toNonEmptyString(action.mediaUrl),
    toNonEmptyString(action.mediaCaption),
    toNonEmptyString(action.endDate),
  ].join("|");

  if (sessionKey) {
    cleanupMarketingActionSessions();
    const sentState = marketingActionSessions.get(sessionKey);
    if (
      sentState?.actionId === action.id
      && toNonEmptyString(sentState?.actionSignature || "") === actionSignature
    ) {
      return {
        text: replyText,
        marketingMedia: null,
      };
    }

    marketingActionSessions.set(sessionKey, {
      actionId: action.id,
      actionSignature,
      sentAt: Date.now(),
    });
  }

  const hasMedia = Boolean(toNonEmptyString(action.mediaUrl));
  if (hasMedia) {
    return {
      text: replyText,
      marketingMedia: {
        actionId: action.id,
        actionName: action.name,
        url: toNonEmptyString(action.mediaUrl),
        caption: toNonEmptyString(action.mediaCaption || action.message),
      },
    };
  }

  return {
    text: `Antes de finalizarmos: ${action.message}\n\n${replyText}`,
    marketingMedia: null,
  };
}

function formatKnowledgeForPrompt(knowledge, tenantCode = "") {
  const identity = knowledge?.identity || {};
  const policies = knowledge?.policies || {};
  const business = knowledge?.business || {};
  const services = Array.isArray(knowledge?.services) ? knowledge.services : [];
  const faq = Array.isArray(knowledge?.faq) ? knowledge.faq : [];
  const marketing = normalizeMarketingConfig(knowledge);
  const toneGuide = extractKnowledgeToneGuide(knowledge);

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

  const marketingText = marketing.actions.length
    ? marketing.actions
        .map(
          (item) =>
            `- ${item.name} | tipo: ${item.type} | trigger: ${item.trigger} | ativo: ${item.enabled ? "sim" : "nao"} | validade ate: ${item.endDate || "sem limite"} | oferta: ${item.message} | imagem: ${item.mediaUrl ? "sim" : "nao"}`,
        )
        .join("\n")
    : "- Sem acoes de marketing cadastradas";

  return [
    "Base de conhecimento do salao (fonte oficial):",
    `- Nome comercial: ${identity?.brandName || "Nao informado"}`,
    `- Nome da concierge digital: ${resolveConciergeDisplayName(knowledge, tenantCode)}`,
    `- Tom das conversas: ${toneGuide}`,
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
    "Marketing e ofertas:",
    `- Ambiente de marketing habilitado: ${marketing.enabled ? "sim" : "nao"}`,
    marketingText,
    "",
    "Regra: sempre priorize esta base para responder duvidas comerciais do salao.",
  ].join("\n");
}

function resolveConciergeDisplayName(knowledge, tenantCode = "") {
  const explicitName = toNonEmptyString(knowledge?.identity?.assistantName);
  if (explicitName) {
    return explicitName;
  }

  const normalizedTenant = normalizeTenantCode(tenantCode);
  if (normalizedTenant === "essencia") {
    return "Rebeka";
  }

  return "Concierge digital";
}

function splitKnowledgeToneValues(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => toNonEmptyString(item))
      .filter(Boolean);
  }
  const text = toNonEmptyString(value);
  if (!text) {
    return [];
  }
  return text
    .split(",")
    .map((item) => toNonEmptyString(item))
    .filter(Boolean);
}

function extractKnowledgeToneGuide(knowledge) {
  const identity = knowledge?.identity && typeof knowledge.identity === "object" && !Array.isArray(knowledge.identity)
    ? knowledge.identity
    : {};

  const rawValues = [
    ...splitKnowledgeToneValues(identity?.toneOptions),
    ...splitKnowledgeToneValues(identity?.toneCustom),
    ...splitKnowledgeToneValues(identity?.toneGuide),
  ];

  const seen = new Set();
  const uniqueValues = [];
  rawValues.forEach((item) => {
    const key = toNonEmptyString(item).normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
    if (!key || seen.has(key)) {
      return;
    }
    seen.add(key);
    uniqueValues.push(item);
  });

  return uniqueValues.length ? uniqueValues.join(", ") : "Nao informado";
}

const SYSTEM_INSTRUCTION = `Voce e a IA.AGENDAMENTO, uma concierge digital premium para atendimento e agendamento.
Use o nome da concierge digital definido na Base de conhecimento; se nao houver, use "Concierge digital".

Diretrizes:
- Siga o tom definido na Base de conhecimento; se nao houver definicao, use tom sofisticado, acolhedor e objetivo.
- Frases curtas, sem paragrafos longos.
- Foco em concluir agendamentos com precisao.
- Ao mencionar profissionais para a cliente, use apenas o primeiro nome.
- Ao chamar a cliente pelo nome, use apenas o primeiro nome.

Fluxo:
- Identifique o servico desejado.
- Antes de sugerir horario, consulte disponibilidade real por profissional (checkAvailability).
- Se a cliente nao tiver preferÃªncia de profissional e informar horario desejado, mostre todas as profissionais que executam o servico e estao livres naquele horario.
- Para agendar, use bookAppointment.
- Antes de finalizar o agendamento, sempre valide disponibilidade e apresente um resumo completo para confirmacao explicita da cliente.
- Se houver mais de um servico, monte todos os itens no campo appointments da ferramenta bookAppointment.
- Para reagendar, use rescheduleAppointment.
- Para desmarcar sem remarcar, use cancelAppointment.
- Regra critica: se a cliente pedir alteracao ou cancelamento, nao use bookAppointment antes de concluir reschedule/cancel.
- Para desmarcar, priorize pedir codigo de confirmacao (TRK). Se a cliente nao tiver codigo, tente localizar pelo telefone da cliente na base Trinks e prossiga com seguranca.
- Quando a cliente perguntar nomes de profissionais, consulte a ferramenta listProfessionalsForDate e responda apenas com dados reais.
- Ao receber preferÃªncia de profissional e/ou horario desejado, use checkAvailability com professionalName e preferredTime para trazer os horarios mais proximos possiveis.
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
    /\bcom\s+(?:a|o)?\s*[a-z]{3,}\b/.test(normalized) ||
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

function buildConversationPrompt(history, message, knowledge, customerContext = null, tenantCode = "", crmContext = null) {
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

  const crmLines = [];
  if (crmContext && typeof crmContext === "object") {
    crmLines.push("", "Contexto especial - Fluxo CRM de retorno de cliente:");
    crmLines.push("- Esta cliente esta em um fluxo de recuperacao de agenda do CRM.");
    if (crmContext.originServiceName) crmLines.push(`- Servico de origem: ${crmContext.originServiceName}`);
    if (crmContext.originCategoryName) crmLines.push(`- Categoria: ${crmContext.originCategoryName}`);
    if (crmContext.lastVisitAt) crmLines.push(`- Ultima visita registrada: ${isoToBrDate(crmContext.lastVisitAt) || crmContext.lastVisitAt}`);
    if (crmContext.currentStep) crmLines.push(`- Etapa atual: ${crmContext.currentStep}`);
    if (crmContext.lastProfessionalName) {
      const statusNote = crmContext.lastProfessionalActive === false ? " (profissional INATIVA - nao oferecer como opcao)" : "";
      crmLines.push(`- Ultimo profissional atendeu: ${crmContext.lastProfessionalName}${statusNote}`);
    }
    crmLines.push(`- Objetivo principal: converter esta cliente para um novo agendamento de ${crmContext.originServiceName || "servico adequado"}.`);
    crmLines.push("- Instrucao especial: priorize oferecer horarios disponiveis e facilitar o agendamento. Seja direto e objetivo.");
  }

  return [
    "Historico da conversa:",
    transcript || "Sem historico anterior.",
    "",
    formatKnowledgeForPrompt(knowledge, tenantCode),
    `- Tenant em atendimento: ${toNonEmptyString(tenantCode) || "nao informado"}`,
    ...crmLines,
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
    "- Regra: pergunte sobre preferÃªncia de profissional somente quando a cliente estiver tentando agendar horario.",
    "- Regra: se nao houver preferÃªncia de profissional e houver horario desejado, liste todas as profissionais disponiveis naquele horario.",
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

async function trinksRequest(path, { method = "GET", estabelecimentoId, body, query, tenantCode = "" } = {}) {
  const runtime = resolveTrinksRuntimeConfig({
    tenantCode,
    establishmentId: estabelecimentoId,
  });
  const baseUrl = runtime.baseUrl;
  const apiKey = runtime.apiKey;

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

function resolveEvolutionBaseUrl(options = {}) {
  return resolveEvolutionRuntimeConfig(options).baseUrl;
}

function resolveEvolutionTimeoutMs() {
  const raw = Number(process.env.EVOLUTION_TIMEOUT_MS || 20000);
  if (!Number.isFinite(raw) || raw < 1000) {
    return 20000;
  }
  return Math.floor(raw);
}

async function evolutionRequest(path, { method = "POST", body, tenantCode = "", instanceName = "" } = {}) {
  const inferredInstanceName = firstNonEmpty([instanceName, inferEvolutionInstanceFromPath(path)]);
  const runtime = resolveEvolutionRuntimeConfig({
    tenantCode,
    instanceName: inferredInstanceName,
  });
  const baseUrl = runtime.baseUrl;
  const apiKey = runtime.apiKey;
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

async function evolutionRequestWithFallback(attempts, context = {}) {
  const errors = [];

  for (const attempt of attempts) {
    try {
      const payload = await evolutionRequest(attempt.path, {
        method: attempt.method || "POST",
        body: attempt.body,
        tenantCode: attempt.tenantCode ?? context.tenantCode ?? "",
        instanceName: attempt.instanceName ?? context.instanceName ?? "",
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

function resolveEvolutionInstance(preferred, options = {}) {
  const fromArg = toNonEmptyString(preferred);
  if (fromArg) {
    return fromArg;
  }

  const normalizedTenantCode = normalizeTenantScopeCode(options?.tenantCode || "");
  if (normalizedTenantCode) {
    const providerConfig = getTenantProviderConfigByCode(normalizedTenantCode, "evolution");
    const config = providerConfig?.config && typeof providerConfig.config === "object" && !Array.isArray(providerConfig.config)
      ? providerConfig.config
      : {};
    const tenantInstance = firstNonEmpty([
      readProviderConfigValue(config, [
        "instance",
        "instanceName",
        "evolutionInstance",
        "credentials.instance",
        "credentials.instanceName",
      ]),
      getTenantIdentifierValueByCode(normalizedTenantCode, "evolution_instance"),
    ]);
    if (tenantInstance) {
      return tenantInstance;
    }
  }

  return toNonEmptyString(process.env.EVOLUTION_INSTANCE);
}

function resolvePublicBaseUrl(req) {
  const envBase = firstNonEmpty([
    process.env.EVOLUTION_WEBHOOK_BASE_URL,
    process.env.WEBHOOK_BASE_URL,
    process.env.PUBLIC_BASE_URL,
    process.env.APP_BASE_URL,
    process.env.BACKEND_PUBLIC_URL,
  ]);
  if (envBase) {
    return String(envBase).replace(/\/$/, "");
  }

  const forwardedProto = toNonEmptyString(String(req?.headers?.["x-forwarded-proto"] || "").split(",")[0]);
  const forwardedHost = toNonEmptyString(String(req?.headers?.["x-forwarded-host"] || "").split(",")[0]);
  const host = forwardedHost || toNonEmptyString(req?.get?.("host"));
  const proto = forwardedProto || req?.protocol || "https";

  if (!host) {
    return "";
  }

  return `${proto}://${host}`.replace(/\/$/, "");
}

function resolveEvolutionWebhookUrl(req, explicitUrl = "") {
  const fromArg = toNonEmptyString(explicitUrl);
  if (fromArg) {
    return fromArg;
  }

  const envWebhookUrl = firstNonEmpty([process.env.EVOLUTION_WEBHOOK_URL, process.env.WHATSAPP_WEBHOOK_URL]);
  if (envWebhookUrl) {
    return String(envWebhookUrl).replace(/\/$/, "");
  }

  const baseUrl = resolvePublicBaseUrl(req);
  if (!baseUrl) {
    return "";
  }

  return `${baseUrl}/webhook/whatsapp`;
}

function buildEvolutionWebhookPayload({
  url = "",
  enabled = true,
  webhookByEvents = true,
  webhookBase64 = false,
  events = [],
} = {}) {
  const normalizedUrl = toNonEmptyString(url);
  if (!normalizedUrl) {
    const error = new Error("Webhook URL nao informada.");
    error.status = 400;
    throw error;
  }

  const normalizedEvents = Array.isArray(events)
    ? events
        .map((item) => toNonEmptyString(item).toUpperCase())
        .filter(Boolean)
    : [];
  const finalEvents = normalizedEvents.length
    ? normalizedEvents
    : ["MESSAGES_UPSERT", "CONNECTION_UPDATE", "SEND_MESSAGE"];

  return {
    enabled: Boolean(enabled),
    url: normalizedUrl,
    webhookByEvents: Boolean(webhookByEvents),
    webhookBase64: Boolean(webhookBase64),
    events: finalEvents,
  };
}

async function setEvolutionWebhook(instanceName, webhookPayload) {
  const instance = toNonEmptyString(instanceName);
  if (!instance) {
    const error = new Error("Nome da instancia nao informado.");
    error.status = 400;
    throw error;
  }

  const payload = buildEvolutionWebhookPayload(webhookPayload);
  const { payload: response, attempt } = await evolutionRequestWithFallback([
    {
      path: `/webhook/set/${instance}`,
      method: "POST",
      body: { webhook: payload },
    },
    {
      path: `/webhook/set/${instance}`,
      method: "POST",
      body: payload,
    },
  ], { instanceName: instance });

  return { payload: response, attempt, request: payload };
}

async function findEvolutionWebhook(instanceName) {
  const instance = toNonEmptyString(instanceName);
  if (!instance) {
    const error = new Error("Nome da instancia nao informado.");
    error.status = 400;
    throw error;
  }

  const { payload, attempt } = await evolutionRequestWithFallback([
    { path: `/webhook/find/${instance}`, method: "GET" },
    { path: `/webhook/find?instance=${encodeURIComponent(instance)}`, method: "GET" },
  ], { instanceName: instance });

  return { payload, attempt };
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
    ], { instanceName: name });

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
  ], { instanceName: instance });

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

function normalizeEvolutionConnectionStatus(value) {
  return toNonEmptyString(value).toLowerCase();
}

function isEvolutionInstanceConnected(instancePayload) {
  if (!instancePayload || typeof instancePayload !== "object") {
    return false;
  }

  const status = normalizeEvolutionConnectionStatus(
    firstNonEmpty([
      instancePayload.connectionStatus,
      instancePayload.status,
      instancePayload.state,
    ]),
  );

  if (status === "open" || status === "connected" || status === "online") {
    return true;
  }

  if (status === "close" || status === "closed" || status === "disconnected") {
    return false;
  }

  return false;
}

async function disconnectEvolutionInstance(instanceName) {
  const instance = toNonEmptyString(instanceName);
  if (!instance) {
    const error = new Error("Nome da instancia nao informado.");
    error.status = 400;
    throw error;
  }

  const attempts = [
    { path: `/instance/logout/${instance}`, method: "DELETE" },
    { path: `/instance/logout/${instance}`, method: "POST" },
    { path: `/instance/disconnect/${instance}`, method: "DELETE" },
    { path: `/instance/disconnect/${instance}`, method: "POST" },
    { path: `/instance/close/${instance}`, method: "POST" },
  ];

  const errors = [];
  for (const attempt of attempts) {
    try {
      const payload = await evolutionRequest(attempt.path, {
        method: attempt.method,
        instanceName: instance,
      });
      return {
        payload,
        attempt,
      };
    } catch (error) {
      const extractTexts = (value) => {
        if (value == null) return [];
        if (typeof value === "string") return [value.toLowerCase()];
        if (Array.isArray(value)) return value.flatMap((item) => extractTexts(item));
        if (typeof value === "object") return Object.values(value).flatMap((item) => extractTexts(item));
        return [String(value).toLowerCase()];
      };
      const texts = [String(error?.message || "").toLowerCase(), ...extractTexts(error?.details)];
      const alreadyDisconnected = texts.some((text) => text.includes("not connected") || text.includes("nao conect"));
      if (alreadyDisconnected) {
        return {
          payload: {
            status: "SUCCESS",
            alreadyDisconnected: true,
            message: "Instancia ja estava desconectada.",
          },
          attempt,
        };
      }

      errors.push({
        path: attempt.path,
        method: attempt.method,
        message: error.message || "Erro ao desconectar instancia.",
        status: error.status || null,
        details: error.details || null,
      });
    }
  }

  const finalError = new Error("Nenhum endpoint de desconexao respondeu com sucesso.");
  finalError.status = 502;
  finalError.details = errors;
  throw finalError;
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

  // Check aliases for perfect match or strong overlap
  if (Array.isArray(candidate?.aliases) && candidate.aliases.length) {
    for (const alias of candidate.aliases) {
      const normalizedAlias = normalizeServiceText(alias);
      if (target === normalizedAlias || targetRaw === normalizeForMatch(alias).trim()) {
        return 1;
      }
      const aliasTokens = normalizedAlias.split(" ").filter(Boolean);
      const targetTokens = target.split(" ").filter(Boolean);
      if (aliasTokens.length && targetTokens.length) {
        const aliasSet = new Set(aliasTokens);
        const overlap = targetTokens.filter((token) => aliasSet.has(token)).length;
        if (overlap === targetTokens.length && overlap > 0) {
          return 0.95;
        }
        if (normalizedAlias.includes(target) || target.includes(normalizedAlias)) {
          return 0.85;
        }
      }
    }
  }

  const targetTokens = target.split(" ").filter(Boolean);
  const synonymMap = {
    escova: ["brushing", "brush"],
    brushing: ["escova", "escov"],
    hidratacao: ["hidratação", "hydrat", "hydration"],
  };
  const expandTokens = (tokens) => {
    const expanded = new Set(tokens);
    for (const token of tokens) {
      const mapped = synonymMap[token];
      if (!mapped) continue;
      for (const item of mapped) {
        const normalized = normalizeServiceText(item);
        if (normalized) {
          normalized.split(" ").filter(Boolean).forEach((part) => expanded.add(part));
        }
      }
    }
    return [...expanded];
  };
  const candidateTokens = candidateName.split(" ").filter(Boolean);
  if (!targetTokens.length || !candidateTokens.length) {
    return 0;
  }

  const expandedTargetTokens = expandTokens(targetTokens);
  const expandedCandidateTokens = expandTokens(candidateTokens);

  const candidateSet = new Set(candidateTokens);
  const baseOverlap = targetTokens.filter((token) => candidateSet.has(token)).length;
  const expandedSet = new Set(expandedCandidateTokens);
  const expandedOverlap = expandedTargetTokens.filter((token) => expandedSet.has(token)).length;
  const overlap = Math.max(baseOverlap, expandedOverlap);
  if (!overlap) {
    return 0;
  }

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

function isEssenciaTenantScope({ tenantCode = "", establishmentId = null } = {}) {
  const normalizedTenant = normalizeTenantCode(tenantCode);
  if (normalizedTenant === "essencia") {
    return true;
  }

  const parsedEstablishmentId = Number(establishmentId);
  if (Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0) {
    if (parsedEstablishmentId === 62217) {
      return true;
    }
    const tenant = getActiveTenantByEstablishmentId(parsedEstablishmentId);
    if (normalizeTenantCode(tenant?.code || "") === "essencia") {
      return true;
    }
  }

  return false;
}

function resolveEssenciaServiceCanonicalName(serviceName = "") {
  const raw = toNonEmptyString(serviceName);
  if (!raw) {
    return "";
  }

  const normalized = normalizeForMatch(raw)
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) {
    return raw;
  }

  if (/\bmanicure\b/.test(normalized) && !/\bmao\b/.test(normalized)) {
    return "mao tradicional";
  }
  if (/\bpedicure\b/.test(normalized) && !/\bpe\b/.test(normalized)) {
    return "pe tradicional";
  }

  return raw;
}

function resolveServiceLookupCandidates({
  serviceName = "",
  tenantCode = "",
  establishmentId = null,
} = {}) {
  const raw = toNonEmptyString(serviceName);
  if (!raw) {
    return [];
  }

  const candidates = [];
  const seen = new Set();
  const pushCandidate = (value) => {
    const cleaned = toNonEmptyString(value);
    if (!cleaned) {
      return;
    }
    const key = normalizeForMatch(cleaned).replace(/\s+/g, " ").trim();
    if (!key || seen.has(key)) {
      return;
    }
    seen.add(key);
    candidates.push(cleaned);
  };

  if (isEssenciaTenantScope({ tenantCode, establishmentId })) {
    pushCandidate(resolveEssenciaServiceCanonicalName(raw));
  }
  pushCandidate(raw);

  return candidates;
}

function classifyEssenciaServiceType(item = {}) {
  const combined = [
    toNonEmptyString(item?.serviceResolvedName),
    toNonEmptyString(item?.service),
    toNonEmptyString(item?.serviceCategory),
  ]
    .map((value) => normalizeForMatch(value))
    .join(" ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!combined) {
    return "other";
  }

  if (/\bdepil/.test(combined) || /\bcera\b/.test(combined)) {
    return "depilation";
  }

  if (
    /\bmanicure\b/.test(combined) ||
    /\bpedicure\b/.test(combined) ||
    /\bmao\b/.test(combined) ||
    /\bunha\b/.test(combined) ||
    /\bpe\b/.test(combined)
  ) {
    return "manicure";
  }

  if (
    /\bescova\b/.test(combined) ||
    /\bcabelo\b/.test(combined) ||
    /\bcorte\b/.test(combined) ||
    /\bcolora/.test(combined) ||
    /\bmecha/.test(combined) ||
    /\breflex/.test(combined) ||
    /\bpentead/.test(combined) ||
    /\bhidrat/.test(combined) ||
    /\breconstr/.test(combined)
  ) {
    return "hair";
  }

  return "other";
}

function validateTenantSimultaneousBookingRules(items = [], { tenantCode = "", establishmentId = null } = {}) {
  if (!Array.isArray(items) || items.length < 2) {
    return null;
  }

  if (!isEssenciaTenantScope({ tenantCode, establishmentId })) {
    return null;
  }

  const groupedBySlot = new Map();
  for (const item of items) {
    const date = toNonEmptyString(item?.date);
    const time = normalizeTimeValue(item?.time) || toNonEmptyString(item?.time);
    if (!date || !time) {
      continue;
    }
    const key = `${date}|${time}`;
    if (!groupedBySlot.has(key)) {
      groupedBySlot.set(key, []);
    }
    groupedBySlot.get(key).push(item);
  }

  for (const [slotKey, group] of groupedBySlot.entries()) {
    if (group.length < 2) {
      continue;
    }

    const classified = group.map((item) => ({
      item,
      kind: classifyEssenciaServiceType(item),
    }));
    const hasDepilation = classified.some((entry) => entry.kind === "depilation");
    const hasHair = classified.some((entry) => entry.kind === "hair");
    const hasManicure = classified.some((entry) => entry.kind === "manicure");

    if (hasDepilation && (hasHair || hasManicure)) {
      const [date, time] = slotKey.split("|");
      return {
        status: "invalid_simultaneous_combination",
        message:
          "No Essencia, no mesmo horario permitimos cabelo junto com manicure. Combinacoes com depilacao devem ser em outro horario.",
        details: {
          date,
          time,
          services: group.map((item) => ({
            service: toNonEmptyString(item?.serviceResolvedName || item?.service),
            professionalName: professionalDisplayName(item?.professionalName || ""),
          })),
        },
      };
    }
  }

  return null;
}

function buildServiceFallbackAliases(serviceName = "") {
  const normalized = normalizeForMatch(serviceName)
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) {
    return [];
  }

  const aliases = [];

  if (/\bmanicure\b/.test(normalized) || /\besmalt/.test(normalized)) {
    aliases.push("mao tradicional", "mao");
  }

  if (/\bpedicure\b/.test(normalized)) {
    aliases.push("pe tradicional", "pe");
  }

  if (/\bdepilacao\b/.test(normalized) && /\bintima\b/.test(normalized)) {
    aliases.push("depilacao virilha");
  }

  const seen = new Set();
  const output = [];
  for (const alias of aliases) {
    const cleaned = toNonEmptyString(alias);
    if (!cleaned) {
      continue;
    }
    const key = normalizeForMatch(cleaned);
    if (!key || seen.has(key) || key === normalized) {
      continue;
    }
    seen.add(key);
    output.push(cleaned);
  }
  return output;
}

async function findServiceByName(estabelecimentoId, serviceName) {
  const normalizedInput = toNonEmptyString(serviceName);
  const lookupCandidates = resolveServiceLookupCandidates({
    serviceName: normalizedInput,
    establishmentId: estabelecimentoId,
  });
  if (!lookupCandidates.length) {
    return null;
  }

  const tryLookup = async (targetInput) => {
    const directPayload = await trinksRequest("/servicos", {
      method: "GET",
      estabelecimentoId,
      query: {
        nome: targetInput,
        page: 1,
        pageSize: 100,
      },
    });

    const directItems = extractItems(directPayload);
    const directMatch = findBestServiceMatch(targetInput, directItems);
    if (directMatch) {
      return directMatch;
    }

    // Segunda tentativa: busca por termos importantes da frase (ex.: "pedicure").
    const tokenQueries = [...new Set(tokenizeMeaningfulText(targetInput).filter((token) => token.length >= 4))].slice(
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
      const tokenMatch = findBestServiceMatch(targetInput, tokenItems);
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

    return findBestServiceMatch(targetInput, fallbackItems);
  };

  for (const candidate of lookupCandidates) {
    const match = await tryLookup(candidate);
    if (match) {
      return match;
    }
  }

  return null;
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
    /(?:\bas|\bÃ s)\s*(\d{1,2})(?::(\d{2}))?(?!\/)/i,
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
  {
    professionalName = "",
    preferredTime = "",
    strictProfessional = false,
    allowedProfessionalNames = [],
  } = {},
) {
  let foundService = await findServiceByName(establishmentId, service);
  if (!foundService) {
    return {
      availableTimes: [],
      professionals: [],
      suggestions: [],
      message: `Servico nao encontrado para: ${service}`,
    };
  }

  let serviceId = Number(serviceIdFrom(foundService));
  let resolvedServiceName = toNonEmptyString(
    foundService?.nome || foundService?.name || foundService?.servicoNome || service,
  ) || toNonEmptyString(service);
  let serviceCategory = toNonEmptyString(
    foundService?.categoria || foundService?.categoriaNome || foundService?.category || "",
  );
  let duration = Number(
    foundService?.duracaoEmMinutos || foundService?.duracao || foundService?.duracaoMinutos || 60,
  );
  let durationMinutes = Number.isFinite(duration) ? duration : 60;
  let serviceAmount = Number(foundService?.valor || foundService?.preco || 0);
  let serviceAliasAppliedFrom = "";

  let professionalsRaw = await getProfessionals({
    establishmentId,
    date,
    serviceId: Number.isFinite(serviceId) ? serviceId : undefined,
  });

  if (!professionalsRaw.length) {
    const fallbackAliases = buildServiceFallbackAliases(service);
    for (const alias of fallbackAliases) {
      const aliasService = await findServiceByName(establishmentId, alias);
      if (!aliasService) {
        continue;
      }

      const aliasServiceId = Number(serviceIdFrom(aliasService));
      if (!Number.isFinite(aliasServiceId) || aliasServiceId <= 0) {
        continue;
      }

      const aliasProfessionalsRaw = await getProfessionals({
        establishmentId,
        date,
        serviceId: aliasServiceId,
      });
      if (!aliasProfessionalsRaw.length) {
        continue;
      }

      serviceAliasAppliedFrom = resolvedServiceName;
      foundService = aliasService;
      serviceId = aliasServiceId;
      resolvedServiceName = toNonEmptyString(
        aliasService?.nome || aliasService?.name || aliasService?.servicoNome || alias,
      ) || alias;
      serviceCategory = toNonEmptyString(
        aliasService?.categoria || aliasService?.categoriaNome || aliasService?.category || serviceCategory,
      );
      duration = Number(
        aliasService?.duracaoEmMinutos || aliasService?.duracao || aliasService?.duracaoMinutos || durationMinutes,
      );
      durationMinutes = Number.isFinite(duration) ? duration : durationMinutes;
      serviceAmount = Number(aliasService?.valor || aliasService?.preco || serviceAmount || 0);
      professionalsRaw = aliasProfessionalsRaw;
      break;
    }
  }

  const professionals = filterProfessionalsByAllowedList(professionalsRaw, allowedProfessionalNames);

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
      const allDayProfessionals = filterProfessionalsByAllowedList(allDayProfessionalsRaw, allowedProfessionalNames);
      byProfessionalAllDay = allDayProfessionals.map((professional) => {
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
    serviceCategory: serviceCategory || null,
    serviceAliasAppliedFrom: serviceAliasAppliedFrom || null,
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
            : preferredProfessionalGeneralTimes.length
              ? `${requestedProfessionalDisplay} tem agenda geral no dia ${isoToBrDate(date) || date} em: ${
                  preferredProfessionalGeneralTimes.join(", ")
                }, mas nao encontrei disponibilidade vinculada ao servico ${resolvedServiceName}. Posso verificar o servico correto para ela?`
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
  tenantCode = "",
  service,
  date,
  time,
  professionalName,
  allowedProfessionalNames = [],
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
  assertBookingWithinMaxDaysAhead({
    date: normalizedDate,
    tenantCode,
    establishmentId,
  });

  const availability = await getAvailability(
    establishmentId,
    normalizedService,
    normalizedDate,
    {
      professionalName: requestedProfessional,
      preferredTime: normalizedTime,
      strictProfessional: true,
      allowedProfessionalNames,
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
    serviceCategory: toNonEmptyString(availability?.serviceCategory) || "",
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

async function executeConfirmedBookings({ establishmentId, tenantCode = "", clientName, clientPhone, items }) {
  const simultaneousRuleViolation = validateTenantSimultaneousBookingRules(items, {
    tenantCode,
    establishmentId,
  });
  if (simultaneousRuleViolation) {
    return {
      successes: [],
      failures: (Array.isArray(items) ? items : []).map((item) => ({
        ...item,
        message: simultaneousRuleViolation.message,
        status: 422,
        requestReference: "",
      })),
    };
  }

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
        tenantCode,
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
          tenantCode,
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
  tenantCode = "",
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
    assertBookingWithinMaxDaysAhead({
      date,
      tenantCode,
      establishmentId,
    });

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
      tenantCode,
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
      tenantCode,
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

  const statusId = Number(
    firstNonEmpty([
      item?.statusId,
      item?.status?.id,
      item?.situacaoId,
      item?.stateId,
      item?.estadoId,
    ]),
  );
  if (Number.isFinite(statusId) && statusId === TRINKS_STATUS_ID_CANCELLED) {
    return true;
  }

  const statusText = normalizeForMatch(firstNonEmpty([
    item?.status,
    item?.status?.nome,
    item?.status?.name,
    item?.situacao,
    item?.state,
    item?.estado,
  ]));
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

async function cancelAppointmentById({ establishmentId, tenantCode = "", appointmentId, reason, requestPayload }) {
  const normalizedReason = toNonEmptyString(reason);
  const cancellationNote = normalizedReason
    ? `Cancelado via IA.AGENDAMENTO | Motivo: ${normalizedReason}`
    : "Cancelado via IA.AGENDAMENTO";
  const cancellationStatusPayload = {
    motivo: normalizedReason || "Cancelado via IA.AGENDAMENTO",
    quemCancelou: 3,
  };

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
      method: "PATCH",
      path: `/agendamentos/${parsedId}/status/cancelado`,
      body: cancellationStatusPayload,
      mode: "PATCH_STATUS_CANCELADO",
    },
    {
      method: "POST",
      path: `/agendamentos/${parsedId}/status/cancelado`,
      body: cancellationStatusPayload,
      mode: "POST_STATUS_CANCELADO",
    },
    {
      method: "PATCH",
      path: `/agendamentos/${parsedId}/status/cancelada`,
      body: undefined,
      mode: "PATCH_STATUS_CANCELADA",
    },
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
        tenantCode,
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
    tenantCode,
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

async function rescheduleAppointment({ establishmentId, tenantCode = "", confirmationCode, appointmentId, date, time }) {
  const parsedId = parseAppointmentId({ confirmationCode, appointmentId });
  const requestedTime = normalizeTimeValue(time) || String(time || "");
  assertBookingWithinMaxDaysAhead({
    date,
    tenantCode,
    establishmentId,
  });
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
        tenantCode,
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
    tenantCode,
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

function isLikelyHttpUrl(value) {
  const raw = toNonEmptyString(value);
  if (!raw) {
    return false;
  }
  return /^https?:\/\/.+/i.test(raw);
}

function inferImageMimeTypeFromName(nameOrPath = "") {
  const ext = String(path.extname(toNonEmptyString(nameOrPath))).toLowerCase();
  if (ext === ".png") {
    return "image/png";
  }
  if (ext === ".webp") {
    return "image/webp";
  }
  if (ext === ".gif") {
    return "image/gif";
  }
  return "image/jpeg";
}

function resolveLocalUploadPathFromMediaValue(mediaValue = "") {
  const raw = toNonEmptyString(mediaValue);
  if (!raw) {
    return "";
  }

  let uploadPath = "";
  if (raw.startsWith("/uploads/")) {
    uploadPath = raw;
  } else if (isLikelyHttpUrl(raw)) {
    try {
      const parsed = new URL(raw);
      if (toNonEmptyString(parsed.pathname).startsWith("/uploads/")) {
        uploadPath = parsed.pathname;
      }
    } catch {
      uploadPath = "";
    }
  }

  if (!uploadPath.startsWith("/uploads/")) {
    return "";
  }

  const relative = uploadPath.replace(/^\/uploads\//, "");
  const safePath = path.resolve(PUBLIC_UPLOADS_DIR_PATH, relative);
  const uploadsRoot = path.resolve(PUBLIC_UPLOADS_DIR_PATH);
  if (!safePath.startsWith(`${uploadsRoot}${path.sep}`) && safePath !== uploadsRoot) {
    return "";
  }

  return safePath;
}

async function sendEvolutionImageMessage({ instance, number, mediaUrl, caption = "" }) {
  const normalizedInstance = toNonEmptyString(instance);
  const normalizedNumber = normalizePhone(number);
  const mediaSource = toNonEmptyString(mediaUrl);
  if (!normalizedInstance || !normalizedNumber || !mediaSource) {
    const error = new Error("Dados obrigatorios ausentes para envio de imagem.");
    error.status = 400;
    throw error;
  }

  const payloadCaption = toNonEmptyString(caption);
  const attempts = [];

  if (isLikelyHttpUrl(mediaSource)) {
    attempts.push({
      path: `/message/sendMedia/${normalizedInstance}`,
      method: "POST",
      body: {
        number: normalizedNumber,
        mediatype: "image",
        mimetype: inferImageMimeTypeFromName(mediaSource),
        media: mediaSource,
        fileName: path.basename(mediaSource.split("?")[0] || "imagem.jpg"),
        caption: payloadCaption,
      },
    });
    attempts.push({
      path: `/message/sendImage/${normalizedInstance}`,
      method: "POST",
      body: {
        number: normalizedNumber,
        image: mediaSource,
        caption: payloadCaption,
      },
    });
  }

  if (mediaSource.startsWith("data:image")) {
    const parsedDataUrl = parseImageDataUrl(mediaSource);
    if (parsedDataUrl?.base64) {
      attempts.push({
        path: `/message/sendMedia/${normalizedInstance}`,
        method: "POST",
        body: {
          number: normalizedNumber,
          mediatype: "image",
          mimetype: toNonEmptyString(parsedDataUrl.mime) || "image/jpeg",
          media: toNonEmptyString(parsedDataUrl.base64),
          fileName: `mkt-${Date.now()}.${marketingUploadMimeToExtension(parsedDataUrl.mime) || "jpg"}`,
          caption: payloadCaption,
        },
      });
    }
  } else {
    const localUploadPath = resolveLocalUploadPathFromMediaValue(mediaSource);
    if (localUploadPath) {
      try {
        const buffer = readFileSync(localUploadPath);
        if (buffer?.length) {
          attempts.push({
            path: `/message/sendMedia/${normalizedInstance}`,
            method: "POST",
            body: {
              number: normalizedNumber,
              mediatype: "image",
              mimetype: inferImageMimeTypeFromName(localUploadPath),
              media: buffer.toString("base64"),
              fileName: path.basename(localUploadPath) || "imagem.jpg",
              caption: payloadCaption,
            },
          });
        }
      } catch {
        // Keep URL attempts only when file cannot be read locally.
      }
    }
  }

  if (!attempts.length) {
    const error = new Error("Imagem invalida. Use URL http(s), data URL ou arquivo em /uploads.");
    error.status = 400;
    throw error;
  }

  const result = await evolutionRequestWithFallback(attempts);
  return {
    payload: result.payload,
    sourcePath: result.attempt?.path || "",
  };
}

async function cancelAppointment({
  establishmentId,
  tenantCode = "",
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
      tenantCode,
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
    tenantCode,
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

async function sendChatMessage({
  establishmentId,
  message,
  history,
  customerContext,
  knowledge: scopedKnowledge = null,
  tenantCode = "",
  crmContext = null,
}) {
  const knowledge = scopedKnowledge && typeof scopedKnowledge === "object" && !Array.isArray(scopedKnowledge)
    ? scopedKnowledge
    : loadSalonKnowledge();
  const allowedProfessionalNames = extractAllowedProfessionalNames(knowledge);
  const shouldApplyProfessionalWhitelist = !isEssenciaTenantScope({ tenantCode, establishmentId });
  const scopedAllowedProfessionalNames = shouldApplyProfessionalWhitelist ? allowedProfessionalNames : [];
  const restrictedProfessionalMessage = buildRestrictedProfessionalMessage({
    allowedProfessionalNames: scopedAllowedProfessionalNames,
  });
  const bookingSingleMessageRetryHint = buildBookingSingleMessageRetryHint({
    allowedProfessionalNames: scopedAllowedProfessionalNames,
  });
  const dateContext = getSaoPauloDateContext();
  const relativeDate = detectRelativeDateReference(message, dateContext);
  const knownClientName = clientFirstName(customerContext?.name);
  const normalizedMessageForGate = normalizeForMatch(message);
  const bookingTestSignal = detectTestSignal({
    message,
    internalTester: Boolean(customerContext?.internalTester),
    explicitTestMode: Boolean(customerContext?.testMode || customerContext?.isTest),
  });
  const testAuthorization = normalizeTestAuthorization(customerContext?.testAuthorization);
  const inferredPreferredTime = extractPreferredTimeFromMessage(message);
  const explicitDateFromMessage = extractIsoDateFromText(message, relativeDate?.iso || dateContext.isoToday);
  const hasDateOrTimeHint =
    Boolean(relativeDate) ||
    Boolean(explicitDateFromMessage) ||
    Boolean(inferredPreferredTime) ||
    /\b\d{1,2}[\/\-]\d{1,2}(?:[\/\-]\d{2,4})?\b/.test(normalizedMessageForGate) ||
    /\b\d{1,2}:\d{2}\b/.test(normalizedMessageForGate) ||
    /\b\d{1,2}\s*h(?:\s*\d{2})?\b/.test(normalizedMessageForGate);
  const hasLikelyServiceTerm =
    /\b(escova|corte|colora|tonaliz|reflex|manicure|pedicure|depil|hidrat|reconstr|maqui|penteado|sobrancel|unha|mao|pe)\b/.test(
      normalizedMessageForGate,
    );
  const hasBookingTimeIntent = messageSuggestsBookingTimeIntent(message, dateContext);
  const hasSchedulingIntent = messageSuggestsSchedulingIntent(message);
  const hasCancellationIntent = messageSuggestsCancellationIntent(message);
  const hasRescheduleIntent = messageSuggestsRescheduleIntent(message);
  const confirmationIntent = detectConfirmationIntent(message);
  const hasChangeRequestIntent = hasCancellationIntent || hasRescheduleIntent;
  const hasRecentChangeConfirmationPrompt = historyHasRecentChangeConfirmationPrompt(history);
  const asksProfessionals = /(profission|quem atende|cabeleireir)/.test(normalizedMessageForGate);
  const hasProfessionalHintInMessage = messageContainsProfessionalPreferenceHint(message);
  const hasProfessionalHintInHistory = historyHasProfessionalContext(history);
  const pendingSessionKey = resolvePendingSessionKey(establishmentId, customerContext, { tenantCode });
  const pendingConfirmation = getPendingBookingConfirmation(pendingSessionKey);
  const recoveredDraft = recoverBookingDraftFromHistory(
    history,
    explicitDateFromMessage || relativeDate?.iso || dateContext.isoToday,
  );
  const sessionClientName = toNonEmptyString(customerContext?.name);
  const sessionClientPhone = normalizePhone(customerContext?.phone);
  const finalizeChatResponse = (text) =>
    applyMarketingActionBeforeClosing({
      knowledge,
      customerMessage: message,
      assistantReply: text,
      sessionKey: pendingSessionKey,
    });
  const capturePendingConfirmationFromText = (candidateText) => {
    if (pendingConfirmation) {
      return { captured: false, items: [] };
    }

    const rawText = toNonEmptyString(candidateText);
    if (!rawText) {
      return { captured: false, items: [] };
    }

    const normalizedText = normalizeForMatch(rawText);
    const likelyBookingDraft =
      textLooksLikeBookingConfirmationRequest(rawText) ||
      /\b(confirmando a disponibilidade final|estou processando o seu agendamento|finalizo a reserva|confirma estes agendamentos|confirma este agendamento)\b/.test(
        normalizedText,
      );
    if (!likelyBookingDraft) {
      return { captured: false, items: [] };
    }

    const draft = recoverBookingDraftFromHistory(
      [{ role: "assistant", content: rawText }],
      explicitDateFromMessage || relativeDate?.iso || dateContext.isoToday,
    );
    if (!draft.items.length) {
      return { captured: false, items: [] };
    }

    const sessionKey = resolvePendingSessionKey(establishmentId, customerContext, {
      tenantCode,
      clientName: sessionClientName,
      clientPhone: sessionClientPhone,
    });
    if (!sessionKey || !sessionClientName || !sessionClientPhone) {
      return { captured: false, items: [] };
    }

    const normalizedItems = draft.items
      .map((item) => ({
        service: toNonEmptyString(item?.service),
        date: normalizeBookingDate(item?.date, explicitDateFromMessage || relativeDate?.iso || dateContext.isoToday),
        time: normalizeBookingTime(item?.time),
        professionalName: professionalDisplayName(item?.professionalName || ""),
      }))
      .filter((item) => item.service && item.date && item.time && item.professionalName);
    if (!normalizedItems.length) {
      return { captured: false, items: [] };
    }

    setPendingBookingConfirmation(sessionKey, {
      establishmentId,
      clientName: sessionClientName,
      clientPhone: sessionClientPhone,
      items: normalizedItems,
    });

    return { captured: true, items: normalizedItems };
  };

  if (pendingConfirmation) {
    if (confirmationIntent === "confirm") {
      const execution = await executeConfirmedBookings({
        establishmentId,
        tenantCode,
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
        return finalizeChatResponse(`Perfeito, agendamento confirmado com sucesso:\n${successLines.join("\n")}`);
      }

      if (execution.successes.length && execution.failures.length) {
        return finalizeChatResponse([
          "Consegui confirmar parte dos agendamentos.",
          "",
          "Confirmados:",
          successLines.join("\n"),
          "",
          "Nao confirmados:",
          failureLines.join("\n"),
        ].join("\n"));
      }

      return finalizeChatResponse(`Nao consegui confirmar os agendamentos solicitados:\n${failureLines.join("\n")}`);
    }

    if (confirmationIntent === "deny") {
      clearPendingBookingConfirmation(pendingSessionKey);
      return finalizeChatResponse("Perfeito, nao vou confirmar ainda. Me diga o que deseja ajustar (servico, profissional, data ou horario).");
    }

    const hasAdjustmentIntent =
      hasSchedulingIntent ||
      hasBookingTimeIntent ||
      Boolean(inferredPreferredTime) ||
      /\b(ajust|troc|mudar|alterar|outro|dia|horario|hora|profissional|servico)\b/.test(normalizedMessageForGate);

    if (hasAdjustmentIntent) {
      clearPendingBookingConfirmation(pendingSessionKey);
    } else {
      return finalizeChatResponse(`${buildBookingConfirmationMessage(pendingConfirmation.items)}\n\nSe quiser ajustar algo, me diga o que devo alterar.`);
    }

  }

  if (!pendingConfirmation && isBookingStatusInquiry(message)) {
    if (recoveredDraft.items.length) {
      return finalizeChatResponse(
        "Ainda nao tenho confirmacao final registrada deste agendamento. Se quiser que eu finalize agora, responda \"sim\".",
      );
    }

    if (historyHasRecentBookingConfirmationPrompt(history)) {
      return finalizeChatResponse(
        "Ainda estou aguardando a confirmacao final para concluir. Se estiver tudo certo, responda \"sim\".",
      );
    }

    return finalizeChatResponse(
      "Ainda nao tenho um agendamento pendente para confirmar nesta conversa. Me envie servico, data, horario e profissional para eu seguir.",
    );
  }

  if (
    confirmationIntent === "confirm" &&
    !pendingConfirmation
  ) {
    if (recoveredDraft.items.length && !hasRecentChangeConfirmationPrompt) {
      const resolvedClientName = toNonEmptyString(customerContext?.name);
      const resolvedClientPhone = normalizePhone(customerContext?.phone);

      if (!resolvedClientName || !resolvedClientPhone) {
        return finalizeChatResponse(
          "Perfeito. Consigo confirmar, mas antes preciso do seu nome e telefone para concluir o agendamento com seguranca.",
        );
      }

      const execution = await executeConfirmedBookings({
        establishmentId,
        tenantCode,
        clientName: resolvedClientName,
        clientPhone: resolvedClientPhone,
        items: recoveredDraft.items,
      });

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
        return finalizeChatResponse(
          `Perfeito, confirmei o agendamento com sucesso:\n${successLines.join("\n")}`,
        );
      }

      if (execution.successes.length && execution.failures.length) {
        return finalizeChatResponse(
          [
            "Consegui confirmar parte dos agendamentos.",
            "",
            "Confirmados:",
            successLines.join("\n"),
            "",
            "Nao confirmados:",
            failureLines.join("\n"),
          ].join("\n"),
        );
      }

      return finalizeChatResponse(
        `Nao consegui confirmar os agendamentos solicitados:\n${failureLines.join("\n")}`,
      );
    }

    if (hasRecentChangeConfirmationPrompt) {
      // Recent history is about cancellation/reschedule confirmation, so avoid treating "sim" as a booking confirmation.
    } else if (!historyHasRecentBookingConfirmationPrompt(history)) {
      return finalizeChatResponse(
        "Perfeito. Para eu confirmar com seguranca, podemos continuar por partes. Me diga o que falta ajustar ou confirmar: servico, data, horario ou profissional.",
      );
    }

    return finalizeChatResponse(
      bookingSingleMessageRetryHint,
    );
  }

  if (
    !pendingConfirmation &&
    hasDateOrTimeHint &&
    !hasChangeRequestIntent &&
    confirmationIntent === "none" &&
    (!hasLikelyServiceTerm || !hasProfessionalHintInMessage)
  ) {
    const parts = [];
    const resolvedDate = explicitDateFromMessage || relativeDate?.iso || "";
    if (resolvedDate) {
      parts.push(`data ${isoToBrDate(resolvedDate) || resolvedDate}`);
    }
    if (inferredPreferredTime) {
      parts.push(`horario ${inferredPreferredTime}`);
    }

    const opener = parts.length
      ? `Perfeito, anotei ${parts.join(" e ")}.`
      : "Perfeito, consegui registrar o que voce enviou.";

    if (!hasProfessionalHintInMessage) {
      return finalizeChatResponse(
        `${opener} Para eu concluir sem erro, me confirme agora: servico e profissional.`,
      );
    }

    return finalizeChatResponse(
      `${opener} Para eu concluir sem erro, me confirme agora o servico desejado.`,
    );
  }

  const shouldAskPreferenceFirst =
    hasBookingTimeIntent &&
    !hasProfessionalHintInMessage &&
    !hasProfessionalHintInHistory &&
    !asksProfessionals &&
    !historyAlreadyAskedProfessionalPreference(history);

  if (shouldAskPreferenceFirst) {
    const salutation = knownClientName ? `Perfeito, ${knownClientName}. ` : "Perfeito. ";
    return finalizeChatResponse(`${salutation}Antes de eu sugerir os horarios, voce tem preferÃªncia por alguma profissional?`);
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

    const professionalsRaw = await getProfessionals({
      establishmentId,
      date: targetDate,
      serviceId,
    });
    const professionals = filterProfessionalsByAllowedList(professionalsRaw, scopedAllowedProfessionalNames);

    const names = uniqueProfessionalDisplayNames(
      professionals
        .filter(professionalHasOpenSchedule)
        .map((item) => toNonEmptyString(item?.name || professionalNameFrom(item))),
    );

    if (!names.length) {
      return finalizeChatResponse(`No momento, nao encontrei profissionais disponiveis para ${isoToBrDate(targetDate)}. Posso consultar outra data para voce.`);
    }

    return finalizeChatResponse(`Para ${isoToBrDate(targetDate)}, as profissionais disponiveis sao: ${names.join(", ")}.`);
  }

  const faqAnswer = findBestFaqAnswer(knowledge, message);
  if (faqAnswer && !hasSchedulingIntent) {
    return finalizeChatResponse(faqAnswer);
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
    message: buildConversationPrompt(
      history,
      message,
      knowledge,
      customerContext || null,
      tenantCode,
      crmContext,
    ),
  });

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const calls = response.functionCalls || [];
    if (!calls.length) {
      let text = String(response.text || "");
      if (
        relativeDate &&
        /(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?|\d{4}-\d{2}-\d{2})/.test(text) &&
        !text.includes(relativeDate.br) &&
        !text.includes(relativeDate.iso)
      ) {
        const corrected = await chat.sendMessage({
          message: `Correcao obrigatoria: para esta conversa, "${relativeDate.label}" = ${relativeDate.iso} (${relativeDate.br}). Reescreva a ultima resposta com a data correta, sem alterar o restante do sentido.`,
        });
        text = String(corrected.text || text);
      }

      const captured = capturePendingConfirmationFromText(text);
      if (captured.captured && !textLooksLikeBookingConfirmationRequest(text)) {
        text = `${text}\n\n${buildBookingConfirmationMessage(captured.items)}`;
      }

      return finalizeChatResponse(text);
    }

    const results = [];
    for (const call of calls) {
      try {
        if (call.name === "checkAvailability") {
          const requestedDate = relativeDate?.iso || toNonEmptyString(call.args?.date);
          const requestedProfessional = toNonEmptyString(call.args?.professionalName);
          if (
            requestedProfessional &&
            Array.isArray(scopedAllowedProfessionalNames) &&
            scopedAllowedProfessionalNames.length &&
            !professionalMatchesAllowedList(requestedProfessional, scopedAllowedProfessionalNames)
          ) {
            results.push({
              name: call.name,
              result: {
                status: "restricted_professional",
                message: restrictedProfessionalMessage,
              },
            });
            continue;
          }
          const requestedPreferredTime =
            normalizeTimeValue(call.args?.preferredTime) || inferredPreferredTime;
          const availability = await getAvailability(
            establishmentId,
            String(call.args.service),
            requestedDate,
            {
              professionalName: requestedProfessional,
              preferredTime: requestedPreferredTime,
              allowedProfessionalNames: scopedAllowedProfessionalNames,
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

          const professionalsRaw = await getProfessionals({
            establishmentId,
            date: requestedDate,
            serviceId,
          });
          const professionals = filterProfessionalsByAllowedList(professionalsRaw, scopedAllowedProfessionalNames);

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
          if (STRICT_TEST_BOOKING_GUARD_ENABLED && bookingTestSignal.inferredTest) {
            if (!hasValidTestAuthorization(testAuthorization)) {
              results.push({
                name: call.name,
                result: buildTestAuthorizationBlock({ signal: bookingTestSignal }),
              });
              continue;
            }
          }

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

          const restrictedItem = normalizedItems.find(
            (item) =>
              item.professionalName &&
              Array.isArray(scopedAllowedProfessionalNames) &&
              scopedAllowedProfessionalNames.length &&
              !professionalMatchesAllowedList(item.professionalName, scopedAllowedProfessionalNames),
          );
          if (restrictedItem) {
            results.push({
              name: call.name,
              result: {
                status: "restricted_professional",
                message: restrictedProfessionalMessage,
                requestedProfessional: restrictedItem.professionalName,
              },
            });
            continue;
          }

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
                tenantCode,
                service: item.service,
                date: item.date,
                time: item.time,
                professionalName: item.professionalName,
                allowedProfessionalNames: scopedAllowedProfessionalNames,
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

          const simultaneousRuleViolation = validateTenantSimultaneousBookingRules(previewItems, {
            tenantCode,
            establishmentId,
          });
          if (simultaneousRuleViolation) {
            results.push({
              name: call.name,
              result: {
                status: simultaneousRuleViolation.status,
                message: simultaneousRuleViolation.message,
                details: simultaneousRuleViolation.details || null,
              },
            });
            continue;
          }

          const sessionKey = resolvePendingSessionKey(
            establishmentId,
            customerContext,
            { clientPhone: resolvedClientPhone, clientName: resolvedClientName, tenantCode },
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
            tenantCode,
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
            tenantCode,
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

    const pendingConfirmationResult = results.find((item) =>
      toNonEmptyString(item?.name) === "bookAppointment" &&
      normalizeForMatch(item?.result?.status || "") === "pending_confirmation",
    );
    if (pendingConfirmationResult) {
      const pendingMessage = toNonEmptyString(pendingConfirmationResult?.result?.message) ||
        buildBookingConfirmationMessage(pendingConfirmationResult?.result?.items || []);
      return finalizeChatResponse(pendingMessage);
    }

    response = await chat.sendMessage({
      message: JSON.stringify(results),
    });
  }

  return finalizeChatResponse(
    "Nao consegui concluir esta etapa automaticamente. Me envie novamente servico, data, horario e profissional para eu seguir com seu atendimento.",
  );
}

app.get("/api/health", (req, res) => {
  res.json({ status: "ok" });
});

app.get("/api/knowledge", (req, res) => {
  try {
    const principal = resolveAdminPrincipal(req);
    const requestedTenantCode = toNonEmptyString(req.query?.tenantCode || req.query?.tenant || "");

    if (principal?.role === "tenant") {
      const tenant = getTenantByCode(principal.tenantCode);
      if (!tenant) {
        return res.status(404).json({
          message: "Tenant da sessao nao encontrado.",
        });
      }
      return res.json({
        knowledge: tenant.knowledge || {},
        scope: "tenant",
        tenantCode: tenant.code,
      });
    }

    if (requestedTenantCode) {
      if (!principal || principal.role !== "superadmin") {
        return res.status(401).json({
          message: "Nao autorizado para consultar base de conhecimento de tenant.",
        });
      }

      const tenant = getTenantByCode(requestedTenantCode);
      if (!tenant) {
        return res.status(404).json({
          message: "Tenant nao encontrado.",
        });
      }

      return res.json({
        knowledge: tenant.knowledge || {},
        scope: "tenant",
        tenantCode: tenant.code,
      });
    }

    const knowledge = loadSalonKnowledge();
    return res.json({ knowledge, scope: "global" });
  } catch (error) {
    return res.status(500).json({
      message: error.message || "Erro ao carregar base de conhecimento.",
    });
  }
});

app.put("/api/knowledge", (req, res) => {
  try {
    const principal = resolveAdminPrincipal(req);
    if (!principal || !isKnowledgeWriteAuthorized(req)) {
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

    if (principal.role === "tenant") {
      const tenant = updateTenantByCode(principal.tenantCode, { knowledge });
      return res.json({
        status: "ok",
        scope: "tenant",
        tenantCode: tenant.code,
        knowledge: tenant.knowledge || {},
      });
    }

    const requestedTenantCode = toNonEmptyString(req.body?.tenantCode || req.body?.tenant || req.query?.tenantCode || req.query?.tenant || "");
    if (requestedTenantCode) {
      const tenant = updateTenantByCode(requestedTenantCode, { knowledge });
      return res.json({
        status: "ok",
        scope: "tenant",
        tenantCode: tenant.code,
        knowledge: tenant.knowledge || {},
      });
    }

    const saved = saveSalonKnowledge(knowledge);
    return res.json({ status: "ok", scope: "global", knowledge: saved });
  } catch (error) {
    return res.status(500).json({
      message: error.message || "Erro ao salvar base de conhecimento.",
    });
  }
});

app.post("/api/admin/uploads/marketing-image", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    const parsed = parseImageDataUrl(req.body?.imageDataUrl);
    if (!parsed) {
      return res.status(400).json({
        message: "Envie imageDataUrl em formato data URL (png, jpg, webp ou gif).",
      });
    }

    const extension = marketingUploadMimeToExtension(parsed.mime);
    if (!extension) {
      return res.status(400).json({
        message: "Formato de imagem nao suportado. Use png, jpg, webp ou gif.",
      });
    }

    const buffer = Buffer.from(parsed.base64, "base64");
    if (!buffer.length) {
      return res.status(400).json({
        message: "Conteudo da imagem vazio.",
      });
    }
    if (buffer.length > MARKETING_UPLOAD_MAX_BYTES) {
      return res.status(400).json({
        message: `Imagem acima do limite (${Math.round(MARKETING_UPLOAD_MAX_BYTES / (1024 * 1024))}MB).`,
      });
    }

    const requestedTenantCode = normalizeTenantCode(req.body?.tenantCode || req.body?.tenant || "");
    const effectiveTenantCode = principal.role === "tenant"
      ? normalizeTenantCode(principal.tenantCode)
      : requestedTenantCode;
    const tenantSegment = effectiveTenantCode || "global";
    const tenantDirPath = path.join(MARKETING_UPLOADS_DIR_PATH, tenantSegment);
    mkdirSync(tenantDirPath, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
    const randomSuffix = crypto.randomBytes(6).toString("hex");
    const fileStem = sanitizeUploadFileStem(req.body?.fileName || req.body?.name || "imagem");
    const fileName = `${fileStem}_${timestamp}_${randomSuffix}.${extension}`;
    const absolutePath = path.join(tenantDirPath, fileName);
    writeFileSync(absolutePath, buffer);

    const relativePath = `/uploads/marketing/${tenantSegment}/${fileName}`;
    const publicBaseUrl = getPublicBackendBaseUrl(req);
    const url = publicBaseUrl ? `${publicBaseUrl}${relativePath}` : relativePath;

    return res.json({
      status: "ok",
      url,
      path: relativePath,
      mime: parsed.mime,
      size: buffer.length,
      tenantCode: effectiveTenantCode || null,
    });
  } catch (error) {
    return res.status(error?.status || 500).json({
      message: error?.message || "Erro ao fazer upload da imagem de marketing.",
      details: error?.details || null,
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
      schedulingProviders: "GET /api/scheduling/providers",
      schedulingAvailability: "POST /api/scheduling/availability",
      schedulingAppointments: "POST /api/scheduling/appointments",
      schedulingAppointmentsDay: "POST /api/scheduling/appointments/day",
      schedulingProfessionals: "POST /api/scheduling/professionals",
      schedulingReschedule: "POST /api/scheduling/appointments/reschedule",
      schedulingCancel: "POST /api/scheduling/appointments/cancel",
      adminAuthLogin: "POST /api/admin/auth/login",
      adminAuthMe: "GET /api/admin/auth/me",
      adminAuthLogout: "POST /api/admin/auth/logout",
      adminTenantsList: "GET /api/admin/tenants",
      adminTenantsCreate: "POST /api/admin/tenants",
      adminTenantGet: "GET /api/admin/tenants/:code",
      adminTenantUpdate: "PUT /api/admin/tenants/:code",
      adminTenantResolve: "GET /api/admin/tenants/resolve?kind=evolution_instance&value=ia-agendamento",
      adminTenantIdentifiers: "POST /api/admin/tenants/:code/identifiers",
      adminTenantProviders: "POST /api/admin/tenants/:code/providers/:provider",
      adminTenantUsersList: "GET /api/admin/tenants/:code/users",
      adminTenantUsersCreate: "POST /api/admin/tenants/:code/users",
      adminTenantUsersUpdate: "PUT /api/admin/tenants/:code/users/:userId",
      adminTenantCrmSettingsGet: "GET /api/admin/tenants/:code/crm/settings",
      adminTenantCrmSettingsPut: "PUT /api/admin/tenants/:code/crm/settings",
      adminTenantCrmCatalog: "GET /api/admin/tenants/:code/crm/services/catalog",
      adminTenantCrmServiceRulesGet: "GET /api/admin/tenants/:code/crm/services/rules",
      adminTenantCrmServiceRulesPut: "PUT /api/admin/tenants/:code/crm/services/rules",
      adminTenantCrmCategoryRulesGet: "GET /api/admin/tenants/:code/crm/categories/rules",
      adminTenantCrmCategoryRulesPut: "PUT /api/admin/tenants/:code/crm/categories/rules",
      adminTenantCrmBlocksGet: "GET /api/admin/tenants/:code/crm/blocks",
      adminTenantCrmBlocksPost: "POST /api/admin/tenants/:code/crm/blocks",
      adminTenantCrmFlows: "GET /api/admin/tenants/:code/crm/flows",
      adminTenantCrmOpportunities: "GET /api/admin/tenants/:code/crm/opportunities",
      adminTenantCrmDashboard: "GET /api/admin/tenants/:code/crm/dashboard",
      adminTenantCrmPreviewRun: "POST /api/admin/tenants/:code/crm/preview-run",
      adminUploadMarketingImage: "POST /api/admin/uploads/marketing-image",
      trinksAvailability: "POST /api/trinks/availability",
      trinksAppointments: "POST /api/trinks/appointments",
      trinksAppointmentsDay: "POST /api/trinks/appointments/day",
      trinksProfessionals: "POST /api/trinks/professionals",
      trinksReschedule: "POST /api/trinks/appointments/reschedule",
      trinksCancel: "POST /api/trinks/appointments/cancel",
      evolutionSendText: "POST /api/evolution/send-text",
      evolutionQrPage: "GET /api/evolution/instance/connect?instance=SEU_NOME",
      evolutionWebhookFind: "GET /api/evolution/webhook/find?instance=SEU_NOME",
      evolutionWebhookSet: "POST /api/evolution/webhook/set",
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
    const shouldConfigureWebhook = req.body?.configureWebhook !== false;
    const webhookUrl = resolveEvolutionWebhookUrl(req, req.body?.webhookUrl || req.body?.url);
    let webhook = null;

    if (shouldConfigureWebhook && webhookUrl) {
      try {
        webhook = await setEvolutionWebhook(instance, {
          url: webhookUrl,
          enabled: req.body?.webhookEnabled ?? true,
          webhookByEvents: req.body?.webhookByEvents ?? true,
          webhookBase64: req.body?.webhookBase64 ?? false,
          events: req.body?.webhookEvents,
        });
      } catch (webhookError) {
        webhook = {
          status: "error",
          message: webhookError.message || "Falha ao configurar webhook.",
          details: webhookError.details || null,
        };
      }
    }

    return res.json({ status: "ok", instance, created, webhook });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao criar instancia na Evolution.",
      details: error.details || null,
    });
  }
});

app.get("/api/evolution/webhook/find", async (req, res) => {
  try {
    const instance = resolveEvolutionInstance(req.query.instance || req.query.instanceName);
    if (!instance) {
      return res.status(400).json({ message: "Informe instance ou configure EVOLUTION_INSTANCE." });
    }

    const webhook = await findEvolutionWebhook(instance);
    return res.json({
      status: "ok",
      instance,
      webhook: webhook.payload,
      sourcePath: webhook.attempt?.path || null,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao consultar webhook da instancia.",
      details: error.details || null,
    });
  }
});

app.post("/api/evolution/webhook/set", async (req, res) => {
  try {
    const instance = resolveEvolutionInstance(req.body?.instance || req.body?.instanceName);
    if (!instance) {
      return res.status(400).json({ message: "Informe instance/instanceName ou configure EVOLUTION_INSTANCE." });
    }

    const webhookUrl = resolveEvolutionWebhookUrl(req, req.body?.webhookUrl || req.body?.url);
    if (!webhookUrl) {
      return res.status(400).json({
        message:
          "Nao foi possivel resolver URL de webhook. Informe webhookUrl/url no body ou configure EVOLUTION_WEBHOOK_URL.",
      });
    }

    const webhook = await setEvolutionWebhook(instance, {
      url: webhookUrl,
      enabled: req.body?.enabled ?? req.body?.webhookEnabled ?? true,
      webhookByEvents: req.body?.webhookByEvents ?? true,
      webhookBase64: req.body?.webhookBase64 ?? false,
      events: req.body?.events || req.body?.webhookEvents,
    });

    return res.json({
      status: "ok",
      instance,
      webhookUrl,
      sourcePath: webhook.attempt?.path || null,
      request: webhook.request,
      response: webhook.payload,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao configurar webhook da instancia.",
      details: error.details || null,
    });
  }
});

app.post("/api/evolution/instance/disconnect", async (req, res) => {
  try {
    const instance = resolveEvolutionInstance(
      req.query.instance || req.query.instanceName || req.body?.instance || req.body?.instanceName,
    );
    if (!instance) {
      return res.status(400).json({ message: "Informe instance/instanceName ou configure EVOLUTION_INSTANCE." });
    }

    const result = await disconnectEvolutionInstance(instance);
    return res.json({ status: "ok", success: true, instance, result });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      success: false,
      message: error.message || "Erro ao desconectar instancia.",
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

    const payload = await evolutionRequest("/instance/fetchInstances", {
      method: "GET",
      tenantCode: req.query.tenantCode || req.query.tenant || "",
      instanceName: instance,
    });
    const instances = Array.isArray(payload) ? payload : Array.isArray(payload?.instances) ? payload.instances : [];
    const found = instances.find(
      (item) =>
        toNonEmptyString(item?.name || item?.instanceName || item?.instance).toLowerCase() === instance.toLowerCase(),
    );

    return res.json({
      status: "ok",
      instance,
      connected: isEvolutionInstanceConnected(found),
      data: found || null,
      raw: payload,
    });
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
    const webhookUrl = resolveEvolutionWebhookUrl(req, req.query.webhookUrl || req.query.url);
    let webhookResult = null;
    let webhookWarning = "";

    if (webhookUrl) {
      try {
        webhookResult = await setEvolutionWebhook(instance, {
          url: webhookUrl,
          enabled: true,
          webhookByEvents: true,
          webhookBase64: false,
        });
      } catch (webhookError) {
        webhookWarning = webhookError.message || "Falha ao configurar webhook automaticamente.";
      }
    }

    const qr = await fetchEvolutionQr(instance);

    const statusMessage = created?.alreadyExists
      ? "Instancia ja existia. Escaneie o QR para conectar."
      : "Instancia criada. Escaneie o QR no WhatsApp Business.";
    const statusMessageWithWebhook = webhookWarning
      ? `${statusMessage} Aviso: ${webhookWarning}`
      : statusMessage;

    const detailsObject = {
      create: created,
      webhook: webhookResult
        ? {
            sourcePath: webhookResult.attempt?.path || null,
            request: webhookResult.request,
            response: webhookResult.payload,
          }
        : null,
      qrSource: qr.attempt?.path,
      qrRaw: qr.qrRaw,
      raw: qr.payload,
    };
    const details = qr.qrDataUrl ? "" : JSON.stringify(detailsObject, null, 2);

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    return res.send(
      renderQrPage({
        instance,
        qrDataUrl: qr.qrDataUrl,
        pairingCode: qr.pairingCode,
        statusMessage: statusMessageWithWebhook,
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

app.post("/api/admin/auth/login", (req, res) => {
  try {
    const tenantCode = toNonEmptyString(req.body?.tenantCode || req.body?.tenant || "");
    const username = toNonEmptyString(req.body?.username || req.body?.user || "");
    const password = String(req.body?.password || "");

    if (!tenantCode || !username || !password) {
      return res.status(400).json({
        status: "error",
        message: "Campos obrigatorios: tenantCode, username, password.",
      });
    }

    const tenantUser = findTenantUserForLogin({ tenantCode, username });
    if (!tenantUser || !verifyTenantPassword(password, tenantUser.passwordHash)) {
      return res.status(401).json({
        status: "error",
        message: "Credenciais invalidas.",
      });
    }

    if (!tenantUser.tenantActive || !tenantUser.userActive) {
      return res.status(403).json({
        status: "error",
        message: "Usuario/tenant desativado. Contate o administrador.",
      });
    }

    const session = createTenantUserSession({ tenantUserId: tenantUser.id });
    db.prepare(
      `
        UPDATE tenant_users
        SET last_login_at = ?,
            updated_at = ?
        WHERE id = ?
      `,
    ).run(new Date().toISOString(), new Date().toISOString(), tenantUser.id);

    return res.json({
      status: "ok",
      token: session.token,
      expiresAt: session.expiresAt,
      principal: {
        role: "tenant",
        tenantCode: tenantUser.tenantCode,
        tenantName: tenantUser.tenantName,
        username: tenantUser.username,
        displayName: tenantUser.displayName,
      },
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao autenticar usuario do tenant.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/auth/me", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    return res.json({
      status: "ok",
      principal,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao validar sessao admin.",
      details: error.details || null,
    });
  }
});

app.post("/api/admin/auth/logout", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    if (principal.role === "tenant") {
      const providedToken = getAdminTokenFromRequest(req);
      const allDevices = parseBooleanEnv(req.body?.allDevices, false);
      if (allDevices) {
        revokeTenantSessionsByUserId(principal.tenantUserId);
      } else {
        revokeTenantSessionByToken(providedToken);
      }
    }

    return res.json({ status: "ok" });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao encerrar sessao.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    const includeInactive = principal.role === "superadmin"
      ? parseBooleanEnv(req.query.includeInactive, false)
      : false;
    const withDetails = parseBooleanEnv(req.query.withDetails, false);
    const sourceTenants = principal.role === "superadmin"
      ? listTenants({ includeInactive })
      : (() => {
          const ownTenant = getTenantByCode(principal.tenantCode);
          return ownTenant ? [ownTenant] : [];
        })();

    const tenants = sourceTenants.map((tenant) =>
      withDetails
        ? {
          ...tenant,
          identifiers: getTenantIdentifiersByCode(tenant.code),
          providerConfigs: getTenantProviderConfigsByCode(tenant.code),
          users: principal.role === "superadmin" ? listTenantUsersByCode(tenant.code) : undefined,
        }
        : tenant,
    );

    return res.json({
      status: "ok",
      count: tenants.length,
      data: tenants,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar tenants.",
      details: error.details || null,
    });
  }
});

app.post("/api/admin/tenants", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    ensureSuperAdminPrincipal(principal);

    const tenant = createTenant({
      code: req.body?.code,
      name: req.body?.name,
      segment: req.body?.segment,
      active: req.body?.active == null ? true : Boolean(req.body.active),
      defaultProvider: req.body?.defaultProvider || req.body?.provider,
      establishmentId: req.body?.establishmentId,
      knowledge: req.body?.knowledge || {},
    });

    if (Array.isArray(req.body?.identifiers)) {
      for (const identifier of req.body.identifiers) {
        if (!identifier || typeof identifier !== "object") {
          continue;
        }
        if (!identifier.kind || !identifier.value) {
          continue;
        }
        upsertTenantIdentifierByCode(tenant.code, {
          kind: identifier.kind,
          value: identifier.value,
        });
      }
    }

    if (req.body?.providerConfigs && typeof req.body.providerConfigs === "object") {
      for (const [provider, config] of Object.entries(req.body.providerConfigs)) {
        upsertTenantProviderConfigByCode(tenant.code, provider, {
          enabled: config?.enabled ?? true,
          config: config?.config || config || {},
        });
      }
    }

    const responseTenant = getTenantByCode(tenant.code);
    return res.status(201).json({
      status: "ok",
      tenant: responseTenant,
      identifiers: getTenantIdentifiersByCode(tenant.code),
      providerConfigs: getTenantProviderConfigsByCode(tenant.code),
    });
  } catch (error) {
    if (String(error?.message || "").includes("UNIQUE constraint failed: tenants.code")) {
      return res.status(409).json({
        status: "error",
        message: "Ja existe tenant com esse code.",
      });
    }
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao criar tenant.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/resolve", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    ensureSuperAdminPrincipal(principal);

    const kind = toNonEmptyString(req.query.kind || "");
    const value = toNonEmptyString(req.query.value || "");
    if (!kind || !value) {
      return res.status(400).json({ message: "Informe kind e value." });
    }

    const tenant = resolveTenantByIdentifier({ kind, value });
    return res.json({
      status: "ok",
      kind: normalizeTenantIdentifierKind(kind),
      value,
      found: Boolean(tenant),
      tenant: tenant || null,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao resolver tenant por identificador.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const tenant = getTenantByCode(req.params.code);
    if (!tenant) {
      return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    }

    return res.json({
      status: "ok",
      tenant,
      identifiers: getTenantIdentifiersByCode(tenant.code),
      providerConfigs: getTenantProviderConfigsByCode(tenant.code),
      users: principal.role === "superadmin" ? listTenantUsersByCode(tenant.code) : [],
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao consultar tenant.",
      details: error.details || null,
    });
  }
});

app.put("/api/admin/tenants/:code", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    if (
      principal.role !== "superadmin"
      && (
        req.body?.active != null
        || req.body?.establishmentId != null
        || req.body?.defaultProvider != null
        || req.body?.provider != null
      )
    ) {
      return res.status(403).json({
        status: "error",
        message: "Perfil do tenant nao pode alterar active/defaultProvider/establishmentId.",
      });
    }

    const tenant = updateTenantByCode(req.params.code, {
      name: req.body?.name,
      segment: req.body?.segment,
      active: req.body?.active,
      defaultProvider: req.body?.defaultProvider || req.body?.provider,
      establishmentId: req.body?.establishmentId,
      knowledge: req.body?.knowledge,
    });

    return res.json({
      status: "ok",
      tenant,
      identifiers: getTenantIdentifiersByCode(tenant.code),
      providerConfigs: getTenantProviderConfigsByCode(tenant.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao atualizar tenant.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/identifiers", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const tenant = getTenantByCode(req.params.code);
    if (!tenant) {
      return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    }

    return res.json({
      status: "ok",
      tenant: { code: tenant.code, name: tenant.name },
      identifiers: getTenantIdentifiersByCode(tenant.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar identificadores do tenant.",
      details: error.details || null,
    });
  }
});

app.post("/api/admin/tenants/:code/identifiers", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const identifiers = upsertTenantIdentifierByCode(req.params.code, {
      kind: req.body?.kind,
      value: req.body?.value,
    });

    return res.json({
      status: "ok",
      identifiers,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao salvar identificador do tenant.",
      details: error.details || null,
    });
  }
});

app.delete("/api/admin/tenants/:code/identifiers/:id", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const tenant = getTenantByCode(req.params.code);
    if (!tenant) {
      return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    }

    const id = Number(req.params.id);
    if (!Number.isFinite(id) || id <= 0) {
      return res.status(400).json({ status: "error", message: "id invalido." });
    }

    db.prepare(
      `
        DELETE FROM tenant_identifiers
        WHERE id = ?
          AND tenant_id = ?
      `,
    ).run(id, tenant.id);

    return res.json({
      status: "ok",
      identifiers: getTenantIdentifiersByCode(tenant.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao excluir identificador do tenant.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/providers", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const tenant = getTenantByCode(req.params.code);
    if (!tenant) {
      return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    }

    return res.json({
      status: "ok",
      tenant: { code: tenant.code, name: tenant.name, defaultProvider: tenant.defaultProvider },
      providerConfigs: getTenantProviderConfigsByCode(tenant.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar providers do tenant.",
      details: error.details || null,
    });
  }
});

app.post("/api/admin/tenants/:code/providers/:provider", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const providerConfigs = upsertTenantProviderConfigByCode(req.params.code, req.params.provider, {
      enabled: req.body?.enabled ?? true,
      config: req.body?.config || req.body || {},
    });

    return res.json({
      status: "ok",
      providerConfigs,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao salvar configuracao de provider do tenant.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/users", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    ensureSuperAdminPrincipal(principal);

    const tenant = getTenantByCode(req.params.code);
    if (!tenant) {
      return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    }

    return res.json({
      status: "ok",
      tenant: { code: tenant.code, name: tenant.name },
      users: listTenantUsersByCode(tenant.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar usuarios do tenant.",
      details: error.details || null,
    });
  }
});

app.post("/api/admin/tenants/:code/users", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    ensureSuperAdminPrincipal(principal);

    const users = createTenantUserByCode(req.params.code, {
      username: req.body?.username,
      displayName: req.body?.displayName || req.body?.name,
      password: req.body?.password,
      active: req.body?.active == null ? true : Boolean(req.body.active),
    });

    return res.status(201).json({
      status: "ok",
      users,
    });
  } catch (error) {
    if (String(error?.message || "").includes("UNIQUE constraint failed: tenant_users.tenant_id, tenant_users.username")) {
      return res.status(409).json({
        status: "error",
        message: "Ja existe usuario com esse username neste tenant.",
      });
    }
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao criar usuario do tenant.",
      details: error.details || null,
    });
  }
});

app.put("/api/admin/tenants/:code/users/:userId", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    ensureSuperAdminPrincipal(principal);

    const users = updateTenantUserByCodeAndId(req.params.code, req.params.userId, {
      displayName: req.body?.displayName || req.body?.name,
      password: req.body?.password,
      active: req.body?.active,
    });

    return res.json({
      status: "ok",
      users,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao atualizar usuario do tenant.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/settings", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const tenant = getTenantByCode(req.params.code);
    if (!tenant) {
      return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    }

    return res.json({
      status: "ok",
      tenant: { code: tenant.code, name: tenant.name },
      settings: getTenantCrmSettingsByCode(tenant.code)?.config || getDefaultCrmSettings(),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao carregar configuracoes de CRM.",
      details: error.details || null,
    });
  }
});

app.put("/api/admin/tenants/:code/crm/settings", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const settings = upsertTenantCrmSettingsByCode(req.params.code, req.body?.settings || req.body || {});
    return res.json({
      status: "ok",
      settings: settings?.config || getDefaultCrmSettings(),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao salvar configuracoes de CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/services/catalog", async (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const tenant = getTenantByCode(req.params.code);
    if (!tenant) {
      return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    }

    const services = await buildTenantCrmServiceCatalogWithRules(tenant.code);
    return res.json({
      status: "ok",
      tenant: { code: tenant.code, name: tenant.name, establishmentId: tenant.establishmentId },
      data: services,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar servicos do CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/services/rules", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    return res.json({
      status: "ok",
      rules: listTenantServiceReturnRulesByCode(req.params.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar regras por servico do CRM.",
      details: error.details || null,
    });
  }
});

app.put("/api/admin/tenants/:code/crm/services/rules", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const rules = upsertTenantServiceReturnRulesByCode(req.params.code, req.body?.rules || []);
    return res.json({
      status: "ok",
      rules,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao salvar regras por servico do CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/categories/rules", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    return res.json({
      status: "ok",
      rules: listTenantCategoryOpportunityRulesByCode(req.params.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar regras por categoria do CRM.",
      details: error.details || null,
    });
  }
});

app.put("/api/admin/tenants/:code/crm/categories/rules", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const rules = upsertTenantCategoryOpportunityRulesByCode(req.params.code, req.body?.rules || []);
    return res.json({
      status: "ok",
      rules,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao salvar regras por categoria do CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/blocks", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    return res.json({
      status: "ok",
      blocks: listCrmClientBlocksByCode(req.params.code, { phone: req.query.phone || "" }),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar bloqueios do CRM.",
      details: error.details || null,
    });
  }
});

app.post("/api/admin/tenants/:code/crm/blocks", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const block = upsertCrmClientBlockByCode(req.params.code, req.body || {});
    return res.json({
      status: "ok",
      block,
      blocks: listCrmClientBlocksByCode(req.params.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao salvar bloqueio do CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/flows", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    return res.json({
      status: "ok",
      data: listCrmReturnFlowsByCode(req.params.code, {
        phone: req.query.phone || "",
        status: req.query.status || "",
        limit: req.query.limit || 200,
      }),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar fluxos do CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/opportunities", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    return res.json({
      status: "ok",
      data: listCrmCategoryOpportunitiesByCode(req.params.code, {
        status: req.query.status || "",
        limit: req.query.limit || 200,
      }),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar oportunidades do CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/dashboard", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    return res.json({
      status: "ok",
      dashboard: buildTenantCrmDashboard(req.params.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao montar dashboard do CRM.",
      details: error.details || null,
    });
  }
});

app.post("/api/admin/tenants/:code/crm/preview-run", async (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }

    const preview = await runTenantCrmPreview(req.params.code, {
      lookbackDays: req.body?.lookbackDays || req.body?.days || 365,
      materialize: Boolean(req.body?.materialize),
      limit: req.body?.limit || 250,
    });

    return res.json({
      status: "ok",
      preview,
      dashboard: buildTenantCrmDashboard(req.params.code),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao rodar preview do CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/admin/tenants/:code/crm/diagnostic", async (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) return;
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }
    const tenant = getTenantByCode(req.params.code);
    if (!tenant) return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    const serviceRules = listTenantServiceReturnRulesByCode(tenant.code).filter(
      (item) => item.active && Number(item.returnDays) > 0,
    );
    const auditRows = queryRecentTenantAppointmentAudit(tenant.code, { limit: 2000 });
    const serviceNamesInHistory = new Set();
    for (const row of auditRows) {
      if (toNonEmptyString(row.serviceName)) {
        serviceNamesInHistory.add(toNonEmptyString(row.serviceName));
      }
    }
    const matchedServices = [];
    const unmatchedServices = [];
    for (const serviceName of serviceNamesInHistory) {
      const rule = findMatchingServiceRuleForName(serviceName, serviceRules);
      if (rule) {
        matchedServices.push({ serviceName, matchedRule: rule.serviceName });
      } else {
        unmatchedServices.push(serviceName);
      }
    }
    const configuredRulesNotInHistory = serviceRules.filter(
      (rule) => !Array.from(serviceNamesInHistory).some(
        (sn) => findMatchingServiceRuleForName(sn, [rule]) !== null,
      ),
    );
    return res.json({
      status: "ok",
      tenant: {
        code: tenant.code,
        name: tenant.name,
      },
      summary: {
        serviceNamesInHistory: serviceNamesInHistory.size,
        configuredRules: serviceRules.length,
        matched: matchedServices.length,
        unmatched: unmatchedServices.length,
        configuredNotInHistory: configuredRulesNotInHistory.length,
      },
      matchedServices: matchedServices.slice(0, 50),
      unmatchedServices: Array.from(unmatchedServices).slice(0, 50),
      configuredRulesNotInHistory: configuredRulesNotInHistory.map((r) => ({
        serviceName: r.serviceName,
        returnDays: r.returnDays,
        aliases: toNonEmptyString(r.serviceNameAliases || r.service_name_aliases || ""),
      })).slice(0, 50),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao diagnosticar CRM.",
    });
  }
});

// CRM Phase 6 routes: flow events, approve, stop

app.get("/api/admin/tenants/:code/crm/flows/:id/events", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) return;
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }
    const tenant = getTenantByCode(req.params.code);
    if (!tenant) return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    const flowId = Number(req.params.id);
    if (!flowId) return res.status(400).json({ status: "error", message: "ID invalido." });
    const events = getCrmFlowEventsByFlowId(flowId, tenant.id);
    const flow = getCrmFlowById(flowId, tenant.id);
    return res.json({ status: "ok", flow, events });
  } catch (error) {
    return res.status(error.status || 500).json({ status: "error", message: error.message || "Erro." });
  }
});

app.post("/api/admin/tenants/:code/crm/flows/:id/approve", async (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) return;
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }
    const tenant = getTenantByCode(req.params.code);
    if (!tenant) return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    const flowId = Number(req.params.id);
    if (!flowId) return res.status(400).json({ status: "error", message: "ID invalido." });
    const flow = getCrmFlowById(flowId, tenant.id);
    if (!flow) return res.status(404).json({ status: "error", message: "Fluxo nao encontrado." });
    if (!["pending_approval", "scheduled_step_1", "eligible"].includes(flow.flow_status)) {
      return res.status(400).json({
        status: "error",
        message: `Fluxo nao pode ser aprovado no status: ${flow.flow_status}`,
      });
    }
    const crmSettings = getTenantCrmSettingsByCode(req.params.code)?.config || getDefaultCrmSettings();
    const sendResult = await sendCrmFlowStepNow({
      tenant,
      flow,
      crmSettings,
      source: "approve",
    });
    return res.json({
      status: "ok",
      message: `Etapa ${sendResult.stepNumber} enviada com sucesso.`,
      nextStatus: sendResult.nextStatus,
      messageSent: sendResult.messageSent,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao aprovar fluxo.",
    });
  }
});

app.post("/api/admin/tenants/:code/crm/flows/:id/stop", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) return;
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }
    const tenant = getTenantByCode(req.params.code);
    if (!tenant) return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    const flowId = Number(req.params.id);
    if (!flowId) return res.status(400).json({ status: "error", message: "ID invalido." });
    const stopReason = toNonEmptyString(req.body?.reason) || "manual_stop";
    updateCrmFlowStatus(flowId, tenant.id, {
      flow_status: "stopped",
      stop_reason: stopReason,
    });
    recordCrmFlowEvent({
      flowId,
      tenantId: tenant.id,
      eventType: "stopped",
      metadata: { reason: stopReason, stoppedBy: toNonEmptyString(principal.tenantCode || "admin") },
    });
    return res.json({ status: "ok", message: "Fluxo encerrado." });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao encerrar fluxo.",
    });
  }
});

app.post("/api/admin/tenants/:code/crm/flows/:id/send-now", async (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) return;
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }
    const tenant = getTenantByCode(req.params.code);
    if (!tenant) return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    const flowId = Number(req.params.id);
    if (!flowId) return res.status(400).json({ status: "error", message: "ID invalido." });
    const flow = getCrmFlowById(flowId, tenant.id);
    if (!flow) return res.status(404).json({ status: "error", message: "Fluxo nao encontrado." });

    const crmSettings = getTenantCrmSettingsByCode(req.params.code)?.config || getDefaultCrmSettings();
    const sendResult = await sendCrmFlowStepNow({
      tenant,
      flow,
      crmSettings,
      source: "send_now",
    });
    return res.json({
      status: "ok",
      message: `Etapa ${sendResult.stepNumber} enviada manualmente.`,
      nextStatus: sendResult.nextStatus,
      messageSent: sendResult.messageSent,
      flow: getCrmFlowById(flowId, tenant.id),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao enviar etapa do CRM.",
      details: error.details || null,
    });
  }
});

app.post("/api/admin/tenants/:code/crm/flows/:id/handoff", async (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) return;
    if (!principalCanAccessTenant(principal, req.params.code)) {
      return res.status(403).json({ status: "error", message: "Sem permissao para este tenant." });
    }
    const tenant = getTenantByCode(req.params.code);
    if (!tenant) return res.status(404).json({ status: "error", message: "Tenant nao encontrado." });
    const flowId = Number(req.params.id);
    if (!flowId) return res.status(400).json({ status: "error", message: "ID invalido." });
    const flow = getCrmFlowById(flowId, tenant.id);
    if (!flow) return res.status(404).json({ status: "error", message: "Fluxo nao encontrado." });
    if (["converted", "stopped", "expired"].includes(toNonEmptyString(flow.flow_status || flow.flowStatus))) {
      return res.status(400).json({
        status: "error",
        message: `Fluxo nao permite handoff no status atual: ${flow.flow_status || flow.flowStatus}`,
      });
    }

    const crmSettings = getTenantCrmSettingsByCode(req.params.code)?.config || getDefaultCrmSettings();
    const initiatedBy = toNonEmptyString(principal.displayName || principal.username || principal.tenantCode || "admin");
    const handoffResult = await activateCrmHumanHandoff({
      tenant,
      flow,
      crmSettings,
      triggerSource: "crm_admin",
      triggerReason: toNonEmptyString(req.body?.reason) || "Encaminhado manualmente pelo CRM",
      customerMessage: toNonEmptyString(req.body?.customerMessage),
      initiatedBy,
    });

    return res.json({
      status: "ok",
      message: "Handoff humano acionado com sucesso.",
      handoff: handoffResult,
      flow: getCrmFlowById(flowId, tenant.id),
      events: getCrmFlowEventsByFlowId(flowId, tenant.id),
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao acionar handoff humano do CRM.",
      details: error.details || null,
    });
  }
});

app.get("/api/scheduling/providers", (req, res) => {
  try {
    const tenantCode = toNonEmptyString(req.query.tenantCode || req.query.tenant || "");
    const tenant = tenantCode ? getTenantByCode(tenantCode) : null;
    const providers = listSchedulingProviders({ tenantCode });
    return res.json({
      status: "ok",
      tenantCode: tenantCode || null,
      tenantFound: tenantCode ? Boolean(tenant) : null,
      defaultProvider: resolveSchedulingProviderForTenant({ tenantCode }),
      providers,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao listar providers de agenda.",
      details: error.details || null,
    });
  }
});

app.post("/api/scheduling/availability", async (req, res) => {
  try {
    const {
      establishmentId,
      service,
      date,
      professionalName,
      preferredTime,
      provider,
      tenantCode,
      tenant,
    } = req.body || {};
    const context = resolveSchedulingRequestContext({
      tenantCode,
      tenantAlias: tenant,
      establishmentId,
    });

    if (!context.establishmentId || !service || !date) {
      return res.status(400).json({ message: "Campos obrigatorios: establishmentId, service, date" });
    }

    const adapter = getSchedulingAdapter({
      provider,
      tenantCode: context.tenantCode,
    });
    const availability = await adapter.getAvailability({
      establishmentId: context.establishmentId,
      service,
      date,
      professionalName,
      preferredTime,
    });

    return res.json(availability);
  } catch (error) {
    return res.status(error.status || 500).json({
      message: error.message || "Erro ao consultar disponibilidade.",
      details: error.details || null,
    });
  }
});

app.post("/api/scheduling/appointments", async (req, res) => {
  try {
    const {
      establishmentId,
      service,
      date,
      time,
      professionalName,
      clientName,
      clientPhone,
      provider,
      tenantCode,
      tenant,
    } = req.body || {};
    const directTestSignal = detectDirectBookingTestSignal(req.body || {});
    const directTestAuthorization = resolveTrustedTestAuthorizationFromRequest(
      req,
      req.body?.testAuthorization || req.body?.testApproval,
    );

    if (STRICT_TEST_BOOKING_GUARD_ENABLED && directTestSignal.inferredTest) {
      if (!hasValidTestAuthorization(directTestAuthorization)) {
        return res.status(403).json(buildTestAuthorizationBlock({ signal: directTestSignal }));
      }
    }

    const context = resolveSchedulingRequestContext({
      tenantCode,
      tenantAlias: tenant,
      establishmentId,
    });

    if (!context.establishmentId || !service || !date || !time || !clientName || !clientPhone) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, service, date, time, clientName, clientPhone",
      });
    }

    const adapter = getSchedulingAdapter({
      provider,
      tenantCode: context.tenantCode,
    });
    const createdAppointment = await adapter.createAppointment({
      establishmentId: context.establishmentId,
      tenantCode: context.tenantCode,
      service,
      date,
      time,
      professionalName,
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

app.post("/api/scheduling/appointments/day", async (req, res) => {
  try {
    const { establishmentId, date, provider, tenantCode, tenant } = req.body || {};
    const context = resolveSchedulingRequestContext({
      tenantCode,
      tenantAlias: tenant,
      establishmentId,
    });

    if (!context.establishmentId || !date) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, date",
      });
    }

    const adapter = getSchedulingAdapter({
      provider,
      tenantCode: context.tenantCode,
    });
    const result = await adapter.getAppointmentsDay({ establishmentId: context.establishmentId, date });
    return res.json(result);
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao buscar agendamentos do dia.",
      details: error.details || null,
    });
  }
});

app.post("/api/scheduling/professionals", async (req, res) => {
  try {
    const { establishmentId, date, serviceId, provider, tenantCode, tenant } = req.body || {};
    const context = resolveSchedulingRequestContext({
      tenantCode,
      tenantAlias: tenant,
      establishmentId,
    });

    if (!context.establishmentId || !date) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, date",
      });
    }

    const adapter = getSchedulingAdapter({
      provider,
      tenantCode: context.tenantCode,
    });
    const result = await adapter.getProfessionals({
      establishmentId: context.establishmentId,
      date,
      serviceId,
    });
    return res.json(result);
  } catch (error) {
    return res.status(error.status || 500).json({
      status: "error",
      message: error.message || "Erro ao buscar profissionais.",
      details: error.details || null,
    });
  }
});

app.post("/api/scheduling/appointments/reschedule", async (req, res) => {
  try {
    const {
      establishmentId,
      confirmationCode,
      appointmentId,
      date,
      time,
      provider,
      tenantCode,
      tenant,
    } = req.body || {};

    const context = resolveSchedulingRequestContext({
      tenantCode,
      tenantAlias: tenant,
      establishmentId,
    });

    if (!context.establishmentId || !date || !time || (!confirmationCode && !appointmentId)) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, date, time e (confirmationCode ou appointmentId)",
      });
    }

    const adapter = getSchedulingAdapter({
      provider,
      tenantCode: context.tenantCode,
    });
    const result = await adapter.rescheduleAppointment({
      establishmentId: context.establishmentId,
      tenantCode: context.tenantCode,
      confirmationCode,
      appointmentId,
      date,
      time,
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

app.post("/api/scheduling/appointments/cancel", async (req, res) => {
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
      provider,
      tenantCode,
      tenant,
    } = req.body || {};

    const context = resolveSchedulingRequestContext({
      tenantCode,
      tenantAlias: tenant,
      establishmentId,
    });

    if (!context.establishmentId || (!confirmationCode && !appointmentId && !clientPhone)) {
      return res.status(400).json({
        message:
          "Campos obrigatorios: establishmentId e (confirmationCode ou appointmentId ou clientPhone)",
      });
    }

    const adapter = getSchedulingAdapter({
      provider,
      tenantCode: context.tenantCode,
    });
    const result = await adapter.cancelAppointment({
      establishmentId: context.establishmentId,
      tenantCode: context.tenantCode,
      confirmationCode,
      appointmentId,
      reason,
      clientPhone,
      clientName,
      date,
      time,
      service,
      professionalName,
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
    const directTestSignal = detectDirectBookingTestSignal(req.body || {});
    const directTestAuthorization = resolveTrustedTestAuthorizationFromRequest(
      req,
      req.body?.testAuthorization || req.body?.testApproval,
    );

    if (STRICT_TEST_BOOKING_GUARD_ENABLED && directTestSignal.inferredTest) {
      if (!hasValidTestAuthorization(directTestAuthorization)) {
        return res.status(403).json(buildTestAuthorizationBlock({ signal: directTestSignal }));
      }
    }

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
    const { establishmentId, message, history, customerContext, tenantCode, tenant, instance, instanceName } = req.body || {};
    const baseCustomerContext =
      customerContext && typeof customerContext === "object" && !Array.isArray(customerContext)
        ? { ...customerContext }
        : {};
    if (req.body?.testMode !== undefined || req.body?.isTest !== undefined) {
      baseCustomerContext.testMode = Boolean(req.body?.testMode || req.body?.isTest);
    }
    const testAuthorization = resolveTrustedTestAuthorizationFromRequest(
      req,
      req.body?.testAuthorization || req.body?.testApproval || baseCustomerContext?.testAuthorization,
    );
    if (testAuthorization) {
      baseCustomerContext.testAuthorization = testAuthorization;
    }
    if (isInternalTestPhone(baseCustomerContext?.phone)) {
      baseCustomerContext.internalTester = true;
    }

    const context = resolveConversationTenantContext({
      tenantCode,
      tenantAlias: tenant,
      instanceName: firstNonEmpty([instance, instanceName]),
      establishmentId,
    });

    if (!message) {
      return res.status(400).json({
        message: "Campo obrigatorio: message",
      });
    }

    if (!context.establishmentId) {
      return res.status(400).json({
        message: "Informe establishmentId valido ou tenantCode com establishmentId cadastrado.",
      });
    }

    const response = await sendChatMessage({
      establishmentId: Number(context.establishmentId),
      message: String(message),
      history: Array.isArray(history) ? history : [],
      customerContext: baseCustomerContext,
      knowledge: context.knowledge,
      tenantCode: context.tenantCode,
    });

    const text = toNonEmptyString(response?.text || response);
    const marketingMedia = response?.marketingMedia && typeof response.marketingMedia === "object"
      ? response.marketingMedia
      : null;

    return res.json({ text, marketingMedia });
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
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    const requestedTenantCode = normalizeTenantCode(req.query.tenantCode || req.query.tenant || "");
    const tenantCode = principal.role === "tenant"
      ? normalizeTenantCode(principal.tenantCode)
      : requestedTenantCode;
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 500);
    const rows = tenantCode
      ? db.prepare(
        `
          SELECT m.phone,
                 m.content AS lastMessage,
                 m.role AS lastRole,
                 m.at AS updatedAt,
                 (
                   SELECT sender_name
                   FROM whatsapp_messages u
                   WHERE u.phone = m.phone
                     AND u.tenant_code = m.tenant_code
                     AND u.role = 'user'
                     AND COALESCE(u.sender_name, '') <> ''
                   ORDER BY datetime(u.at) DESC, u.id DESC
                   LIMIT 1
                 ) AS name,
                 (
                   SELECT COUNT(*)
                   FROM whatsapp_messages c
                   WHERE c.phone = m.phone
                     AND c.tenant_code = m.tenant_code
                 ) AS count
          FROM whatsapp_messages m
          JOIN (
            SELECT tenant_code, phone, MAX(id) AS max_id
            FROM whatsapp_messages
            WHERE tenant_code = ?
            GROUP BY tenant_code, phone
          ) latest ON latest.max_id = m.id
          ORDER BY datetime(m.at) DESC, m.id DESC
          LIMIT ?
        `,
      ).all(tenantCode, limit)
      : db.prepare(
        `
          SELECT m.phone,
                 m.content AS lastMessage,
                 m.role AS lastRole,
                 m.at AS updatedAt,
                 (
                   SELECT sender_name
                   FROM whatsapp_messages u
                   WHERE u.phone = m.phone
                     AND u.tenant_code = m.tenant_code
                     AND u.role = 'user'
                     AND COALESCE(u.sender_name, '') <> ''
                   ORDER BY datetime(u.at) DESC, u.id DESC
                   LIMIT 1
                 ) AS name,
                 (
                   SELECT COUNT(*)
                   FROM whatsapp_messages c
                   WHERE c.phone = m.phone
                     AND c.tenant_code = m.tenant_code
                 ) AS count
          FROM whatsapp_messages m
          JOIN (
            SELECT tenant_code, phone, MAX(id) AS max_id
            FROM whatsapp_messages
            GROUP BY tenant_code, phone
          ) latest ON latest.max_id = m.id
          ORDER BY datetime(m.at) DESC, m.id DESC
          LIMIT ?
        `,
      ).all(limit);

    return res.json({ status: "ok", tenantCode: tenantCode || null, data: rows });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: error.message || "Erro ao consultar conversas no banco.",
    });
  }
});

app.get("/api/db/messages", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    const requestedTenantCode = normalizeTenantCode(req.query.tenantCode || req.query.tenant || "");
    const tenantCode = principal.role === "tenant"
      ? normalizeTenantCode(principal.tenantCode)
      : requestedTenantCode;
    const phone = normalizePhone(req.query.phone || "");
    if (!phone) {
      return res.status(400).json({ message: "Informe ?phone=numero" });
    }

    const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 1000);
    const rows = tenantCode
      ? db.prepare(
        `
          SELECT id, phone, role, content, sender_name AS senderName, at, source, tenant_code AS tenantCode
          FROM whatsapp_messages
          WHERE phone = ?
            AND tenant_code = ?
          ORDER BY datetime(at) DESC, id DESC
          LIMIT ?
        `,
      ).all(phone, tenantCode, limit)
      : db.prepare(
        `
          SELECT id, phone, role, content, sender_name AS senderName, at, source, tenant_code AS tenantCode
          FROM whatsapp_messages
          WHERE phone = ?
          ORDER BY datetime(at) DESC, id DESC
          LIMIT ?
        `,
      ).all(phone, limit);

    return res.json({ status: "ok", phone, tenantCode: tenantCode || null, messages: rows.reverse() });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: error.message || "Erro ao consultar mensagens no banco.",
    });
  }
});

app.get("/api/db/appointments-audit", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    const requestedTenantCode = normalizeTenantCode(req.query.tenantCode || req.query.tenant || "");
    const tenantCode = principal.role === "tenant"
      ? normalizeTenantCode(principal.tenantCode)
      : requestedTenantCode;
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
    if (tenantCode) {
      conditions.push("tenant_code = ?");
      params.push(tenantCode);
    }
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

    return res.json({ status: "ok", tenantCode: tenantCode || null, data: rows });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: error.message || "Erro ao consultar auditoria de agendamentos.",
    });
  }
});

app.get("/api/db/webhook-events", (req, res) => {
  try {
    const principal = requireAdminPrincipal(req, res);
    if (!principal) {
      return;
    }

    const requestedTenantCode = normalizeTenantCode(req.query.tenantCode || req.query.tenant || "");
    const tenantCode = principal.role === "tenant"
      ? normalizeTenantCode(principal.tenantCode)
      : requestedTenantCode;
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
    if (tenantCode) {
      conditions.push("tenant_code = ?");
      params.push(tenantCode);
    }
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

    return res.json({ status: "ok", tenantCode: tenantCode || null, data: rows });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: error.message || "Erro ao consultar eventos de webhook.",
    });
  }
});

app.get("/api/whatsapp/inbox", (req, res) => {
  const principal = requireAdminPrincipal(req, res);
  if (!principal) {
    return;
  }

  const requestedTenantCode = normalizeTenantCode(req.query.tenantCode || req.query.tenant || "");
  const tenantCode = principal.role === "tenant"
    ? normalizeTenantCode(principal.tenantCode)
    : requestedTenantCode;
  const conversations = summarizeWhatsappConversations({ tenantCode });
  return res.json({ status: "ok", tenantCode: tenantCode || null, conversations });
});

app.get("/api/whatsapp/messages", (req, res) => {
  const principal = requireAdminPrincipal(req, res);
  if (!principal) {
    return;
  }

  const requestedTenantCode = normalizeTenantCode(req.query.tenantCode || req.query.tenant || "");
  const tenantCode = principal.role === "tenant"
    ? normalizeTenantCode(principal.tenantCode)
    : requestedTenantCode;
  const phone = normalizePhone(req.query.phone || "");
  if (!phone) {
    return res.status(400).json({ message: "Informe ?phone=numero" });
  }

  const messages = getWhatsappHistory(phone, { tenantCode });
  return res.json({ status: "ok", phone, tenantCode: tenantCode || null, messages });
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

    const conversationContext = resolveConversationTenantContext({
      tenantCode: req.body?.tenantCode || req.body?.tenant || "",
      instanceName: instance,
      establishmentId: resolveConversationTenantFallbackEstablishmentId(),
    });
    const tenantCode = normalizeTenantScopeCode(conversationContext.tenantCode);
    if (!tenantCode && isMultiTenantModeActive()) {
      return res.status(422).json({
        status: "error",
        message:
          "Nao foi possivel identificar o tenant da instancia informada. Verifique o identificador evolution_instance para evitar mistura entre saloes.",
      });
    }

    if (normalizedTarget && /^\/(retomar(\s|-)?ia|ia\s+on)$/i.test(trimmedText)) {
      clearHumanHandoffSession(normalizedTarget, tenantCode);
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
        tenantCode,
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
      tenantCode,
      instanceName: instance,
    });

    const normalized = normalizePhone(to);
    if (normalized) {
      pushWhatsappHistory(normalized, "assistant", text, "", { tenantCode });
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

  const context = resolveConversationTenantContext({
    tenantCode: req.query.tenantCode || req.query.tenant || "",
    instanceName: req.query.instance || req.query.instanceName || "",
    establishmentId: req.query.establishmentId || null,
  });
  const tenantCode = normalizeTenantScopeCode(context.tenantCode);
  const session = getHumanHandoffSession(phone, tenantCode);
  return res.json({
    status: "ok",
    phone,
    tenantCode: tenantCode || null,
    active: Boolean(session?.active),
    session: session || null,
  });
});

app.post("/api/handoff/activate", (req, res) => {
  const phone = normalizePhone(req.body?.phone || req.body?.clientPhone || "");
  if (!phone) {
    return res.status(400).json({ message: "Campo obrigatorio: phone" });
  }

  const context = resolveConversationTenantContext({
    tenantCode: req.body?.tenantCode || req.body?.tenant || "",
    instanceName: req.body?.instance || req.body?.instanceName || "",
    establishmentId: req.body?.establishmentId || null,
  });
  const tenantCode = normalizeTenantScopeCode(context.tenantCode);
  const session = setHumanHandoffSession(phone, {
    source: "api",
    reason: toNonEmptyString(req.body?.reason) || "Ativado por endpoint",
    customerName: toNonEmptyString(req.body?.customerName),
    tenantCode,
  });

  return res.json({
    status: "ok",
    action: "handoffActivated",
    phone,
    tenantCode: tenantCode || null,
    session,
  });
});

app.post("/api/handoff/resume", async (req, res) => {
  try {
    const phone = normalizePhone(req.body?.phone || req.body?.clientPhone || "");
    if (!phone) {
      return res.status(400).json({ message: "Campo obrigatorio: phone" });
    }

    const conversationContext = resolveConversationTenantContext({
      tenantCode: req.body?.tenantCode || req.body?.tenant || "",
      instanceName: req.body?.instance || req.body?.instanceName || "",
      establishmentId: req.body?.establishmentId || resolveConversationTenantFallbackEstablishmentId(),
    });
    const tenantCode = normalizeTenantScopeCode(conversationContext.tenantCode);
    const hadSession = Boolean(getHumanHandoffSession(phone, tenantCode));
    clearHumanHandoffSession(phone, tenantCode);

    const notifyClient = Boolean(req.body?.notifyClient);
    if (notifyClient) {
      const instance = resolveEvolutionInstance(req.body?.instance || req.body?.instanceName);
      if (!instance) {
        return res.status(400).json({
          message: "Para notifyClient=true, informe instance/instanceName ou configure EVOLUTION_INSTANCE.",
        });
      }

      const conciergeName = resolveConciergeDisplayName(
        conversationContext.knowledge,
        conversationContext.tenantCode,
      );
      const defaultResumeMessage = `Perfeito. Voltei com o atendimento automatico da ${conciergeName} para te ajudar.`;
      const message =
        toNonEmptyString(req.body?.message) ||
        defaultResumeMessage;

      await evolutionRequest(`/message/sendText/${instance}`, {
        method: "POST",
        body: { number: phone, text: message },
        tenantCode,
        instanceName: instance,
      });
      pushWhatsappHistory(phone, "assistant", message, "", { tenantCode });
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
    const inferTenantCodeFromInstance = (instanceName = "") => {
      const context = resolveConversationTenantContext({
        instanceName,
        establishmentId: resolveConversationTenantFallbackEstablishmentId(),
      });
      return normalizeTenantScopeCode(context.tenantCode);
    };

    if (payloadParse.parseError) {
      const inferredSender = incoming.senderNumber || inferSenderFromRawWebhookText(payloadParse.rawText);
      recordWebhookEvent({
        tenantCode: inferTenantCodeFromInstance(incoming.instanceName),
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
        tenantCode: inferTenantCodeFromInstance(incoming.instanceName),
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
        tenantCode: inferTenantCodeFromInstance(incoming.instanceName),
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
        tenantCode: inferTenantCodeFromInstance(incoming.instanceName),
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

    const conversationContext = resolveConversationTenantContext({
      instanceName: incoming.instanceName,
      establishmentId: resolveConversationTenantFallbackEstablishmentId(),
    });
    const webhookTenantCode = normalizeTenantScopeCode(conversationContext.tenantCode);
    const establishmentId = conversationContext.establishmentId;
    if (!webhookTenantCode && isMultiTenantModeActive()) {
      recordWebhookEvent({
        tenantCode: "",
        event: incoming.event,
        instanceName: incoming.instanceName,
        senderRaw: incoming.senderRaw,
        senderNumber: incoming.senderNumber,
        senderName: incoming.senderName,
        messageId: incoming.messageId,
        messageType: incoming.messageType,
        messageText: incoming.messageText,
        status: "ignored",
        reason: "unresolvedTenantInstance",
        details: {
          instanceName: incoming.instanceName || "",
          note:
            "Instancia sem mapeamento de tenant em ambiente multi-tenant. Mensagem ignorada para evitar mistura.",
        },
      });
      return res.status(200).json({
        received: true,
        ignored: true,
        reason: "unresolvedTenantInstance",
      });
    }

    if (!incoming.messageText) {
      const messageEvent = isLikelyIncomingMessageEvent(incoming);
      if (messageEvent) {
        const knownClient = establishmentId
          ? await findExistingClientByPhone(establishmentId, incoming.senderNumber).catch(() => null)
          : null;
        const knownClientName = toNonEmptyString(clientDisplayNameFrom(knownClient));
        const effectiveClientName = knownClientName || incoming.senderName;
        const instance = resolveEvolutionInstance(incoming.instanceName);
        const placeholder = unsupportedInboundPlaceholder(incoming);

        pushWhatsappHistory(incoming.senderNumber, "user", placeholder, effectiveClientName, {
          tenantCode: webhookTenantCode,
        });

        if (instance) {
          await evolutionRequest(`/message/sendText/${instance}`, {
            method: "POST",
            body: {
              number: incoming.senderNumber,
              text: UNSUPPORTED_MESSAGE_REPLY,
            },
          });
          pushWhatsappHistory(incoming.senderNumber, "assistant", UNSUPPORTED_MESSAGE_REPLY, "", {
            tenantCode: webhookTenantCode,
          });
        }

        recordWebhookEvent({
          tenantCode: webhookTenantCode,
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
        tenantCode: webhookTenantCode,
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

    if (isDuplicateIncomingWhatsapp(incoming, { tenantCode: webhookTenantCode, instanceName: incoming.instanceName })) {
      recordWebhookEvent({
        tenantCode: webhookTenantCode,
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

    const previousHistory = getWhatsappHistory(incoming.senderNumber, {
      tenantCode: webhookTenantCode,
    });
    pushWhatsappHistory(incoming.senderNumber, "user", incoming.messageText, effectiveClientName, {
      tenantCode: webhookTenantCode,
    });
    const webhookTenant = webhookTenantCode ? getTenantByCode(webhookTenantCode) : null;
    const activeCrmFlow = webhookTenant?.id
      ? getActiveCrmFlowForPhone(webhookTenant.id, incoming.senderNumber)
      : null;
    const webhookCrmSettings = webhookTenant?.code
      ? (getTenantCrmSettingsByCode(webhookTenant.code)?.config || getDefaultCrmSettings())
      : null;

    const activeBotSession = getBotAutoClosedSession(incoming.senderNumber, webhookTenantCode);
    if (activeBotSession?.active) {
      if (isExplicitHumanReopenMessage(incoming.messageText)) {
        clearBotAutoClosedSession(incoming.senderNumber, webhookTenantCode);
      } else {
        recordWebhookEvent({
          tenantCode: webhookTenantCode,
          event: incoming.event,
          instanceName: instance,
          senderRaw: incoming.senderRaw,
          senderNumber: incoming.senderNumber,
          senderName: effectiveClientName,
          messageId: incoming.messageId,
          messageType: incoming.messageType,
          messageText: incoming.messageText,
          status: "processed",
          reason: "botConversationClosedIgnored",
          details: {
            botSessionCreatedAt: Number(activeBotSession.createdAt || 0),
            botSessionExpiresAt: Number(activeBotSession.expiresAt || 0),
          },
        });

        return res.status(200).json({
          received: true,
          processed: true,
          botConversationClosed: true,
          reason: "botConversationClosedIgnored",
          instance,
          to: incoming.senderNumber,
          event: incoming.event || null,
          messageId: incoming.messageId || null,
        });
      }
    }

    const botClosureDecision = shouldAutoCloseBotConversation({
      message: incoming.messageText,
      history: previousHistory,
      knownClientName,
    });
    if (botClosureDecision.shouldClose) {
      const closureText = buildBotAutoCloseMessage({
        tenantName: resolveTenantDisplayName(
          conversationContext.knowledge,
          conversationContext.tenantCode,
        ),
      });

      await evolutionRequest(`/message/sendText/${instance}`, {
        method: "POST",
        body: {
          number: incoming.senderNumber,
          text: closureText,
        },
      });
      pushWhatsappHistory(incoming.senderNumber, "assistant", closureText, "", {
        tenantCode: webhookTenantCode,
      });
      setBotAutoClosedSession(incoming.senderNumber, webhookTenantCode, {
        source: "autoDetection",
        detection: {
          score: botClosureDecision.signal.score,
          reasons: botClosureDecision.signal.reasons,
          recentHits: botClosureDecision.recentHits,
          repeatedCount: botClosureDecision.repeatedCount,
        },
        messageId: incoming.messageId || "",
      });

      recordWebhookEvent({
        tenantCode: webhookTenantCode,
        event: incoming.event,
        instanceName: instance,
        senderRaw: incoming.senderRaw,
        senderNumber: incoming.senderNumber,
        senderName: effectiveClientName,
        messageId: incoming.messageId,
        messageType: incoming.messageType,
        messageText: incoming.messageText,
        status: "processed",
        reason: "botConversationAutoClosed",
        details: {
          score: botClosureDecision.signal.score,
          reasons: botClosureDecision.signal.reasons,
          recentHits: botClosureDecision.recentHits,
          repeatedCount: botClosureDecision.repeatedCount,
        },
      });

      return res.status(200).json({
        received: true,
        processed: true,
        botConversationClosed: true,
        reason: "botConversationAutoClosed",
        instance,
        to: incoming.senderNumber,
        event: incoming.event || null,
        messageId: incoming.messageId || null,
      });
    }

    if (isHumanHandoffEnabled()) {
      const askedHuman = isHumanHandoffRequest(incoming.messageText);
      const askedResume = isHumanHandoffResumeRequest(incoming.messageText);
      const activeHandoff = getHumanHandoffSession(incoming.senderNumber, webhookTenantCode);

      if (askedResume && activeHandoff?.active) {
        clearHumanHandoffSession(incoming.senderNumber, webhookTenantCode);

        const conciergeName = resolveConciergeDisplayName(
          conversationContext.knowledge,
          conversationContext.tenantCode,
        );
        const resumeMessage =
          toNonEmptyString(process.env.HUMAN_HANDOFF_RESUME_MESSAGE) ||
          `Perfeito. Voltei com o atendimento automatico da ${conciergeName} para te ajudar.`;

        await evolutionRequest(`/message/sendText/${instance}`, {
          method: "POST",
          body: {
            number: incoming.senderNumber,
            text: resumeMessage,
          },
        });
        pushWhatsappHistory(incoming.senderNumber, "assistant", resumeMessage, "", {
          tenantCode: webhookTenantCode,
        });
        recordWebhookEvent({
          tenantCode: webhookTenantCode,
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
        let session = null;
        let alertResult = null;
        let ackMessage = "";
        if (webhookTenant && webhookCrmSettings?.humanHandoffEnabled) {
          const handoffResult = await activateCrmHumanHandoff({
            tenant: webhookTenant,
            flow: activeCrmFlow,
            crmSettings: webhookCrmSettings,
            instance,
            phone: incoming.senderNumber,
            customerName: effectiveClientName,
            customerMessage: incoming.messageText,
            triggerSource: "customer_request",
            triggerReason: incoming.messageText,
            initiatedBy: "cliente_whatsapp",
          });
          session = handoffResult.session;
          alertResult = handoffResult.internalAlert;
          ackMessage = handoffResult.clientMessage;
        } else {
          session = setHumanHandoffSession(incoming.senderNumber, {
            source: "customerRequest",
            reason: incoming.messageText,
            establishmentId,
            customerName: effectiveClientName,
            messageId: incoming.messageId || "",
            tenantCode: webhookTenantCode,
          });

          ackMessage =
            toNonEmptyString(process.env.HUMAN_HANDOFF_ACK_MESSAGE) ||
            "Perfeito. Vou acionar nossa recepcao agora e um atendente humano segue com voce.";

          await evolutionRequest(`/message/sendText/${instance}`, {
            method: "POST",
            body: {
              number: incoming.senderNumber,
              text: ackMessage,
            },
          });
          pushWhatsappHistory(incoming.senderNumber, "assistant", ackMessage, "", {
            tenantCode: webhookTenantCode,
          });

          alertResult = await notifyHumanAlertPhones({
            instance,
            establishmentId,
            customerPhone: incoming.senderNumber,
            customerName: effectiveClientName,
            customerMessage: incoming.messageText,
          });
        }
        recordWebhookEvent({
          tenantCode: webhookTenantCode,
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
          pushWhatsappHistory(incoming.senderNumber, "assistant", waitingMessage, "", {
            tenantCode: webhookTenantCode,
          });

          setHumanHandoffSession(incoming.senderNumber, {
            ...activeHandoff,
            lastWaitAckAt: now,
            tenantCode: webhookTenantCode,
          });
        }

        recordWebhookEvent({
          tenantCode: webhookTenantCode,
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

    // ---- CRM Phase 6: reply detection, opt-out, context injection ----
    let crmContext = null;

    if (activeCrmFlow) {
      if (isCrmOptOutText(incoming.messageText)) {
        upsertCrmClientBlockByCode(webhookTenant.code, {
          clientId: activeCrmFlow.client_id,
          clientName: toNonEmptyString(activeCrmFlow.client_name || effectiveClientName),
          phone: incoming.senderNumber,
          isBlocked: true,
          blockReason: "opt_out",
          blockNotes: "Bloqueio automatico por pedido da cliente no fluxo CRM.",
          blockedBy: "crm_opt_out",
        });
        stopCrmFlowForSystemReason(activeCrmFlow.id, webhookTenant.id, "opt_out", {
          source: "webhook",
          phone: normalizePhone(incoming.senderNumber),
        });
        recordCrmFlowEvent({
          flowId: activeCrmFlow.id,
          tenantId: webhookTenant.id,
          eventType: "opted_out",
          replySummary: incoming.messageText.slice(0, 300),
        });
        const optOutAck =
          "Tudo bem! Vou remover voce da nossa lista de comunicacoes. Se quiser agendar no futuro, estaremos aqui.";
        await evolutionRequest(`/message/sendText/${instance}`, {
          method: "POST",
          body: { number: incoming.senderNumber, text: optOutAck },
        });
        pushWhatsappHistory(incoming.senderNumber, "assistant", optOutAck, "", {
          tenantCode: webhookTenantCode,
        });
        recordWebhookEvent({
          tenantCode: webhookTenantCode,
          event: incoming.event,
          instanceName: instance,
          senderRaw: incoming.senderRaw,
          senderNumber: incoming.senderNumber,
          senderName: effectiveClientName,
          messageId: incoming.messageId,
          messageType: incoming.messageType,
          messageText: incoming.messageText,
          status: "processed",
          reason: "crmOptOut",
        });
        return res.status(200).json({
          received: true,
          processed: true,
          reason: "crmOptOut",
          instance,
          to: incoming.senderNumber,
          event: incoming.event || null,
          messageId: incoming.messageId || null,
        });
      }

      recordCrmFlowEvent({
        flowId: activeCrmFlow.id,
        tenantId: webhookTenant.id,
        eventType: "reply",
        step: activeCrmFlow.current_step || null,
        replySummary: incoming.messageText.slice(0, 300),
      });

      const scheduledCrmStatuses = ["scheduled_step_1", "scheduled_step_2", "scheduled_step_3", "pending_approval"];
      if (scheduledCrmStatuses.includes(activeCrmFlow.flow_status)) {
        updateCrmFlowStatus(activeCrmFlow.id, webhookTenant.id, { flow_status: "in_progress" });
      }

      crmContext = {
        originServiceName: toNonEmptyString(activeCrmFlow.origin_service_name),
        originCategoryName: toNonEmptyString(activeCrmFlow.origin_category_name),
        lastVisitAt: toNonEmptyString(activeCrmFlow.last_visit_at),
        currentStep: Number(activeCrmFlow.current_step) || 0,
        lastProfessionalName: toNonEmptyString(activeCrmFlow.last_professional_name),
        lastProfessionalActive: activeCrmFlow.last_professional_active == null
          ? null
          : Number(activeCrmFlow.last_professional_active) !== 0,
      };
    }
    // ---- end CRM pre-processing ----

    const answerPayload = await sendChatMessage({
      establishmentId,
      message: incoming.messageText,
      history: previousHistory,
      customerContext: {
        name: effectiveClientName,
        phone: incoming.senderNumber,
        fromTrinks: Boolean(knownClientName),
        internalTester: isInternalTestPhone(incoming.senderNumber),
      },
      knowledge: conversationContext.knowledge,
      tenantCode: conversationContext.tenantCode,
      crmContext,
    });

    // ---- CRM Phase 6: conversion detection ----
    if (activeCrmFlow && webhookTenant?.id) {
      try {
        const recentBooking = findVeryRecentBookingAuditForPhone(
          webhookTenantCode,
          incoming.senderNumber,
          180,
        );
        if (recentBooking) {
          updateCrmFlowStatus(activeCrmFlow.id, webhookTenant.id, {
            flow_status: "converted",
            converted_at: new Date().toISOString(),
            converted_appointment_id: recentBooking.appointmentId ? Number(recentBooking.appointmentId) : null,
            stop_reason: "",
          });
          recordCrmFlowEvent({
            flowId: activeCrmFlow.id,
            tenantId: webhookTenant.id,
            eventType: "converted",
            step: Number(activeCrmFlow.current_step) || null,
            bookingId: recentBooking.appointmentId ? Number(recentBooking.appointmentId) : null,
            metadata: { confirmationCode: recentBooking.confirmationCode || "" },
          });
        }
      } catch (crmConvertError) {
        console.error("[crm] conversion detection error:", crmConvertError?.message);
      }
    }
    // ---- end CRM conversion detection ----

    let answer = toNonEmptyString(answerPayload?.text || answerPayload);
    const marketingMedia = answerPayload?.marketingMedia && typeof answerPayload.marketingMedia === "object"
      ? answerPayload.marketingMedia
      : null;
    let marketingMediaSent = false;
    let marketingMediaError = "";

    if (marketingMedia?.url) {
      try {
        await sendEvolutionImageMessage({
          instance,
          number: incoming.senderNumber,
          mediaUrl: marketingMedia.url,
          caption: toNonEmptyString(marketingMedia.caption),
        });
        marketingMediaSent = true;
      } catch (mediaError) {
        marketingMediaError = toNonEmptyString(mediaError?.message || "");
        console.error("[marketing] failed to send media offer:", mediaError?.message || mediaError);
      }
    }

    if (marketingMedia?.url && !marketingMediaSent) {
      const fallbackOffer = [
        toNonEmptyString(marketingMedia.caption),
        `Imagem da oferta: ${toNonEmptyString(marketingMedia.url)}`,
      ]
        .filter(Boolean)
        .join("\n");
      if (fallbackOffer) {
        answer = `${fallbackOffer}\n\n${answer}`;
      }
    }

    await evolutionRequest(`/message/sendText/${instance}`, {
      method: "POST",
      body: {
        number: incoming.senderNumber,
        text: answer,
      },
    });

    pushWhatsappHistory(incoming.senderNumber, "assistant", answer, "", {
      tenantCode: webhookTenantCode,
    });
    recordWebhookEvent({
      tenantCode: webhookTenantCode,
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
      details: marketingMedia?.url
        ? {
            marketingMedia: {
              actionId: toNonEmptyString(marketingMedia.actionId),
              actionName: toNonEmptyString(marketingMedia.actionName),
              url: toNonEmptyString(marketingMedia.url),
              sent: marketingMediaSent,
              error: marketingMediaSent ? "" : marketingMediaError,
            },
          }
        : null,
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
        const fallbackTenantCode = normalizeTenantScopeCode(
          resolveConversationTenantContext({
            instanceName: incoming?.instanceName,
            establishmentId: resolveConversationTenantFallbackEstablishmentId(),
          }).tenantCode,
        );
        pushWhatsappHistory(fallbackPhone, "assistant", fallbackText, "", {
          tenantCode: fallbackTenantCode,
        });
      } catch {
        // Evita falha em cascata: ainda devolvemos 200 para o webhook.
      }
    }

    recordWebhookEvent({
      tenantCode: normalizeTenantScopeCode(
        resolveConversationTenantContext({
          instanceName: incoming?.instanceName,
          establishmentId: resolveConversationTenantFallbackEstablishmentId(),
        }).tenantCode,
      ),
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

// CRM return-flow scheduler: sends steps 2 and 3 automatically
async function runCrmReturnFlowScheduler() {
  try {
    const tenants = listTenants({ includeInactive: false });
    for (const tenant of tenants) {
      if (!tenant.id || !tenant.code) continue;
      const settings = getTenantCrmSettingsByCode(tenant.code)?.config;
      if (!settings?.crmReturnEnabled) continue;
      const { isoToday } = getSaoPauloDateContext();
      const currentClockTime = getSaoPauloClockTime();
      if (
        !isTimeWithinCrmWindow(
          currentClockTime,
          settings.messageSendingWindowStart,
          settings.messageSendingWindowEnd,
        )
      ) {
        continue;
      }
      const dailyLimit = Number(settings.messageDailyLimit || 0);
      let sentTodayCount = countTenantCrmStepSentOnIsoDate(tenant.id, isoToday);
      if (dailyLimit > 0 && sentTodayCount >= dailyLimit) {
        continue;
      }
      const nowIso = new Date().toISOString();
      const dueFlows = db.prepare(
        `SELECT * FROM crm_return_flows
         WHERE tenant_id = ?
           AND flow_status IN ('scheduled_step_2', 'scheduled_step_3')
           AND next_scheduled_send_at IS NOT NULL AND next_scheduled_send_at != ''
           AND next_scheduled_send_at <= ?
         LIMIT 50`,
      ).all(tenant.id, nowIso);
      if (!dueFlows.length) continue;
      const instance = resolveEvolutionInstance(null, { tenantCode: tenant.code });
      if (!instance) continue;
      const futureBookingCache = new Map();
      for (const flow of dueFlows) {
        if (dailyLimit > 0 && sentTodayCount >= dailyLimit) {
          break;
        }
        try {
          if (isCrmPhoneBlockedForTenantCode(tenant.code, flow.phone)) {
            stopCrmFlowForSystemReason(flow.id, tenant.id, "client_blocked", {
              source: "scheduler",
              phone: normalizePhone(flow.phone),
            });
            continue;
          }
          if (settings.stopFlowOnAnyFutureBooking) {
            const futureBooking = await detectFutureBookingForPhone(
              tenant,
              flow.phone,
              futureBookingCache,
              isoToday,
            );
            if (futureBooking?.hasFutureBooking) {
              stopCrmFlowForSystemReason(flow.id, tenant.id, "future_booking", {
                source: "scheduler",
                phone: normalizePhone(flow.phone),
                firstFutureBooking: futureBooking.firstFutureBooking || null,
              });
              continue;
            }
          }
          const stepNumber = flow.flow_status === "scheduled_step_2" ? 2 : 3;
          const serviceRule = db.prepare(
            `SELECT * FROM tenant_service_return_rules WHERE tenant_id = ? AND service_key = ? LIMIT 1`,
          ).get(tenant.id, flow.origin_service_key);
          const template = toNonEmptyString(
            stepNumber === 2 ? serviceRule?.step2_message_template : serviceRule?.step3_message_template,
          );
          const msg = formatCrmStepMessage(
            template || "Ola {{client_name}}! Passamos para lembrar que voce pode agendar seu {{service_name}} conosco.",
            {
              clientName: toNonEmptyString(flow.client_name),
              serviceName: toNonEmptyString(flow.origin_service_name),
              lastVisitAt: toNonEmptyString(flow.last_visit_at),
              humanNumber: toNonEmptyString(settings.humanHandoffClientNumber),
            },
          );
          await evolutionRequest(`/message/sendText/${instance}`, {
            method: "POST",
            body: { number: flow.phone, text: msg },
          });
          const maxSteps = Number(settings.maxSteps || 3);
          const isLast = stepNumber >= maxSteps;
          const step3DelayDays = Number(serviceRule?.step3_delay_days || 14);
          const nextAt = addDaysToIsoDate(nowIso.slice(0, 10), step3DelayDays) + "T09:00:00";
          updateCrmFlowStatus(flow.id, tenant.id, {
            flow_status: isLast ? "expired" : "scheduled_step_3",
            current_step: stepNumber,
            last_message_sent_at: nowIso,
            next_scheduled_send_at: isLast ? "" : nextAt,
            stop_reason: isLast ? "exhausted" : "",
          });
          recordCrmFlowEvent({
            flowId: flow.id,
            tenantId: tenant.id,
            eventType: "step_sent",
            step: stepNumber,
            messageSent: msg,
            messagePreview: msg.slice(0, 200),
          });
          sentTodayCount += 1;
        } catch (innerErr) {
          console.error(`[crm-scheduler] flow ${flow.id} step error:`, innerErr?.message);
        }
      }
    }
  } catch (err) {
    console.error("[crm-scheduler] error:", err?.message);
  }
}

setInterval(runCrmReturnFlowScheduler, 5 * 60 * 1000);
