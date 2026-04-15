import Database from "better-sqlite3";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.join(__dirname, "..");
const defaultDbPath = path.join(projectRoot, "server", "data", "ia_agendamento.sqlite");

function toNonEmptyString(value) {
  const text = String(value || "").trim();
  return text || "";
}

function normalizeTenantCode(value) {
  return toNonEmptyString(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
}

function normalizeTenantIdentifierKind(value) {
  return toNonEmptyString(value)
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
}

function normalizePhone(value) {
  return String(value || "").replace(/\D/g, "");
}

function normalizeTenantIdentifierValue(kind, value) {
  const normalizedKind = normalizeTenantIdentifierKind(kind);
  const rawValue = toNonEmptyString(value);
  if (!rawValue) {
    return "";
  }

  if (normalizedKind === "evolution_number") {
    return normalizePhone(rawValue);
  }

  if (normalizedKind === "domain") {
    return rawValue
      .replace(/^https?:\/\//i, "")
      .split("/")[0]
      .toLowerCase();
  }

  if (normalizedKind === "api_key") {
    return rawValue;
  }

  return rawValue.toLowerCase();
}

function isSupportedTenantIdentifierKind(value) {
  return [
    "evolution_instance",
    "evolution_number",
    "domain",
    "api_key",
    "custom",
  ].includes(normalizeTenantIdentifierKind(value));
}

function normalizeSchedulingProvider(value) {
  const normalized = toNonEmptyString(value)
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  if (!normalized) {
    return "trinks";
  }
  if (normalized === "google" || normalized === "googlecalendar" || normalized === "calendar") {
    return "google_calendar";
  }
  if (normalized === "google_calendar") {
    return "google_calendar";
  }
  if (normalized === "trinks") {
    return "trinks";
  }
  return normalized;
}

function hashTenantPassword(password, salt = "") {
  const effectiveSalt = toNonEmptyString(salt) || crypto.randomBytes(16).toString("hex");
  const digest = crypto.scryptSync(String(password || ""), Buffer.from(effectiveSalt, "hex"), 64).toString("hex");
  return `${effectiveSalt}:${digest}`;
}

function safeJsonStringify(value) {
  try {
    return JSON.stringify(value ?? null);
  } catch {
    return JSON.stringify({});
  }
}

function parseArgs(argv) {
  const args = {
    file: "",
    dryRun: false,
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      args.help = true;
      continue;
    }
    if (token === "--dry-run") {
      args.dryRun = true;
      continue;
    }
    if ((token === "--file" || token === "-f") && argv[index + 1]) {
      args.file = String(argv[index + 1] || "");
      index += 1;
      continue;
    }
  }

  return args;
}

function printHelp() {
  console.log(`
Uso:
  node scripts/upsert-tenant.mjs --file server/tenants/SEU_SALAO/bootstrap.json
  node scripts/upsert-tenant.mjs --file server/tenants/SEU_SALAO/bootstrap.json --dry-run

Formato esperado:
{
  "tenant": {
    "code": "jacques-vsf",
    "name": "Jacques Janine VSF",
    "segment": "salao",
    "active": true,
    "defaultProvider": "trinks",
    "establishmentId": 62261
  },
  "knowledge": { ... },
  "identifiers": [{ "kind": "evolution_instance", "value": "ia-jacques-vsf" }],
  "providerConfigs": {
    "trinks": { "enabled": true, "config": {} },
    "google_calendar": { "enabled": false, "config": {} }
  },
  "users": [
    { "username": "admin.vsf", "displayName": "Admin VSF", "password": "SenhaForte123", "active": true }
  ]
}
`.trim());
}

function loadBootstrapFile(absoluteFilePath) {
  if (!fs.existsSync(absoluteFilePath)) {
    throw new Error(`Arquivo nao encontrado: ${absoluteFilePath}`);
  }
  const raw = fs.readFileSync(absoluteFilePath, "utf-8");
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("JSON invalido: raiz deve ser objeto.");
  }
  return parsed;
}

function ensureSchema(db) {
  db.exec(`
    PRAGMA foreign_keys = ON;

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
  `);
}

function getTenantByCode(db, code) {
  return db.prepare("SELECT * FROM tenants WHERE code = ? LIMIT 1").get(code) || null;
}

function parseTenantInput(raw) {
  const tenant = raw && typeof raw === "object" ? raw : {};
  const name = toNonEmptyString(tenant.name);
  const code = normalizeTenantCode(tenant.code || name);
  if (!name) {
    throw new Error("Campo obrigatorio: tenant.name");
  }
  if (!code) {
    throw new Error("Campo obrigatorio: tenant.code");
  }

  const parsedEstablishmentId = Number(tenant.establishmentId);
  return {
    code,
    name,
    segment: toNonEmptyString(tenant.segment),
    active: tenant.active == null ? true : Boolean(tenant.active),
    defaultProvider: normalizeSchedulingProvider(tenant.defaultProvider || tenant.provider),
    establishmentId: Number.isFinite(parsedEstablishmentId) && parsedEstablishmentId > 0
      ? parsedEstablishmentId
      : null,
  };
}

function upsertTenant(db, payload) {
  const now = new Date().toISOString();
  const current = getTenantByCode(db, payload.code);
  if (!current) {
    db.prepare(`
      INSERT INTO tenants (
        code, name, segment, active, default_provider, establishment_id,
        knowledge_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      payload.code,
      payload.name,
      payload.segment,
      payload.active ? 1 : 0,
      payload.defaultProvider,
      payload.establishmentId,
      safeJsonStringify(payload.knowledge || {}),
      now,
      now,
    );
    return { action: "created", tenant: getTenantByCode(db, payload.code) };
  }

  db.prepare(`
    UPDATE tenants
    SET name = ?,
        segment = ?,
        active = ?,
        default_provider = ?,
        establishment_id = ?,
        knowledge_json = ?,
        updated_at = ?
    WHERE code = ?
  `).run(
    payload.name,
    payload.segment,
    payload.active ? 1 : 0,
    payload.defaultProvider,
    payload.establishmentId,
    safeJsonStringify(payload.knowledge || {}),
    now,
    payload.code,
  );

  return { action: "updated", tenant: getTenantByCode(db, payload.code) };
}

function upsertIdentifiers(db, tenantId, identifiers = []) {
  const now = new Date().toISOString();
  const report = [];

  for (const item of identifiers) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const kind = normalizeTenantIdentifierKind(item.kind);
    const value = toNonEmptyString(item.value);
    const normalizedValue = normalizeTenantIdentifierValue(kind, value);

    if (!isSupportedTenantIdentifierKind(kind)) {
      throw new Error(`Identifier kind nao suportado: ${String(item.kind || "")}`);
    }
    if (!normalizedValue) {
      throw new Error(`Identifier value invalido para kind=${kind}`);
    }

    db.prepare(`
      INSERT INTO tenant_identifiers (tenant_id, kind, value, normalized_value, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(kind, normalized_value) DO UPDATE SET
        tenant_id = excluded.tenant_id,
        value = excluded.value,
        updated_at = excluded.updated_at
    `).run(tenantId, kind, value, normalizedValue, now, now);

    report.push({ kind, value, normalizedValue });
  }

  return report;
}

function normalizeProviderConfigs(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return [];
  }
  return Object.entries(raw).map(([providerName, providerPayload]) => {
    const asObject = providerPayload && typeof providerPayload === "object" && !Array.isArray(providerPayload)
      ? providerPayload
      : {};
    return {
      provider: normalizeSchedulingProvider(providerName),
      enabled: asObject.enabled == null ? true : Boolean(asObject.enabled),
      config: asObject.config && typeof asObject.config === "object" && !Array.isArray(asObject.config)
        ? asObject.config
        : (asObject || {}),
    };
  });
}

function upsertProviderConfigs(db, tenantId, providerConfigs = []) {
  const now = new Date().toISOString();
  const report = [];

  for (const entry of providerConfigs) {
    const provider = normalizeSchedulingProvider(entry.provider);
    if (!provider) {
      continue;
    }

    db.prepare(`
      INSERT INTO tenant_provider_configs (tenant_id, provider, enabled, config_json, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(tenant_id, provider) DO UPDATE SET
        enabled = excluded.enabled,
        config_json = excluded.config_json,
        updated_at = excluded.updated_at
    `).run(
      tenantId,
      provider,
      entry.enabled ? 1 : 0,
      safeJsonStringify(entry.config || {}),
      now,
      now,
    );

    report.push({ provider, enabled: Boolean(entry.enabled) });
  }

  return report;
}

function normalizeUsername(value) {
  return toNonEmptyString(value)
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, "")
    .slice(0, 64);
}

function upsertUsers(db, tenantId, users = []) {
  const now = new Date().toISOString();
  const report = [];

  for (const user of users) {
    if (!user || typeof user !== "object") {
      continue;
    }
    const username = normalizeUsername(user.username);
    const displayName = toNonEmptyString(user.displayName || user.name);
    const password = String(user.password || "");
    const active = user.active == null ? true : Boolean(user.active);
    if (!username) {
      throw new Error("Usuario invalido: username obrigatorio.");
    }

    const existing = db.prepare(`
      SELECT id, password_hash AS passwordHash
      FROM tenant_users
      WHERE tenant_id = ? AND username = ?
      LIMIT 1
    `).get(tenantId, username);

    if (!existing) {
      if (!password) {
        throw new Error(`Senha obrigatoria para novo usuario: ${username}`);
      }
      db.prepare(`
        INSERT INTO tenant_users (
          tenant_id, username, display_name, password_hash, active, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        tenantId,
        username,
        displayName,
        hashTenantPassword(password),
        active ? 1 : 0,
        now,
        now,
      );
      report.push({ username, action: "created", active });
      continue;
    }

    db.prepare(`
      UPDATE tenant_users
      SET display_name = ?,
          password_hash = ?,
          active = ?,
          updated_at = ?
      WHERE id = ?
    `).run(
      displayName,
      password ? hashTenantPassword(password) : existing.passwordHash,
      active ? 1 : 0,
      now,
      existing.id,
    );
    report.push({ username, action: "updated", active, passwordUpdated: Boolean(password) });
  }

  return report;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.file) {
    printHelp();
    process.exit(args.help ? 0 : 1);
  }

  const absoluteFilePath = path.resolve(projectRoot, args.file);
  const spec = loadBootstrapFile(absoluteFilePath);
  const tenantInput = parseTenantInput(spec.tenant || {});

  const dbPath = path.resolve(process.env.IA_DB_PATH || defaultDbPath);
  const db = new Database(dbPath);
  ensureSchema(db);

  let report = null;
  const transaction = db.transaction(() => {
    const upsertResult = upsertTenant(db, {
      ...tenantInput,
      knowledge: spec.knowledge || {},
    });
    const tenantId = Number(upsertResult.tenant?.id || 0);
    if (!tenantId) {
      throw new Error("Falha ao resolver tenant apos upsert.");
    }

    const identifiers = upsertIdentifiers(db, tenantId, Array.isArray(spec.identifiers) ? spec.identifiers : []);
    const providerConfigs = upsertProviderConfigs(db, tenantId, normalizeProviderConfigs(spec.providerConfigs));
    const users = upsertUsers(db, tenantId, Array.isArray(spec.users) ? spec.users : []);

    report = {
      dbPath,
      tenant: {
        id: tenantId,
        code: tenantInput.code,
        name: tenantInput.name,
        action: upsertResult.action,
      },
      identifiers,
      providerConfigs,
      users,
    };

    if (args.dryRun) {
      throw new Error("__DRY_RUN__");
    }
  });

  try {
    transaction();
  } catch (error) {
    if (!(args.dryRun && error?.message === "__DRY_RUN__")) {
      throw error;
    }
  } finally {
    db.close();
  }

  if (!report) {
    throw new Error("Nenhum resultado produzido.");
  }

  if (args.dryRun) {
    console.log("[DRY-RUN] Nenhuma alteracao persistida.");
  } else {
    console.log("[OK] Tenant processado com sucesso.");
  }
  console.log(JSON.stringify(report, null, 2));
}

main();
