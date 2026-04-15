const Database = require('better-sqlite3');
const crypto = require('crypto');
const db = new Database('/opt/ia-agendamento/server/data/ia_agendamento.sqlite');

function hashTenantPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const digest = crypto.scryptSync(String(password || ''), Buffer.from(salt, 'hex'), 64).toString('hex');
  return `${salt}:${digest}`;
}

const now = new Date().toISOString();
const tenantCode = 'essencia';
const tenantName = 'Essencia Instituto de Beleza';
const establishmentId = 62217;
const username = 'admin.essencia';
const displayName = 'Admin Essencia';
const password = 'S$LgqJmcxBTgmcR%sG';
const knowledge = {
  identity: {
    brandName: 'Essencia Instituto de Beleza',
    assistantName: 'Rebeka',
    toneGuide: 'acolhedor, transparente, objetivo, grato',
    toneOptions: ['acolhedor', 'objetivo'],
    toneCustom: 'transparente, grato',
  },
  business: {
    address: 'R. Dr. Paulo Ferraz da Costa Aguiar, 1603 - Vila Sao Francisco',
    phone: '551136823002',
    openingHours: 'Atendimento com agenda ativa para Claudia e Mari (consulte horarios no momento do pedido)',
    paymentMethods: ['PIX', 'Cartao de credito', 'Cartao de debito'],
    allowedProfessionals: ['Claudia', 'Mari'],
  },
  policies: {
    latePolicy: 'Atendimento com agenda reduzida. Recomendamos pontualidade para preservar os horarios.',
    cancellationPolicy: 'Cancelamentos e remarcacoes com antecedencia ajudam a reorganizar a agenda da Claudia e da Mari.',
    noShowPolicy: 'Faltas sem aviso podem impactar novos encaixes na agenda atual.',
  },
};

const existingTenant = db.prepare('SELECT id FROM tenants WHERE code = ?').get(tenantCode);
let tenantId = existingTenant?.id ? Number(existingTenant.id) : 0;

if (!tenantId) {
  tenantId = Number(db.prepare(`INSERT INTO tenants (
    code, name, segment, active, default_provider, establishment_id, knowledge_json, created_at, updated_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
    tenantCode,
    tenantName,
    'salao',
    1,
    'trinks',
    establishmentId,
    JSON.stringify(knowledge),
    now,
    now,
  ).lastInsertRowid);
  console.log('created tenant', tenantId);
} else {
  db.prepare(`UPDATE tenants
    SET name = ?, active = 1, default_provider = ?, establishment_id = ?, knowledge_json = ?, updated_at = ?
    WHERE id = ?`).run(
    tenantName,
    'trinks',
    establishmentId,
    JSON.stringify(knowledge),
    now,
    tenantId,
  );
  console.log('updated tenant', tenantId);
}

const currentIdentifiers = db.prepare('SELECT id, kind, normalized_value AS normalizedValue FROM tenant_identifiers WHERE tenant_id = ?').all(tenantId);
const ensureIdentifier = (kind, value, normalizedValue) => {
  const existing = currentIdentifiers.find((item) => item.kind === kind && item.normalizedValue === normalizedValue);
  if (existing?.id) {
    db.prepare('UPDATE tenant_identifiers SET value = ?, updated_at = ? WHERE id = ?').run(value, now, Number(existing.id));
    return;
  }
  db.prepare('INSERT INTO tenant_identifiers (tenant_id, kind, value, normalized_value, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)')
    .run(tenantId, kind, value, normalizedValue, now, now);
};
ensureIdentifier('evolution_instance', 'essencia', 'essencia');
ensureIdentifier('evolution_number', '5511989513465', '5511989513465');

const currentProviders = db.prepare('SELECT id, provider FROM tenant_provider_configs WHERE tenant_id = ?').all(tenantId);
const ensureProvider = (provider, enabled, config) => {
  const existing = currentProviders.find((item) => item.provider === provider);
  if (existing?.id) {
    db.prepare('UPDATE tenant_provider_configs SET enabled = ?, config_json = ?, updated_at = ? WHERE id = ?')
      .run(enabled ? 1 : 0, JSON.stringify(config || {}), now, Number(existing.id));
    return;
  }
  db.prepare('INSERT INTO tenant_provider_configs (tenant_id, provider, enabled, config_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)')
    .run(tenantId, provider, enabled ? 1 : 0, JSON.stringify(config || {}), now, now);
};
ensureProvider('trinks', true, {});
ensureProvider('google_calendar', false, {});

const existingUser = db.prepare('SELECT id FROM tenant_users WHERE tenant_id = ? AND username = ?').get(tenantId, username);
const passwordHash = hashTenantPassword(password);
if (!existingUser?.id) {
  db.prepare(`INSERT INTO tenant_users (tenant_id, username, display_name, password_hash, active, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)`).run(tenantId, username, displayName, passwordHash, 1, now, now);
  console.log('created user', username);
} else {
  db.prepare('UPDATE tenant_users SET display_name = ?, password_hash = ?, active = 1, updated_at = ? WHERE id = ?')
    .run(displayName, passwordHash, now, Number(existingUser.id));
  console.log('updated user', username);
}

const existingCrm = db.prepare('SELECT tenant_id FROM tenant_crm_settings WHERE tenant_id = ?').get(tenantId);
const crmSettings = {
  crmReturnEnabled: false,
  crmMode: 'beta',
  bookingMaxDaysAhead: 60,
  messageSendingWindowStart: '09:00',
  messageSendingWindowEnd: '19:00',
  messageDailyLimit: 20,
  stopFlowOnAnyFutureBooking: true,
  maxSteps: 3,
  humanHandoffEnabled: true,
};
if (!existingCrm) {
  db.prepare('INSERT INTO tenant_crm_settings (tenant_id, config_json, created_at, updated_at) VALUES (?, ?, ?, ?)')
    .run(tenantId, JSON.stringify(crmSettings), now, now);
  console.log('created crm settings');
}

const summary = db.prepare(`SELECT t.id, t.code, t.name, t.establishment_id AS establishmentId, t.active,
  u.username
  FROM tenants t
  LEFT JOIN tenant_users u ON u.tenant_id = t.id
  WHERE t.code = ?`).all(tenantCode);
console.log(JSON.stringify(summary, null, 2));