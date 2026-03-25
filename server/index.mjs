import express from "express";
import dotenv from "dotenv";
import { GoogleGenAI, Type } from "@google/genai";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const envPath = path.join(__dirname, "..", ".env");
dotenv.config({ path: envPath });

const app = express();
const port = Number(process.env.PORT || 3001);
const KNOWLEDGE_FILE_PATH = path.join(__dirname, "salonKnowledge.json");
const MAX_WHATSAPP_HISTORY_MESSAGES = 20;
const whatsappConversations = new Map();

app.use(express.json());
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

function getWhatsappHistory(phone) {
  if (!phone) {
    return [];
  }

  const current = Array.isArray(whatsappConversations.get(phone))
    ? whatsappConversations.get(phone)
    : [];

  return current.map(normalizeWhatsappMessage).filter(Boolean);
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
}

function summarizeWhatsappConversations() {
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

function normalizeTrinksPhone(phone) {
  if (!phone || typeof phone !== "object") {
    return normalizePhone(phone);
  }

  return normalizePhone(`${phone?.ddi || ""}${phone?.ddd || ""}${phone?.telefone || ""}${phone?.numero || ""}`);
}

// Decompõe um telefone brasileiro em { ddi, ddd, numero } para criação de clientes na Trinks
function parseBrazilianPhone(phone) {
  const digits = normalizePhone(phone);
  if (!digits) return null;

  // Remove DDI 55 se presente no início (55 + 10 ou 11 dígitos = 12 ou 13 dígitos)
  let local = digits;
  if (local.length >= 12 && local.startsWith("55")) {
    local = local.slice(2);
  }

  // Extrai DDD (2 dígitos) + número (8 ou 9 dígitos)
  if (local.length >= 10) {
    const ddd = local.slice(0, 2);
    const numero = local.slice(2);
    return { ddi: "55", ddd, numero };
  }

  // Sem DDD reconhecível — retorna só o número
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

const SYSTEM_INSTRUCTION = `Você é a IA.AGENDAMENTO, uma Concierge Digital de altíssimo padrão desenvolvida para o salão de luxo da Fabiana. Sua missão é gerenciar agendamentos via WhatsApp com elegância, precisão e minimalismo.

Diretrizes de Personalidade:
Identidade: Apresente-se como IA.AGENDAMENTO sempre que necessário.
Tom de Voz: Extremamente sofisticado, polido e acolhedor. Use frases curtas e diretas. Evite gírias e excesso de emojis; prefira ✨, 📅 ou 🥂.
Estética: Seu atendimento deve refletir um ambiente de salão high-end e minimalista.

Fluxo de Atendimento:
Reconhecimento: Se for uma cliente nova, dê as boas-vindas ao universo de beleza da Fabiana. Se for recorrente, utilize o histórico para ser mais pessoal.
Consultoria: Identifique o serviço desejado (ex: mechas, corte, tratamento).
Agendamento (Trinks): Quando a cliente solicitar um horário, informe que você verificará a disponibilidade em tempo real na agenda oficial.
Regra de agenda: Antes de sugerir ou confirmar horarios, consulte os agendamentos do dia (listAppointmentsForDate) e evite horarios ocupados.
Reagendamento: Para alterar horário, solicite código de confirmação (TRK-123) ou ID do agendamento.
Fechamento: Após a confirmação do horário, solicite o nome completo e telefone para finalizar a reserva no sistema Trinks.

Restrições:
Nunca invente horários; sempre diga que está consultando o sistema.
Não escreva parágrafos longos; o atendimento de luxo é ágil e eficiente.
Mantenha o foco total em converter a conversa em um agendamento finalizado.`;

const chatTools = [
  {
    name: "checkAvailability",
    parameters: {
      type: Type.OBJECT,
      description: "Verifica a disponibilidade de horários para um serviço específico em uma data.",
      properties: {
        service: {
          type: Type.STRING,
          description: "O nome do serviço (ex: corte, mechas, manicure).",
        },
        date: {
          type: Type.STRING,
          description: "A data desejada (formato YYYY-MM-DD).",
        },
      },
      required: ["service", "date"],
    },
  },
  {
    name: "listAppointmentsForDate",
    parameters: {
      type: Type.OBJECT,
      description: "Lista os horarios ja ocupados (agendamentos) em uma data.",
      properties: {
        date: {
          type: Type.STRING,
          description: "A data desejada (formato YYYY-MM-DD).",
        },
      },
      required: ["date"],
    },
  },
  {
    name: "bookAppointment",
    parameters: {
      type: Type.OBJECT,
      description: "Finaliza a reserva de um horário no sistema.",
      properties: {
        service: { type: Type.STRING },
        date: { type: Type.STRING },
        time: { type: Type.STRING, description: "Horário escolhido (ex: 14:00)." },
        professionalName: {
          type: Type.STRING,
          description: "Nome da profissional desejada (opcional).",
        },
        clientName: { type: Type.STRING },
        clientPhone: { type: Type.STRING },
      },
      required: ["service", "date", "time", "clientName", "clientPhone"],
    },
  },
  {
    name: "rescheduleAppointment",
    parameters: {
      type: Type.OBJECT,
      description: "Altera a data e horário de um agendamento existente.",
      properties: {
        confirmationCode: {
          type: Type.STRING,
          description: "Código de confirmação como TRK-123 (opcional se appointmentId for informado).",
        },
        appointmentId: {
          type: Type.STRING,
          description: "ID do agendamento (opcional se confirmationCode for informado).",
        },
        date: { type: Type.STRING, description: "Nova data no formato YYYY-MM-DD." },
        time: { type: Type.STRING, description: "Novo horário no formato HH:mm." },
      },
      required: ["date", "time"],
    },
  },
];

function buildConversationPrompt(history, message, knowledge) {
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

  const response = await fetch(url, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await response.text();
  const json = text ? safeJsonParse(text) : null;

  if (!response.ok) {
    const message = json?.message || json?.mensagem || text || `Erro ${response.status}`;
    const error = new Error(`Trinks: ${message}`);
    error.status = response.status;
    error.details = json || text;
    throw error;
  }

  return json;
}

async function evolutionRequest(path, { method = "POST", body } = {}) {
  const baseUrl = ensureEnv("EVOLUTION_API_BASE_URL").replace(/\/$/, "");
  const apiKey = ensureEnv("EVOLUTION_API_KEY");

  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      apikey: apiKey,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

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

function extractIncomingWhatsapp(body) {
  const data = body?.data && typeof body.data === "object" ? body.data : body;
  const key = data?.key && typeof data.key === "object" ? data.key : body?.key || {};
  const message = data?.message && typeof data.message === "object" ? data.message : body?.message || {};

  const senderRaw = firstNonEmpty([
    key?.remoteJid,
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
    data?.text,
    data?.body,
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

async function findServiceByName(estabelecimentoId, serviceName) {
  const payload = await trinksRequest("/servicos", {
    method: "GET",
    estabelecimentoId,
    query: {
      nome: serviceName,
      page: 1,
      pageSize: 50,
    },
  });

  const items = extractItems(payload);
  const normalized = String(serviceName).toLowerCase().trim();
  const exact = items.find((item) => String(item?.nome || "").toLowerCase().trim() === normalized);
  return exact || items[0] || null;
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

  return collectProfessionalsFromPayload(payload);
}

async function findProfessionalForBooking({ establishmentId, date, professionalName, serviceId }) {
  const professionals = await getProfessionals({ establishmentId, date, serviceId });
  if (!professionals.length) {
    const error = new Error("Nenhuma profissional disponivel para a data informada.");
    error.status = 422;
    throw error;
  }

  if (!professionalName) {
    return professionals[0];
  }

  const normalized = String(professionalName).toLowerCase().trim();
  const exact = professionals.find((item) => item.name.toLowerCase().trim() === normalized);
  if (exact) {
    return exact;
  }

  const partial = professionals.find((item) => item.name.toLowerCase().includes(normalized));
  if (partial) {
    return partial;
  }

  const error = new Error(`Profissional nao encontrada para: ${professionalName}`);
  error.status = 404;
  throw error;
}

async function findOrCreateClient(estabelecimentoId, clientName, clientPhone) {
  const searches = [];
  const normalizedPhone = normalizePhone(clientPhone);

  if (normalizedPhone) {
    searches.push(listClients(estabelecimentoId, { telefone: normalizedPhone }));
    searches.push(listClients(estabelecimentoId, { phone: normalizedPhone }));
    searches.push(listClients(estabelecimentoId, { celular: normalizedPhone }));
  }

  if (clientName) {
    searches.push(listClients(estabelecimentoId, { nome: clientName }));
  }

  const searchResults = await Promise.allSettled(searches);
  const candidates = dedupeClients(
    searchResults
      .filter((result) => result.status === "fulfilled")
      .flatMap((result) => result.value),
  );

  const existingByPhoneAndName = candidates.find(
    (item) => matchesClientPhone(item, clientPhone) && matchesClientName(item, clientName),
  );
  const existingByPhone = candidates.find((item) => matchesClientPhone(item, clientPhone));
  const existingByName = candidates.find((item) => matchesClientName(item, clientName));
  const existing = existingByPhoneAndName || existingByPhone || existingByName;

  if (existing) {
    return existing;
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
  return created;
}

async function getAvailability(establishmentId, service, date) {
  const foundService = await findServiceByName(establishmentId, service);
  if (!foundService) {
    return {
      availableTimes: [],
      occupiedTimes: [],
      message: `Servico nao encontrado para: ${service}`,
    };
  }

  let availableTimes = [];

  try {
    const professionalsPayload = await trinksRequest(`/agendamentos/profissionais/${date}`, {
      method: "GET",
      estabelecimentoId: establishmentId,
    });

    const all = JSON.stringify(professionalsPayload);
    const matches = all.match(/\b([01]\d|2[0-3]):[0-5]\d\b/g) || [];
    availableTimes = [...new Set(matches)].sort();
  } catch {
    // If the date/professional endpoint is unavailable for this account, keep fallback below.
  }

  if (!availableTimes.length) {
    availableTimes = ["09:00", "10:00", "11:00", "14:00", "15:00", "16:00"];
  }

  let occupiedTimes = [];
  try {
    const response = await getAppointmentsForDate(establishmentId, date);
    const normalized = response.items.map(normalizeAppointmentItem).filter(Boolean);
    const times = normalized.map((item) => item.time).filter(Boolean);
    occupiedTimes = [...new Set(times)].sort();
    if (occupiedTimes.length) {
      const occupiedSet = new Set(occupiedTimes);
      availableTimes = availableTimes.filter((time) => !occupiedSet.has(time));
    }
  } catch {
    // If appointments query fails, keep availability as-is.
  }

  return {
    availableTimes,
    occupiedTimes,
    serviceId: serviceIdFrom(foundService),
    message: `Horarios consultados para ${service} em ${date}`,
  };
}

async function createAppointment({
  establishmentId,
  service,
  date,
  time,
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
  if (!serviceId) {
    const error = new Error("Servico sem ID valido no retorno da API Trinks.");
    error.status = 422;
    throw error;
  }

  const client = await findOrCreateClient(establishmentId, clientName, clientPhone);
  const clientId = clientIdFrom(client);

  const professional = await findProfessionalForBooking({
    establishmentId,
    date,
    professionalName,
    serviceId,
  });

  if (!clientId) {
    const error = new Error("Cliente sem ID valido no retorno da API Trinks.");
    error.status = 422;
    throw error;
  }

  const duration = Number(
    foundService?.duracaoEmMinutos || foundService?.duracao || foundService?.duracaoMinutos || 60,
  );
  const amount = Number(foundService?.valor || foundService?.preco || 0);

  const payload = {
    servicoId: Number(serviceId),
    clienteId: Number(clientId),
    profissionalId: Number(professional.id),
    dataHoraInicio: toIsoDateTime(date, time),
    duracaoEmMinutos: Number.isFinite(duration) ? duration : 60,
    valor: Number.isFinite(amount) ? amount : 0,
    observacoes: "Agendamento criado via IA.AGENDAMENTO",
    confirmado: true,
  };

  let created;
  try {
    created = await trinksRequest("/agendamentos", {
      method: "POST",
      estabelecimentoId: establishmentId,
      body: payload,
    });
  } catch (error) {
    error.details = {
      ...(typeof error.details === "object" && error.details ? error.details : {}),
      requestPayload: payload,
      resolved: {
        establishmentId: Number(establishmentId),
        serviceId: Number(serviceId),
        clientId: Number(clientId),
        professionalId: Number(professional.id),
        professionalName: professional.name,
      },
    };
    throw error;
  }

  const appointmentId =
    created?.id || created?.agendamentoId || created?.data?.id || created?.item?.id || null;

  return {
    status: "success",
    confirmationCode: appointmentId ? `TRK-${appointmentId}` : "TRK-PENDENTE",
    message: `Agendamento enviado ao Trinks com sucesso com ${professional.name}.`,
    professional,
    raw: created,
  };
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
        observacoes: "Agendamento criado via IA.AGENDAMENTO",
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
  const directId = String(appointmentId || "").trim();
  if (/^\d+$/.test(directId)) {
    return Number(directId);
  }

  const fromCode = String(confirmationCode || "").trim().match(/(\d+)/);
  if (fromCode?.[1]) {
    return Number(fromCode[1]);
  }

  const error = new Error("Informe um codigo TRK valido ou ID numerico do agendamento.");
  error.status = 400;
  throw error;
}

async function rescheduleAppointment({ establishmentId, confirmationCode, appointmentId, date, time }) {
  const parsedId = parseAppointmentId({ confirmationCode, appointmentId });
  const dataHoraInicio = toIsoDateTime(date, time);
  const payload = { dataHoraInicio };

  try {
    const updated = await trinksRequest(`/agendamentos/${parsedId}`, {
      method: "PATCH",
      estabelecimentoId: establishmentId,
      body: payload,
    });

    return {
      status: "success",
      confirmationCode: `TRK-${parsedId}`,
      message: `Horario alterado para ${date} as ${time}.`,
      raw: updated,
    };
  } catch (patchError) {
    const updated = await trinksRequest(`/agendamentos/${parsedId}`, {
      method: "PUT",
      estabelecimentoId: establishmentId,
      body: payload,
    });

    return {
      status: "success",
      confirmationCode: `TRK-${parsedId}`,
      message: `Horario alterado para ${date} as ${time}.`,
      raw: updated,
      fallbackMethod: "PUT",
      patchError: patchError?.message || null,
    };
  }
}

async function sendChatMessage({ establishmentId, message, history }) {
  const knowledge = loadSalonKnowledge();
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
    message: buildConversationPrompt(history, message, knowledge),
  });

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const calls = response.functionCalls || [];
    if (!calls.length) {
      return response.text;
    }

    const results = [];
    for (const call of calls) {
      if (call.name === "checkAvailability") {
        const availability = await getAvailability(
          establishmentId,
          String(call.args.service),
          String(call.args.date),
        );
        results.push({ name: call.name, result: availability });
        continue;
      }

      if (call.name === "listAppointmentsForDate") {
        const { items, source } = await getAppointmentsForDate(
          establishmentId,
          String(call.args.date),
        );
        const normalized = items.map(normalizeAppointmentItem).filter(Boolean);
        results.push({ name: call.name, result: { source, appointments: normalized } });
        continue;
      }

      if (call.name === "bookAppointment") {
        const booking = await createAppointment({
          establishmentId,
          service: String(call.args.service),
          date: String(call.args.date),
          time: String(call.args.time),
          professionalName: String(call.args.professionalName || ""),
          clientName: String(call.args.clientName),
          clientPhone: String(call.args.clientPhone),
        });
        results.push({ name: call.name, result: booking });
        continue;
      }

      if (call.name === "rescheduleAppointment") {
        const rescheduled = await rescheduleAppointment({
          establishmentId,
          confirmationCode: String(call.args.confirmationCode || ""),
          appointmentId: String(call.args.appointmentId || ""),
          date: String(call.args.date),
          time: String(call.args.time),
        });
        results.push({ name: call.name, result: rescheduled });
        continue;
      }

      results.push({
        name: call.name,
        result: { status: "error", message: `Ferramenta nao suportada: ${call.name}` },
      });
    }

    response = await chat.sendMessage({
      message: JSON.stringify(results),
    });
  }

  throw new Error("Nao foi possivel concluir a conversa com a IA.");
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
      evolutionSendText: "POST /api/evolution/send-text",
      evolutionQrPage: "GET /api/evolution/instance/connect?instance=SEU_NOME",
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
    const { establishmentId, service, date } = req.body || {};
    if (!establishmentId || !service || !date) {
      return res.status(400).json({ message: "Campos obrigatorios: establishmentId, service, date" });
    }

    const availability = await getAvailability(establishmentId, service, date);

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

    return res.json({ professionals });
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
    const { establishmentId, message, history } = req.body || {};

    if (!establishmentId || !message) {
      return res.status(400).json({
        message: "Campos obrigatorios: establishmentId, message",
      });
    }

    const text = await sendChatMessage({
      establishmentId: Number(establishmentId),
      message: String(message),
      history: Array.isArray(history) ? history : [],
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
    const instance = ensureEnv("EVOLUTION_INSTANCE");
    const { to, text } = req.body || {};

    if (!to || !text) {
      return res.status(400).json({ message: "Campos obrigatorios: to, text" });
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

app.post("/webhook/whatsapp", async (req, res) => {
  try {
    const incoming = extractIncomingWhatsapp(req.body || {});

    if (incoming.fromMe) {
      return res.status(200).json({ received: true, ignored: true, reason: "fromMe" });
    }

    if (incoming.isGroup) {
      return res.status(200).json({ received: true, ignored: true, reason: "groupMessage" });
    }

    if (!incoming.senderNumber || !incoming.messageText) {
      return res.status(200).json({
        received: true,
        ignored: true,
        reason: "missingSenderOrText",
        event: incoming.event || null,
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

    const previousHistory = getWhatsappHistory(incoming.senderNumber);
    pushWhatsappHistory(incoming.senderNumber, "user", incoming.messageText, incoming.senderName);

    const answer = await sendChatMessage({
      establishmentId,
      message: incoming.messageText,
      history: previousHistory,
    });

    const instance = resolveEvolutionInstance(incoming.instanceName);
    if (!instance) {
      const instanceError = new Error("EVOLUTION_INSTANCE nao configurado e nao informado no webhook.");
      instanceError.status = 500;
      throw instanceError;
    }

    await evolutionRequest(`/message/sendText/${instance}`, {
      method: "POST",
      body: {
        number: incoming.senderNumber,
        text: answer,
      },
    });

    pushWhatsappHistory(incoming.senderNumber, "assistant", answer);

    return res.status(200).json({
      received: true,
      processed: true,
      instance,
      to: incoming.senderNumber,
      event: incoming.event || null,
      messageId: incoming.messageId || null,
    });
  } catch (error) {
    return res.status(error.status || 500).json({
      received: true,
      processed: false,
      status: "error",
      message: error.message || "Erro ao processar webhook do WhatsApp.",
      details: error.details || null,
    });
  }
});

app.listen(port, () => {
  console.log(`Backend online em http://localhost:${port}`);
});
