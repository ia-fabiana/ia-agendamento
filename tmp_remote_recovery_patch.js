const fs = require("fs");
const path = "/opt/ia-agendamento/server/index.mjs";
let text = fs.readFileSync(path, "utf8");

function replaceOrThrow(from, to, label) {
  if (!text.includes(from)) {
    throw new Error(`pattern not found: ${label}`);
  }
  text = text.replace(from, to);
}

const helperBlock = `function extractIsoDateFromText(text, dateContext = getSaoPauloDateContext()) {
  const raw = toNonEmptyString(text);
  const match = raw.match(/(\d{1,2})\s*\/\s*(\d{1,2})(?:\s*\/\s*(\d{2,4}))?/);
  if (!match) {
    return "";
  }

  const day = Number(match[1]);
  const month = Number(match[2]);
  let year = Number(match[3] || "");

  if (!Number.isInteger(day) || !Number.isInteger(month) || day < 1 || day > 31 || month < 1 || month > 12) {
    return "";
  }

  if (!Number.isInteger(year) || year <= 0) {
    year = Number(String(dateContext?.isoToday || "").slice(0, 4)) || new Date().getFullYear();
  } else if (year < 100) {
    year += 2000;
  }

  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function normalizeRecoveredProfessionalName(value) {
  const normalized = normalizeForMatch(value);
  if (!normalized) {
    return "";
  }
  if (normalized.includes("marinilza") || normalized.includes("mari")) {
    return "Marinilza";
  }
  if (normalized.includes("claudia")) {
    return "Claudia";
  }
  return professionalDisplayName(value);
}

function normalizeRecoveredServiceName(value) {
  const raw = toNonEmptyString(value);
  const normalized = normalizeForMatch(raw);
  if (!normalized) {
    return "";
  }
  if (/\b(mao|mão|manicure)\b/.test(normalized)) {
    return "Mao Tradicional";
  }
  if (normalized.includes("escova")) {
    return "Escova";
  }
  if (normalized.includes("depil")) {
    return "Depilacao";
  }
  return raw;
}

function recoverBookingDraftFromHistory(history = [], dateContext = getSaoPauloDateContext()) {
  if (!Array.isArray(history) || !history.length) {
    return null;
  }

  const recentAssistantMessages = history
    .slice(-12)
    .filter((item) => toNonEmptyString(item?.role).toLowerCase() === "assistant")
    .reverse();

  for (const item of recentAssistantMessages) {
    const contentRaw = toNonEmptyString(item?.content || item?.text || "");
    if (!contentRaw) {
      continue;
    }

    const content = contentRaw
      .replace(/\*/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    const extractedDate = extractIsoDateFromText(contentRaw, dateContext) || dateContext.isoToday;
    const extractedTime = normalizeBookingTime(extractPreferredTimeFromMessage(contentRaw));

    const items = [];
    const addItem = (serviceName, professionalName, timeValue = "") => {
      const service = normalizeRecoveredServiceName(serviceName);
      const professional = normalizeRecoveredProfessionalName(professionalName);
      const time = normalizeBookingTime(timeValue) || extractedTime;
      if (!service || !professional || !time) {
        return;
      }
      const key = `${normalizeForMatch(service)}|${normalizeForMatch(professional)}|${time}|${extractedDate}`;
      if (items.some((entry) => entry.key === key)) {
        return;
      }
      items.push({
        key,
        service,
        professionalName: professional,
        date: extractedDate,
        time,
      });
    };

    const pairPattern = /([A-Za-zÀ-ÿ\/\s]{3,50})\s+com\s+a?\s*([A-Za-zÀ-ÿ]{3,30})\s+as\s+([0-9hH:\s]{2,8})/gi;
    let pairMatch = null;
    while ((pairMatch = pairPattern.exec(content)) !== null) {
      addItem(pairMatch[1], pairMatch[2], pairMatch[3]);
    }

    const servicesLineMatch = content.match(/servicos?:\s*([^\n]+)/i);
    if (servicesLineMatch && extractedTime) {
      const segments = servicesLineMatch[1].split("+");
      for (const segment of segments) {
        const m = segment.match(/([A-Za-zÀ-ÿ\/\s]{3,50})\s*\(\s*([A-Za-zÀ-ÿ]{3,30})\s*\)/i);
        if (m) {
          addItem(m[1], m[2], extractedTime);
        }
      }
    }

    if (items.length) {
      return { items: items.map(({ key, ...rest }) => rest), sourceText: contentRaw };
    }
  }

  return null;
}

function isBookingStatusInquiry(message) {
  const normalized = normalizeForMatch(message);
  if (!normalized) {
    return false;
  }
  return /\b(conseguiu|agendou|agendamento|deu certo|confirmou|e ai|status)\b/.test(normalized);
}

`;

if (!text.includes("function recoverBookingDraftFromHistory(")) {
  replaceOrThrow(
    "function formatBookingItemSummary(item) {",
    helperBlock + "function formatBookingItemSummary(item) {",
    "insert recovery helpers"
  );
}

const oldNoPendingBlock = `  if (
    confirmationIntent === "confirm" &&
    !pendingConfirmation &&
    historyHasRecentBookingConfirmationPrompt(history)
  ) {
    return finalizeChatResponse(
      "Perfeito. Para evitar erro no fechamento, preciso que voce reenvie em uma unica mensagem: servico, data, horario e profissional. Exemplo: Escova com Claudia em 14/04 as 13:00 e Mao Tradicional com Marinilza as 13:00.",
    );
  }
`;

const newNoPendingBlock = `  const recoveredDraft =
    !pendingConfirmation && historyHasRecentBookingConfirmationPrompt(history)
      ? recoverBookingDraftFromHistory(history, dateContext)
      : null;

  if (
    confirmationIntent === "confirm" &&
    !pendingConfirmation &&
    recoveredDraft &&
    Array.isArray(recoveredDraft.items) &&
    recoveredDraft.items.length
  ) {
    const resolvedClientName = toNonEmptyString(customerContext?.name);
    const resolvedClientPhone = normalizePhone(customerContext?.phone);

    if (!resolvedClientName || !resolvedClientPhone) {
      return finalizeChatResponse(
        "Para confirmar agora, preciso do nome e telefone da cliente. Me envie esses dados e eu concluo.",
      );
    }

    const previewItems = [];
    let previewError = null;
    for (const item of recoveredDraft.items) {
      try {
        const preview = await resolveBookingPreviewItem({
          establishmentId,
          service: item.service,
          date: item.date,
          time: item.time,
          professionalName: item.professionalName,
          allowedProfessionalNames,
        });
        previewItems.push(preview);
      } catch (error) {
        previewError = error;
        break;
      }
    }

    if (previewError || !previewItems.length) {
      return finalizeChatResponse(
        "Nao consegui confirmar automaticamente com os dados anteriores. Me envie novamente servico, data, horario e profissional para eu concluir com seguranca.",
      );
    }

    const execution = await executeConfirmedBookings({
      establishmentId,
      tenantCode,
      clientName: resolvedClientName,
      clientPhone: resolvedClientPhone,
      items: previewItems,
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

  if (
    !pendingConfirmation &&
    recoveredDraft &&
    Array.isArray(recoveredDraft.items) &&
    recoveredDraft.items.length &&
    isBookingStatusInquiry(message)
  ) {
    return finalizeChatResponse(
      `Ainda nao tenho confirmacao final registrada.\n\n${buildBookingConfirmationMessage(recoveredDraft.items)}\n\nSe estiver certo, responda "sim" para eu concluir agora.`,
    );
  }

  if (
    !pendingConfirmation &&
    recoveredDraft &&
    Array.isArray(recoveredDraft.items) &&
    recoveredDraft.items.length &&
    hasBookingTimeIntent &&
    !/(escov|manicure|mao|mão|depila|claudia|mari)/.test(normalizedMessageForGate)
  ) {
    const requestedDate =
      relativeDate?.iso ||
      extractIsoDateFromText(message, dateContext) ||
      recoveredDraft.items[0]?.date ||
      "";
    const requestedTime = normalizeBookingTime(inferredPreferredTime) || recoveredDraft.items[0]?.time || "";

    if (requestedDate || requestedTime) {
      const adjustedItems = recoveredDraft.items.map((item) => ({
        ...item,
        date: requestedDate || item.date,
        time: requestedTime || item.time,
      }));
      return finalizeChatResponse(
        `${buildBookingConfirmationMessage(adjustedItems)}\n\nSe estiver certo, responda "sim" para eu concluir agora.`,
      );
    }
  }

  if (
    confirmationIntent === "confirm" &&
    !pendingConfirmation &&
    historyHasRecentBookingConfirmationPrompt(history)
  ) {
    return finalizeChatResponse(
      "Perfeito. Para evitar erro no fechamento, preciso que voce reenvie em uma unica mensagem: servico, data, horario e profissional. Exemplo: Escova com Claudia em 14/04 as 13:00 e Mao Tradicional com Marinilza as 13:00.",
    );
  }
`;

replaceOrThrow(oldNoPendingBlock, newNoPendingBlock, "replace no-pending confirmation flow");

fs.writeFileSync(path, text, "utf8");
console.log("remote_recovery_patch_applied");