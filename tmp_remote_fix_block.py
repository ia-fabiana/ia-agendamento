# -*- coding: utf-8 -*-
import re
from pathlib import Path

p = Path('/opt/ia-agendamento/server/index.mjs')
text = p.read_text(encoding='utf-8')

replacement = '''  const recoveredDraft =
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
      return finalizeChatResponse([
        "Perfeito, agendamento confirmado com sucesso:",
        ...successLines,
      ].join("\\n"));
    }

    if (execution.successes.length && execution.failures.length) {
      return finalizeChatResponse([
        "Consegui confirmar parte dos agendamentos.",
        "",
        "Confirmados:",
        successLines.join("\\n"),
        "",
        "Nao confirmados:",
        failureLines.join("\\n"),
      ].join("\\n"));
    }

    return finalizeChatResponse([
      "Nao consegui confirmar os agendamentos solicitados:",
      failureLines.join("\\n"),
    ].join("\\n"));
  }

  if (
    !pendingConfirmation &&
    recoveredDraft &&
    Array.isArray(recoveredDraft.items) &&
    recoveredDraft.items.length &&
    isBookingStatusInquiry(message)
  ) {
    return finalizeChatResponse([
      "Ainda nao tenho confirmacao final registrada.",
      "",
      buildBookingConfirmationMessage(recoveredDraft.items),
      "",
      'Se estiver certo, responda "sim" para eu concluir agora.',
    ].join("\\n"));
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
      return finalizeChatResponse([
        buildBookingConfirmationMessage(adjustedItems),
        "",
        'Se estiver certo, responda "sim" para eu concluir agora.',
      ].join("\\n"));
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

  const shouldAskPreferenceFirst ='''

pattern = r"  const recoveredDraft =[\s\S]*?  const shouldAskPreferenceFirst ="
new_text, count = re.subn(pattern, replacement, text, count=1)
if count != 1:
    raise RuntimeError('target block not found for replacement')

p.write_text(new_text, encoding='utf-8')
print('replaced_recovered_block')