from pathlib import Path

p = Path('/opt/ia-agendamento/server/index.mjs')
text = p.read_text(encoding='utf-8')

text = text.replace(
'''function isBookingStatusInquiry(message) {
  const normalized = normalizeForMatch(message);
  if (!normalized) {
    return false;
  }
  return /\\b(conseguiu|agendou|agendamento|deu certo|confirmou|e ai|status)\\b/.test(normalized);
}
''',
'''function isBookingStatusInquiry(message) {
  const normalized = normalizeForMatch(message);
  if (!normalized) {
    return false;
  }

  return (
    normalized.includes("conseg") ||
    normalized.includes("agend") ||
    normalized.includes("confirm") ||
    normalized.includes("deu certo") ||
    normalized.includes("status") ||
    normalized.includes("e ai")
  );
}
'''
)

text = text.replace(
'''  if (
    !pendingConfirmation &&
    !recoveredDraft &&
    isBookingStatusInquiry(message) &&
    historyHasRecentBookingConfirmationPrompt(history)
  ) {
    return finalizeChatResponse(
      "Ainda nao tenho confirmacao final registrada. Para concluir sem erro, me envie em uma unica mensagem: servico, data, horario e profissional.",
    );
  }
''',
'''  if (
    !pendingConfirmation &&
    !recoveredDraft &&
    isBookingStatusInquiry(message)
  ) {
    return finalizeChatResponse(
      "Ainda nao tenho confirmacao final registrada. Para concluir sem erro, me envie em uma unica mensagem: servico, data, horario e profissional.",
    );
  }
'''
)

p.write_text(text, encoding='utf-8')
print('patched_status_guard_hardening')