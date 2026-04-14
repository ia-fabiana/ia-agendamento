from pathlib import Path

p = Path('/opt/ia-agendamento/server/index.mjs')
text = p.read_text(encoding='utf-8')

anchor = '''  if (
    !pendingConfirmation &&
    !recoveredDraft &&
    isBookingStatusInquiry(message)
  ) {
    return finalizeChatResponse(
      "Ainda nao tenho confirmacao final registrada. Para concluir sem erro, me envie em uma unica mensagem: servico, data, horario e profissional.",
    );
  }

'''
insert = anchor + '''  if (
    !pendingConfirmation &&
    hasBookingTimeIntent &&
    !/(escov|manicure|mao|mão|depila|claudia|mari)/.test(normalizedMessageForGate)
  ) {
    return finalizeChatResponse(
      "Entendi data e horario. Para concluir sem erro, me envie em uma unica mensagem: servico e profissional (ex.: Escova com Claudia em 14/04 as 13:00).",
    );
  }

'''

if anchor in text and 'Entendi data e horario. Para concluir sem erro' not in text:
    text = text.replace(anchor, insert, 1)

p.write_text(text, encoding='utf-8')
print('added_datetime_partial_guard')