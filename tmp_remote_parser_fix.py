from pathlib import Path

p = Path('/opt/ia-agendamento/server/index.mjs')
text = p.read_text(encoding='utf-8')

text = text.replace(
    'const pairPattern = /([A-Za-zÀ-ÿ\\/\\s]{3,50})\\s+com\\s+a?\\s*([A-Za-zÀ-ÿ]{3,30})\\s+as\\s+([0-9hH:\\s]{2,8})/gi;',
    'const pairPattern = /([A-Za-zÀ-ÿ\\/\\s]{3,50})\\s+com\\s+a?\\s*([A-Za-zÀ-ÿ]{3,30})\\s+(?:as|às)\\s+([0-9hH:\\s]{2,8})/gi;'
)
text = text.replace(
    'const servicesLineMatch = content.match(/servicos?:\\s*([^\\n]+)/i);',
    'const servicesLineMatch = content.match(/servi[cç]os?:\\s*([^\\n]+)/i);'
)

anchor = '''  if (
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

'''
insert = anchor + '''  if (
    !pendingConfirmation &&
    !recoveredDraft &&
    isBookingStatusInquiry(message) &&
    historyHasRecentBookingConfirmationPrompt(history)
  ) {
    return finalizeChatResponse(
      "Ainda nao tenho confirmacao final registrada. Para concluir sem erro, me envie em uma unica mensagem: servico, data, horario e profissional.",
    );
  }

'''

if anchor in text and 'Ainda nao tenho confirmacao final registrada. Para concluir sem erro' not in text:
    text = text.replace(anchor, insert, 1)

p.write_text(text, encoding='utf-8')
print('patched_parser_and_status_guard')