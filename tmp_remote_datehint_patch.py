from pathlib import Path

p = Path('/opt/ia-agendamento/server/index.mjs')
text = p.read_text(encoding='utf-8')

text = text.replace(
'''  const testAuthorization = normalizeTestAuthorization(customerContext?.testAuthorization);
  const inferredPreferredTime = extractPreferredTimeFromMessage(message);
  const hasBookingTimeIntent = messageSuggestsBookingTimeIntent(message, dateContext);
''',
'''  const testAuthorization = normalizeTestAuthorization(customerContext?.testAuthorization);
  const inferredPreferredTime = extractPreferredTimeFromMessage(message);
  const explicitDateFromMessage = extractIsoDateFromText(message, dateContext);
  const hasDateOrTimeHint = Boolean(
    explicitDateFromMessage ||
    inferredPreferredTime ||
    /\bdia\s+\d{1,2}\b/.test(normalizedMessageForGate),
  );
  const hasBookingTimeIntent = messageSuggestsBookingTimeIntent(message, dateContext);
'''
)

text = text.replace(
'''  if (
    !pendingConfirmation &&
    hasBookingTimeIntent &&
    !/(escov|manicure|mao|mão|depila|claudia|mari)/.test(normalizedMessageForGate)
  ) {
''',
'''  if (
    !pendingConfirmation &&
    hasDateOrTimeHint &&
    !/(escov|manicure|mao|mão|depila|claudia|mari)/.test(normalizedMessageForGate)
  ) {
'''
)

p.write_text(text, encoding='utf-8')
print('patched_date_time_hint_guard')