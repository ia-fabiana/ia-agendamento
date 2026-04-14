import re
from pathlib import Path

p = Path('/opt/ia-agendamento/server/index.mjs')
text = p.read_text(encoding='utf-8')

pattern = r"function isBookingStatusInquiry\(message\) \{[\s\S]*?\n\}\n\nfunction formatBookingItemSummary"
replacement = '''function isBookingStatusInquiry(message) {
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

function formatBookingItemSummary'''

new_text, count = re.subn(pattern, replacement, text, count=1)
if count != 1:
    raise RuntimeError('isBookingStatusInquiry block not found')

p.write_text(new_text, encoding='utf-8')
print('fixed_isBookingStatusInquiry')