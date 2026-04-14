from pathlib import Path

p = Path('/opt/ia-agendamento/server/index.mjs')
lines = p.read_text(encoding='utf-8').splitlines()
out = []
i = 0
fixes = 0
while i < len(lines):
    line = lines[i]
    if 'join("' in line and line.rstrip().endswith('join("') and i + 1 < len(lines):
        nxt = lines[i + 1].strip()
        if nxt.startswith('")'):
            prefix = line.split('join("')[0]
            suffix = nxt[2:]
            out.append(prefix + 'join("\\n")' + suffix)
            i += 2
            fixes += 1
            continue
    out.append(line)
    i += 1
p.write_text('\n'.join(out) + '\n', encoding='utf-8')
print('fixed_multiline_join', fixes)