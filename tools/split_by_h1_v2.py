import re
from pathlib import Path

base = Path('notebooks/form-url-fetching-and-message-writhing')
base.mkdir(parents=True, exist_ok=True)
src = Path('notebooks/tmp_source.py')
text = src.read_text(encoding='utf-8')
lines = text.splitlines(True)

# Split into cell strings by '# %%' boundaries
cells = []
buf = []
for line in lines:
    if line.startswith('# %%'):
        if buf:
            cells.append(''.join(buf))
            buf = []
    buf.append(line)
if buf:
    cells.append(''.join(buf))

h1_re = re.compile(r'^# \#\s*(.+)$', re.M)

sections = []  # list of dict(title, cells)
pre = []
for cell in cells:
    is_md = cell.startswith('# %% [markdown]')
    title = None
    if is_md:
        m = h1_re.search(cell)
        if m:
            title = m.group(1).strip()
    if title:
        if not sections:
            sections.append({'title': title, 'cells': pre + [cell]})
        else:
            sections.append({'title': title, 'cells': [cell]})
    else:
        if sections:
            sections[-1]['cells'].append(cell)
        else:
            pre.append(cell)

# If no sections detected, put everything as single section
if not sections:
    sections = [{'title': 'section', 'cells': cells[:] }]

# Write sections with chained imports
written = []
for idx, sec in enumerate(sections, start=1):
    name = f'section_{idx:02d}.py'
    out = base / name
    content = ''.join(sec['cells'])
    # If title indicates tests, comment out non-markdown lines
    if re.search(r'(テスト|test)', sec['title'] or '', re.I):
        new_lines = []
        for ln in content.splitlines(True):
            if ln.startswith('# %%') or ln.startswith('# '):
                new_lines.append(ln)
            else:
                new_lines.append('# ' + ln)
        content = ''.join(new_lines)
    if idx > 1:
        imports = '\n'.join([f'from .section_{j:02d} import *' for j in range(1, idx)]) + '\n\n'
        content = imports + content
    out.write_text(content, encoding='utf-8')
    written.append(out)

print('WROTE', len(written), 'sections')
for p in written:
    print(p, 'lines=', sum(1 for _ in p.open(encoding='utf-8')))

