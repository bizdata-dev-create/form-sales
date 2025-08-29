import re, pathlib
src = pathlib.Path('notebooks/full_algo.py')
out_dir = pathlib.Path('notebooks/sections')
out_dir.mkdir(parents=True, exist_ok=True)
text = src.read_text(encoding='utf-8')
# Split on Jupytext cell headers; start a new file when a markdown H1 appears
cells = re.split(r"^# %% .*?$", text, flags=re.M)
headers = re.findall(r"^# %% .*?$", text, flags=re.M)
segments = []
current = []
current_title = None
for hdr, body in zip(['']+headers, cells):
    if hdr.startswith('# %% [markdown]') and re.search(r"^# \\# \\s*(.+)$", body, flags=re.M):
        m = re.search(r"^# \\# \\s*(.+)$", body, flags=re.M)
        if current:
            segments.append((current_title, ''.join(current)))
            current = []
        current_title = m.group(1).strip()
    current.append(hdr + body)
if current:
    segments.append((current_title, ''.join(current)))

def slugify(s):
    s = re.sub(r'\s+', '_', s or 'section')
    s = re.sub(r'[^\w_-]+', '', s)
    return s[:40] or 'section'

written = []
for title, content in segments:
    name = slugify(title)
    # comment out non-markdown cells when title suggests tests
    if title and re.search(r'テスト|test', title, re.I):
        lines = []
        for line in content.splitlines(True):
            if line.startswith('# %%') or line.startswith('#'):
                lines.append(line)
            else:
                lines.append('# ' + line)
        content = ''.join(lines)
    path = out_dir / f"{name}.py"
    path.write_text(content, encoding='utf-8')
    written.append(str(path))
print('WROTE', len(written), 'files')

