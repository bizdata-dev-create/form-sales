import json, sys
from pathlib import Path

replacements = {
    'os.environ["OPENAI_API_KEY"]': 'os.getenv("OPENAI_API_KEY", "")',
}

TARGETS = [
    ('os.environ["OPENAI_API_KEY"]', 'os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY", "")\n'),
]

p = Path(sys.argv[1])
nb = json.loads(p.read_text(encoding='utf-8'))
changed = False
for cell in nb.get('cells', []):
    if cell.get('cell_type') != 'code':
        continue
    src_lines = cell.get('source', [])
    new_lines = []
    modified_cell = False
    for line in src_lines:
        orig = line
        if 'OPENAI_API_KEY' in line and 'os.environ' in line:
            line = 'import os\n' if 'import os' in ''.join(src_lines) else line
            # Force assignment to env
        new_lines.append(line)
    # Replace specific literal assignments
    joined = ''.join(new_lines)
    if 'os.environ["OPENAI_API_KEY"]' in joined and '="' in joined:
        joined = joined.replace('\n', '\n')
        joined = joined.replace('os.environ["OPENAI_API_KEY"] = ', 'os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY", "") # ')
    # Generic literals
    joined = joined.replace('API_KEY = "', 'API_KEY = os.getenv("GOOGLE_API_KEY", "") # ')
    joined = joined.replace('GEMNI_API_KEY = "', 'GEMNI_API_KEY = os.getenv("GEMINI_API_KEY", "") # ')
    joined = joined.replace('CSE_ID   = "', 'CSE_ID   = os.getenv("CSE_ID", "") # ')
    joined = joined.replace('failure_storage_SPREADSHEET_ID = "', 'failure_storage_SPREADSHEET_ID = os.getenv("FAILURE_SPREADSHEET_ID", "") # ')
    if joined != ''.join(src_lines):
        cell['source'] = [joined]
        changed = True

if changed:
    p.write_text(json.dumps(nb, ensure_ascii=False, indent=2), encoding='utf-8')
    print('sanitized:', p)
else:
    print('no changes:', p)

