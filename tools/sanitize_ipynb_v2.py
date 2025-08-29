import json, sys
from pathlib import Path

nb_path = Path('notebooks/問い合わせURL取得.ipynb')
nb = json.loads(nb_path.read_text(encoding='utf-8'))
changed = False

replacements = {
    'os.environ["OPENAI_API_KEY"]': 'import os\nos.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY", "")\n',
    'API_KEY': 'API_KEY = os.getenv("GOOGLE_API_KEY", "")\n',
    'GEMNI_API_KEY': 'GEMNI_API_KEY = os.getenv("GEMINI_API_KEY", "")\n',
    'CSE_ID': 'CSE_ID   = os.getenv("CSE_ID", "")\n',
    'failure_storage_SPREADSHEET_ID': 'failure_storage_SPREADSHEET_ID = os.getenv("FAILURE_SPREADSHEET_ID", "")\n',
}

for cell in nb.get('cells', []):
    if cell.get('cell_type') != 'code':
        continue
    src = cell.get('source', [])
    new_src = []
    for line in src:
        s = line.strip()
        if s.startswith('os.environ["OPENAI_API_KEY"]'):
            new_src.append(replacements['os.environ["OPENAI_API_KEY"]'])
            changed = True
            continue
        if s.startswith('API_KEY') and '="' in s:
            new_src.append(replacements['API_KEY'])
            changed = True
            continue
        if s.startswith('GEMNI_API_KEY') and '="' in s:
            new_src.append(replacements['GEMNI_API_KEY'])
            changed = True
            continue
        if s.startswith('CSE_ID') and '="' in s:
            new_src.append(replacements['CSE_ID'])
            changed = True
            continue
        if s.startswith('failure_storage_SPREADSHEET_ID') and '="' in s:
            new_src.append(replacements['failure_storage_SPREADSHEET_ID'])
            changed = True
            continue
        new_src.append(line)
    cell['source'] = new_src

if changed:
    nb_path.write_text(json.dumps(nb, ensure_ascii=False, indent=2), encoding='utf-8')
    print('sanitized:', nb_path)
else:
    print('no changes')

