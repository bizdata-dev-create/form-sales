#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import locale
import os

print("=== Python Encoding Test on VM ===")
print(f"Python version: {sys.version}")
print(f"Default encoding: {sys.getdefaultencoding()}")
print(f"stdin encoding: {sys.stdin.encoding}")
print(f"stdout encoding: {sys.stdout.encoding}")
print(f"stderr encoding: {sys.stderr.encoding}")

print("\n=== Locale Settings ===")
print(f"getlocale(): {locale.getlocale()}")
print(f"getpreferredencoding(): {locale.getpreferredencoding()}")

print("\n=== Environment Variables ===")
for var in ['LANG', 'LC_ALL', 'LC_CTYPE', 'PYTHONIOENCODING', 'PYTHONUTF8']:
    print(f"{var}: {os.environ.get(var, 'Not set')}")

print("\n=== Test Japanese Output ===")
test_text = "営業文生成テスト: 日本語文字化けなし"
print(f"Original: {test_text}")
print(f"Encoded: {test_text.encode('utf-8')}")
print(f"Decoded: {test_text.encode('utf-8').decode('utf-8')}")

print("\n=== Test English Output ===")
english_text = "Sales copy generation test: No garbled characters"
print(f"English: {english_text}")

print("\n=== Test Complete ===")
