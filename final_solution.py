#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import locale
import os
from datetime import datetime

# Create log file
log_file = "japanese_test_log.txt"
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

with open(log_file, "w", encoding="utf-8") as f:
    f.write(f"=== Japanese Text Test Log ===\n")
    f.write(f"Timestamp: {timestamp}\n\n")
    
    f.write("=== Python Environment ===\n")
    f.write(f"Python version: {sys.version}\n")
    f.write(f"Default encoding: {sys.getdefaultencoding()}\n")
    f.write(f"stdout encoding: {sys.stdout.encoding}\n")
    
    f.write("\n=== Locale Settings ===\n")
    f.write(f"getlocale(): {locale.getlocale()}\n")
    f.write(f"getpreferredencoding(): {locale.getpreferredencoding()}\n")
    
    f.write("\n=== Environment Variables ===\n")
    for var in ['LANG', 'LC_ALL', 'LC_CTYPE', 'PYTHONIOENCODING', 'PYTHONUTF8']:
        f.write(f"{var}: {os.environ.get(var, 'Not set')}\n")
    
    f.write("\n=== Japanese Text Test ===\n")
    test_text = "営業文生成テスト: 日本語文字化けなし"
    f.write(f"Original: {test_text}\n")
    f.write(f"Encoded: {test_text.encode('utf-8')}\n")
    f.write(f"Decoded: {test_text.encode('utf-8').decode('utf-8')}\n")
    
    f.write("\n=== English Text Test ===\n")
    english_text = "Sales copy generation test: No garbled characters"
    f.write(f"English: {english_text}\n")
    
    f.write("\n=== Test Complete ===\n")

print(f"Log file created: {log_file}")
print("Content:")
with open(log_file, "r", encoding="utf-8") as f:
    print(f.read())
