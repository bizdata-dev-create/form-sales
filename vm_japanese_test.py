#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import locale
import os
from datetime import datetime

print("=== VM上での日本語テキストテスト ===")
print(f"実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

print("\n=== 環境設定 ===")
print(f"ロケール: {locale.getlocale()}")
print(f"Pythonエンコーディング: {sys.getdefaultencoding()}")
print(f"stdoutエンコーディング: {sys.stdout.encoding}")

print("\n=== 日本語テキストテスト ===")
test_text = "営業文生成テスト: 日本語文字化けなし"
print(f"元のテキスト: {test_text}")
print(f"エンコード: {test_text.encode('utf-8')}")
print(f"デコード: {test_text.encode('utf-8').decode('utf-8')}")

print("\n=== 営業文生成テスト ===")
sales_text = "お客様各位\n\n平素より格別のご高配を賜り、厚く御礼申し上げます。\n\n弊社の新サービスについてご案内申し上げます。"
print(sales_text)

print("\n=== テスト完了 ===")
