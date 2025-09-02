#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Test with different string encodings
print('営業文生成テスト: 日本語文字化けなし')
print('テスト完了: 正常に動作しています')

# Test with unicode escape sequences
print('\u55b6\u696d\u6587\u751f\u6210\u30c6\u30b9\u30c8: \u65e5\u672c\u8a9e\u6587\u5b57\u5316\u3051\u306a\u3057')
print('\u30c6\u30b9\u30c8\u5b8c\u4e86: \u6b63\u5e38\u306b\u52d5\u4f5c\u3057\u3066\u3044\u307e\u3059')

# Test with raw bytes
japanese_bytes = b'\xe5\x96\xb6\xe6\xa5\xad\xe6\x96\x87\xe7\x94\x9f\xe6\x88\x90'
print(f'Bytes decoded: {japanese_bytes.decode("utf-8")}')

# Test with file encoding
import sys
print(f'File encoding: {sys.getfilesystemencoding()}')
print(f'Default encoding: {sys.getdefaultencoding()}')
