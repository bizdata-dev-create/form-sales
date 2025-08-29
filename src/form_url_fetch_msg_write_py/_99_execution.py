# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.3
#   kernelspec:
#     display_name: base
#     language: python
#     name: python3
# ---

# %%
###================================

# %%
import sys, pathlib

# # 上位階層をたどって 'src' を見つけて sys.path へ追加
here = pathlib.Path.cwd()
for base in (here, *here.parents):
    src = base / 'src'
    if src.exists():
        sys.path.insert(0, str(src.resolve()))
        break

from form_url_fetch_msg_write_py._01_helpers import *

# %%
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import os
# プロジェクト直下の .env を特定して上書き読み込み
project_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".env").exists())
env_file = str(project_root / ".env")
load_dotenv(dotenv_path=env_file, override=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
CSE_ID = os.getenv("CSE_ID", "")
failure_storage_SPREADSHEET_ID = os.getenv("failure_storage_SPREADSHEET_ID", "")
cse_client = make_search_client(GOOGLE_API_KEY, CSE_ID)

# OpenAI クライアントは各関数内で os.getenv("OPENAI_API_KEY", "") を参照

# %% [markdown]
# ### dfの読み込み

# %%
#df の格納を確認
# df = pd.read_csv(r'C:\Users\qingy\Documents\自動フォーム営業事業\対象リスト\グーネットリスト.csv')
csv_path = os.getenv("TARGET_LIST_CSV") or (sys.argv[1] if len(sys.argv) > 1 else r'C:\Users\qingy\Documents\自動フォーム営業事業\対象リスト\グーネットリスト.csv')
df = pd.read_csv(csv_path)
# df = pd.read_excel('/content/グーネットURL検証.xlsx')
df.rename(columns={'会社名': 'company_name'}, inplace=True)
# df

# %%
# df.head()

# %% [markdown]
# # 一括作成

# %% [markdown]
# ##### ログ設定

# %%
# ログ設定
import logging
import warnings

# ログレベルをWARNING以上に設定（DEBUGとINFOを非表示）
logging.getLogger().setLevel(logging.WARNING)

# 特定のライブラリのログを無効化
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('openai').setLevel(logging.WARNING)

# 警告も非表示にする場合
warnings.filterwarnings('ignore')

# %% [markdown]
# ##### 実取得

# %%
#必ず一旦動作確認を行うこと！
start = 1300
duration = 10
cycle = 5

for i in range(cycle):
    start_ = start + i*duration
    end_ = start_ + duration
    print(f"fetching urls from {start_} to {end_}","="*80)
    contact_url_filled_df = fill_urls(df[start_:end_], GOOGLE_API_KEY, CSE_ID)
    contact_url_filled_df = fill_contact_url(contact_url_filled_df)
    export_unknown_contacts_to_gsheet_improved(contact_url_filled_df, failure_storage_SPREADSHEET_ID, "問い合わせURL未取得")

    # === 統計処理 ===
    total_len = len(contact_url_filled_df)
    hp_count = contact_url_filled_df["hp_url"].notna() & (contact_url_filled_df["hp_url"].str.strip() != "")
    hp_count = hp_count.sum()
    contact_count = contact_url_filled_df["contact_url"].notna() & (contact_url_filled_df["contact_url"].str.strip() != "")
    contact_count = contact_count.sum()
    # === 取得率計算 ===
    hp_rate = hp_count / total_len if total_len > 0 else 0
    contact_rate = contact_count / hp_count if hp_count > 0 else 0

    print("元々の長さ:", total_len)
    print("hp_url取得数:", hp_count)
    print("contact_url取得数:", contact_count)
    print("hp取得率:", hp_rate)
    print("contact_url取得率:", contact_rate)

    # === その後に不要行を削除 ===
    contact_url_filled_df = contact_url_filled_df[
        contact_url_filled_df["contact_url"].notna() &
        (contact_url_filled_df["contact_url"].str.strip() != "")
    ]
    print("有効リスト数", len(contact_url_filled_df))

    print("営業文作成中")
    contact_url_filled_df = fill_sales_copy_with_gpt(
        contact_url_filled_df,
        url_col="hp_url",
        out_col="sales_copy",
        model="gpt-5-mini",
        classify_prompt_template=PROMPT_CLASSIFY,
        sales_prompt_template=PROMPT_SALES,
        business_vocab=BUSINESS_TYPE_VOCAB,
        overwrite=True,         # 既存の sales_copy を残したいなら False
        sleep_sec=0.8,          # レート調整
        openai_api_key=None,    # None なら OPENAI_API_KEY 環境変数を利用
    )
    # エクセルファイル保存部分
    processed_dir = (project_root / "form-sales" / "data" / "targets" / "messge_processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    fname = processed_dir / f"list_with_sales_copy_shoki_{datetime.today().strftime('%Y%m%d')}_{start_}_{end_}.xlsx"
    contact_url_filled_df.to_excel(fname, index=False, sheet_name="sales_copy", engine="openpyxl")
    print("saved:", fname)
