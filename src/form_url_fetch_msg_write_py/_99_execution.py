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
# # %load_ext autoreload
# # %autoreload 2
# from form_url_fetch_msg_write_py._01_helpers import *

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
BUSINESS_TYPE_VOCAB = [
    "車検工場","整備工場","板金塗装","中古車販売店",
    "パーソナルジム","フィットネスジム","美容室","理容室","エステサロン",
    "整体院","鍼灸院","歯科クリニック",
    "レストラン","居酒屋","カフェ","ラーメン店",
    "工務店","不動産仲介","税理士事務所",
    "学習塾","英会話教室","保育園","写真館",
    "ホテル","旅館","ハウスクリーニング","引越し","葬儀社",
    "IT受託開発","B2Bコンサル","SaaS","ECショップ"
]

PROMPT_CLASSIFY = r"""
あなたは企業サイトの一次情報のみを用いて特徴を同定するアナリストです。
入力は公式サイトURLのみ。この公式サイトを持つ会社の色々な情報を取得してほしい。
基本的には与えられたURLに関してのHTTP取得でサイトを確認し、以下のJSONだけを返してください。URLや[1]等の引用番号は返却値に含めないで。

# 対象URL
{hp_url}

# 出力仕様
- company_name は会社概要等のページから、会社の正式名称を取得して
- business_type は次のリストから必ず1つ（該当なしは "その他"）。名詞のみ/最大10文字/記号・絵文字禁止/英語はカタカナ。
- confidence が0.7未満なら business_type は "その他" にする。
- strengths: 公式サイトから読み取れる「強み」を 40〜120字で要約（箇条書き不可、一次情報のみ）。
- values: 経営理念/大事にしていることを 30〜100字で要約（一次情報のみ）。
- address_text: 会社概要/会社情報/アクセス等からの**住所一行表記**（例: "〒123-4567 東京都渋谷区〇〇1-2-3"）。不明なら空文字。

候補: {vocab_list}

# 応答（JSONのみ、キー順固定）
{{
  "company_name": "正式な社名/屋号（20字以内、なければ空）",
  "business_type": "候補から1つ or その他",
  "other_label": "その他のときのみ5〜10文字、そうでないときは空",
  "strengths": "40〜120字",
  "values": "30〜100字",
  "address_text": "住所一行表記（不明なら空）",
  "evidence": "根拠の簡潔説明（20〜60字）",
  "confidence": 0.0
}}
"""

PROMPT_SALES = r"""
＝＝＝
＃営業文章テンプレート
【ご提案】素敵な{business_type}のWEB集客を「成果が出た時だけ発生する費用」で支援させて頂けませんか？

{company_name}　ご担当者様

突然のご連絡失礼します。友人に依頼され{business_type}を探していたところ偶然御社を拝見し、サービスクオリティがあまりにも高そうだったのでつい連絡してしまいました。（ここに{strengths}や{values}より、この事業所を称える文章をいれて）

自己紹介おくれました、ウェブ経由の集客支援をしている株式会社S-gate代表の佐野と申します。特に御社のようなハイクオリティなサービスで勝負されている事業者様ではWEB集客を強化することで、営業の安定化及び更なる利益向上に結び付きやすく、よければお話できないかと思い、連絡させていただきました。

弊社は店舗運営事業者向けに、「検索KWが上位10位にランクインした場合のみ料金発生するSEO対策サービス」を運営しております。弊社は自社でもSNSやウェブサイト運用を通して集客をしていることから「血の通ったノウハウ提供」ができると自負しております。
さらに私たちは{business_type}の業者様向けのウェブ集客を支援し、1か月以内に売上を50％ほど向上させた実績もあり、その際のノウハウを惜しみなく共有することができます。

ぜひ貴店のようなハイクオリティーなサービスを持ったお店に売上を伸ばしてほしいです。
以下連絡先です。もしよければメール（sano@s-gate-tokyo.co.jp）より連絡ください。

メール：sano@s-gate-tokyo.co.jp
会社HP：https://s-gate-tokyo.co.jp/
紹介資料：https://drive.google.com/file/d/1OKjBJhIUZrM9NskhNfdo9w9H22psE3yo/view?usp=sharing
＝＝＝
"""

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
PROJECT_ID = (
    os.getenv("GOOGLE_CLOUD_PROJECT")
    or os.getenv("GCLOUD_PROJECT")
    or os.getenv("GClOUD_PROJECT_ID")
    or os.getenv("GCLOUD_PROJECT_ID")
    or "test-250817-469308"
)
failure_storage_SPREADSHEET_ID = os.getenv("failure_storage_SPREADSHEET_ID", "")

# %% [markdown]
# ### dfの読み込み

# %%
client_id, df, csv_path = load_incoming_df()
print("client_id:", client_id)
print("csv_path:", csv_path)

# Optional: normalize header
if '会社名' in df.columns and 'company_name' not in df.columns:
    df = df.rename(columns={'会社名': 'company_name'})

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
def run_batches(start: int = 1200, duration: int = 1, cycle: int = 1) -> None:
    #必ず一旦動作確認を行うこと！
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

        bq_df = prepare_contact_url_filled_df_for_bq(
            contact_url_filled_df,
            client_id=client_id  # 省略可。未指定時は空文字が入ります
        )
        loaded = load_sales_list_df_to_bq(
            bq_df,
            project_id=PROJECT_ID,
            dataset_id='dev',
            table_id='sales_list',
            location='asia-northeast1',
            write_disposition='WRITE_APPEND',
            require_all_columns=True,
        )
        print(f'✅ Loaded {loaded} rows to {PROJECT_ID}.dev.sales_list')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run contact URL fetching and sales copy generation batches")
    parser.add_argument("--start", type=int, default=1200)
    parser.add_argument("--duration", type=int, default=1)
    parser.add_argument("--cycle", type=int, default=1)
    # Use parse_known_args to ignore IPython/Jupyter-injected args like --f=... when run via %run
    args, _unknown = parser.parse_known_args()
    run_batches(start=args.start, duration=args.duration, cycle=args.cycle)
