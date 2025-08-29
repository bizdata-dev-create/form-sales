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

# %% [markdown]
# # 事前設定のインストール

# %%
from form_url_fetch_msg_write_py._01_helpers import *
# 以後 helpers.xxx を利用

# %% [markdown]
# ## APIやCSEの読み込み

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
cse_client = make_search_client(GOOGLE_API_KEY, CSE_ID)

# OpenAI クライアントは各関数内で os.getenv("OPENAI_API_KEY", "") を参照

# %% [markdown]
# ## dfの読み込み

# %%
#df の格納を確認
df = pd.read_csv(r'C:\Users\qingy\Documents\自動フォーム営業事業\対象リスト\グーネットリスト.csv')
# df = pd.read_excel('/content/グーネットURL検証.xlsx')
df.rename(columns={'会社名': 'company_name'}, inplace=True)
df.head()

# %% [markdown]
# # 営業文プロンプトテスト

# %% [markdown]
# ## プロンプト変数

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

# %%
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


# %% [markdown]
# ## テストして、良し悪しを見る

# %%
# 修正版で再実行
for i in range(len(df[17:20])):
    name = df.at[i, 'company_name']
    url = get_hp_url(name, cse_client, CSE_ID)
    print("url:",url)
    # resp = openai.responses.create(
    #     model="gpt-5-mini",  
    #     input=PROMPT_CLASSIFY.format(hp_url=url,vocab_list=", ".join(BUSINESS_TYPE_VOCAB)),
    #     tools=[{"type": "web_search"}]  # 内蔵Web検索を有効化
    # )
    # comp_data = json.loads(resp.output_text)
    # print(comp_data)
    # print("="*80)
    # text = generate_sales_copy_with_infomation(comp_data, PROMPT_SALES, model="gpt-5-mini")
    # print(text)
    print(i,"finished","="*80)

