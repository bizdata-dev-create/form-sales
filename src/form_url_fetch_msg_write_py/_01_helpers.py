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

# %% [markdown] id="8RnUTOYIaFBb"
# # 問い合わせURL取得

# %% [markdown] id="X6ZutDC4VoIp"
# ## 関数群

# %% [markdown]
# ### ログレベルの調整

# %%
##xxx実験----------------------------------------------

# %%
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

print("ログ設定完了")

# %% colab={"base_uri": "https://localhost:8080/"} id="ptwJFOn7p4DD" outputId="47bde079-b9f8-48eb-c130-0a0339c0aa49"
# ─── 必要なライブラリのインストール ─────────────────────────
# !pip install google-api-python-client beautifulsoup4
# !pip -q install openpyxl odfpy
# !pip -q install gspread gspread_dataframe

# %% id="4y2w5yroWJuW"
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
# プロジェクト直下の .env を特定して上書き読み込み
project_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".env").exists())
env_file = str(project_root / ".env")
load_dotenv(dotenv_path=env_file, override=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
CSE_ID = os.getenv("CSE_ID", "")
# cse = make_search_client(GOOGLE_API_KEY, CSE_ID)

import os

# %%
import sys
print(sys.executable)

# %% [markdown]
# ## 問い合わせ取得

# %% id="j9Q1McZspUh7"
# ─── インポート ────────────────────────────────────────────
import pandas as pd
import requests
import logging
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from typing import Optional, Tuple, List
from datetime import datetime
from tqdm.auto import tqdm
from datetime import datetime

# ─── ロギング設定 ──────────────────────────────────────────
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

# ─── Google Custom Search API クライアント作成 ──────────────
def make_search_client(api_key: str, cse_id: str):
    logging.debug("Creating Custom Search client")
    client = build("customsearch", "v1", developerKey=api_key).cse()
    logging.debug("Custom Search client created")
    return client

# ─── 会社名で公式サイト URL を取得 ───────────────────────────
def get_hp_url(company_name: str, cse_client, cse_id: str) -> str:
    logging.debug(f"Searching HP URL for: {company_name}")
    try:
        res = cse_client.list(q=company_name, cx=cse_id, num=1).execute()
        items = res.get("items", [])
        hp = items[0]["link"] if items else None
        logging.debug(f"→ HP URL found: {hp}")
        return hp
    except Exception as e:
        logging.error(f"Error fetching HP URL for {company_name}: {e}")
        return None

# ─── ページがフォームか判定 ───────────────────────────
def is_form_page(soup: BeautifulSoup) -> bool:

    text_types ={"text","email","tel","number"}
    inputs = soup.find_all(
        "input",
        attrs={
            "type": lambda t: t and t.lower() in text_types,
            "name": True
        }
    )
    return len(inputs) >= 3

    return True

# ─── 問い合せURL取得 ───────────────────────────
def get_contact_url(hp_url: str, timeout: float = 5.0) -> Optional[str]:
    """
    hp_url から最大深度3までリンクをたどり、
    お問い合わせフォームページと判断できた URL を返す。
    見つからなければ None。
    """
    logging.debug(f"Searching contact URL on: {hp_url}")
    if not hp_url:
        logging.warning("No HP URL provided, skipping contact search")
        return None

    # キーワード定義
    primary_kw   : List[str] = ["問い合わせ", "お問い合わせ", "問合わせ", "問い合せ", "コンタクト", "contact", "inquiry", "request", "entry"]
    secondary_kw : List[str] = ["フォーム", "その他", "採用", "IR", "本部"] + primary_kw

    session = requests.Session()

    def fetch_soup(url: str) -> Tuple[Optional[BeautifulSoup], Optional[str]]:
        """ URL を GET して (soup, 最終的な絶対URL) を返す """
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser"), resp.url
        except Exception as e:
            # logging.warning(f"  → Failed to fetch {url}: {e}")
            return None, None

    def extract_links(soup: BeautifulSoup, base_url: str, kws: List[str]) -> List[str]:
        """
        <a href> の text or href にキーワードが含まれるものを抽出し、
        絶対URLで返す
        """
        results: List[str] = []
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"].lower()
            if any(kw in text for kw in kws) or any(kw in href for kw in kws):
                abs_url = urljoin(base_url, a["href"])
                logging.debug(f"   → Candidate link: {abs_url}")
                results.append(abs_url)
        return results

    # 深度ごとのキーワードマップ
    kw_by_depth = {1: primary_kw, 2: secondary_kw, 3: secondary_kw}

    # BFS で最大深度3まで探索
    frontier = [(hp_url, 0)]  # (URL, depth)
    visited  = set()

    while frontier:
        url, depth = frontier.pop(0)
        if url in visited or depth >= 3:
            continue
        visited.add(url)

        soup, real_url = fetch_soup(url)
        if soup is None:
            continue

        logging.debug(f"[depth={depth}] Visiting: {real_url}")

        # depth>0 のページでフォーム判定
        if depth > 0 and is_form_page(soup):
            # logging.info(f"Contact form URL found at depth {depth}: {real_url}")
            # print(f"Contact form URL found at depth {depth}: {real_url}")
            return real_url

        # 次の深度のリンクを抽出してキューに追加
        next_depth = depth + 1
        kws = kw_by_depth.get(next_depth, [])
        if not kws:
            continue

        for link in extract_links(soup, real_url, kws):
            frontier.append((link, next_depth))

    logging.info("Contact form URL not found within depth 3")
    return None

def fill_contact_from_hp(df):
    mask = df['contact_url'].isna() & df['hp_url'].str.contains(r'contact|inquiry|toiawase|お問い合わせ|お問合せ', case=False, na=False)
    df.loc[mask, 'contact_url'] = df.loc[mask, 'hp_url']
    return df


# ─── DataFrame に対して一括処理 ─────────────────────────────
def fill_urls(df: pd.DataFrame, api_key: str, cse_id: str) -> pd.DataFrame:
    cse = make_search_client(api_key, cse_id)
    hp_urls = []
    contact_urls = []

    for i, name in tqdm(enumerate(df["company_name"], start=1)):
        # print(f"=== 処理開始 {i}/{len(df)}: {name} ===")
        # logging.info(f"=== 処理開始 {i}/{len(df)}: {name} ===")

        # 公式サイト URL
        hp = get_hp_url(name, cse, cse_id)
        hp_urls.append(hp)

        # 問い合わせフォーム URL
        contact = get_contact_url(hp)
        contact_urls.append(contact)

        logging.info(f"→ 結果: HP={hp}, Contact={contact}\n")

        time.sleep(1)  # レート制限対策

    df["hp_url"] = hp_urls
    df["contact_url"] = contact_urls
    return df


# %%
from dotenv import load_dotenv
load_dotenv()  # プロジェクト直下の .env を読み込む

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
CSE_ID = os.getenv("CSE_ID", "")
cse = make_search_client(GOOGLE_API_KEY, CSE_ID)
print("google_api_key:",GOOGLE_API_KEY)
print(GOOGLE_API_KEY)
names = ["ソニー","Apple"]
cse = make_search_client(GOOGLE_API_KEY, CSE_ID)
for name in names:
    hp = get_hp_url(name, cse, CSE_ID)
    print("HP:",hp)

# %% [markdown] id="hAK_42aaWSrB"
# ## ワークシート上不明のものを保存

# %% id="ckdsDSCmjO8K"
# 追加: ハッシュ付きアンカーも候補に入れる
COMMON_RELATIVE_PATHS = [
    "/contact",
    "/contact-us",
    "/contacact.html",
    "/contact/other/",
    "/contact/others/",
    "/contact/form",
    "/contact/recruit/",
    "/inquiry",
    "/inquiries",
    "/request",
    "/requests",
    "#contact",            # ← これを追加
]

from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from tqdm import tqdm

def fill_contact_url(df: pd.DataFrame, timeout: float = 7.0) -> pd.DataFrame:
    null_mask = df["contact_url"].isna() | df["contact_url"].astype(str).str.strip().eq("")
    print(null_mask)
    for idx, row in tqdm(df[null_mask].iterrows(), total=null_mask.sum()):
        base_url = row.get("hp_url")
        if not base_url:
            continue
        # ベースページは1回だけ取得（#fragment用）
        soup_home = None
        try:
            res_home = requests.get(base_url, timeout=timeout)
            if res_home.status_code == 200:
                soup_home = BeautifulSoup(res_home.content, "html.parser")
        except Exception as e:
            print(f"Error fetching base page {base_url}: {e}")

        tested = set()
        found = False

        for path in COMMON_RELATIVE_PATHS:
            # ハッシュ（#...）はページ内アンカー扱い
            if path.startswith("#"):
                if soup_home is None:
                    continue  # ホーム取得失敗時はスキップ
                # 該当アンカーが存在するか
                target = soup_home.select_one(path)  # 例: '#contact'
                if not target:
                    continue

                # セクション単位でフォームらしさを判定（だめならページ全体で判定）
                try:
                    if is_form_page(target) or is_form_page(soup_home):
                        contact_url = urljoin(base_url, path)
                        df.at[idx, "contact_url"] = contact_url
                        print(f"Found contact URL (fragment): {contact_url}")
                        found = True
                        break
                except Exception as e:
                    # is_form_page が Tag を想定していない場合はページ全体で再判定
                    try:
                        if is_form_page(soup_home):
                            contact_url = urljoin(base_url, path)
                            df.at[idx, "contact_url"] = contact_url
                            print(f"Found contact URL (fragment-fallback): {contact_url}")
                            found = True
                            break
                    except Exception as ee:
                        print(f"is_form_page error on fragment {path}: {ee}")
                continue

            # 通常の相対パスは今まで通り取得して判定
            test_url = urljoin(base_url, path)
            if test_url in tested:
                continue
            tested.add(test_url)

            try:
                res = requests.get(test_url, timeout=timeout)
                if res.status_code == 200:
                    soup = BeautifulSoup(res.content, "html.parser")
                    if is_form_page(soup):
                        df.at[idx, "contact_url"] = test_url
                        print(f"Found contact URL: {test_url}")
                        found = True
                        break  # 最初に見つけたフォームで終了
            except Exception as e:
                print(f"Error fetching {test_url}: {e}")
                continue  # タイムアウトや接続エラーは無視

        # 見つからなければ、ホーム内の<a href="#...">から#contact系を補足（保険）
        if not found and soup_home is not None:
            try:
                anchors = soup_home.select('a[href^="#"]')
                for a in anchors:
                    href = a.get("href", "")
                    text = (a.get_text() or "") + " " + href
                    if re.search(r"(contact|inquiry|お問い合わせ|お問合せ|問合せ)", text, re.I):
                        target = soup_home.select_one(href)
                        if target and (is_form_page(target) or is_form_page(soup_home)):
                            contact_url = urljoin(base_url, href)
                            df.at[idx, "contact_url"] = contact_url
                            print(f"Found contact URL (fragment-auto): {contact_url}")
                            break
            except Exception as e:
                print(f"Error scanning fragments on {base_url}: {e}")

    return df


# %% id="n2bNifpYYm0a"
# === 改善版エクスポート関数（API制限対策付き） ===
def export_unknown_contacts_to_gsheet_improved(df, spreadsheet_id, sheet_name):
    """
    改善版：問い合わせURLが未取得の企業データをGoogle Sheetsにエクスポートする
    （既存データを保持して新しいデータを追加、API制限対策付き）
    """
    try:
        import gspread
        from gspread_dataframe import set_with_dataframe
        from google.oauth2.service_account import Credentials
        import os
        import time
        
        # 認証情報の設定
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        
        # サービスアカウントキーファイルのパスを確認
        service_account_key_path = r"C:\Users\qingy\Documents\自動フォーム営業事業\form-sales-log-bffd68dc6996.json"
        
        if not os.path.exists(service_account_key_path):
            print(f"❌ サービスアカウントキーファイルが見つかりません: {service_account_key_path}")
            return export_unknown_contacts_to_csv(df)
        
        # サービスアカウントキーファイルを使用
        credentials = Credentials.from_service_account_file(
            service_account_key_path, 
            scopes=scope
        )
        gc = gspread.authorize(credentials)
        
        print(f"✅ 認証成功: {credentials.service_account_email}")
        
        # スプレッドシートを開く
        try:
            spreadsheet = gc.open_by_key(spreadsheet_id)
            print(f"✅ スプレッドシートアクセス成功: {spreadsheet.title}")
        except Exception as e:
            print(f"❌ スプレッドシートアクセスエラー: {e}")
            return export_unknown_contacts_to_csv(df)
        
        # シートを取得または作成
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            print(f"✅ 既存シートを使用: {sheet_name}")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            print(f"✅ 新規シートを作成: {sheet_name}")
        
        # 問い合わせURLが未取得のデータをフィルタリング
        unknown_contacts = df[
            df["contact_url"].isna() | 
            (df["contact_url"].str.strip() == "") |
            (df["contact_url"] == "None")
        ].copy()
        
        if len(unknown_contacts) == 0:
            print("✅ エクスポートする未取得データはありません")
            return
        
        print(f"📊 エクスポート対象: {len(unknown_contacts)}件")
        
        # 既存データの確認
        existing_data = worksheet.get_all_values()
        print(f"📋 既存データ行数: {len(existing_data)}")
        
        # 新しいデータを追加（既存データを保持）
        if len(existing_data) == 0:
            # シートが空の場合、ヘッダーとデータを追加
            print("�� 空のシートにデータを追加します")
            set_with_dataframe(worksheet, unknown_contacts)
        else:
            # 既存データがある場合、バッチ処理で一括追加（API制限対策）
            print("�� 既存データの最終行から新しいデータを一括追加します")
            
            # 最終行の行番号を取得
            next_row = len(existing_data) + 1
            
            # バッチ処理で一括追加（API制限対策）
            batch_data = []
            for row in unknown_contacts.values:
                # データを文字列として変換（Noneを空文字列に）
                row_data = [str(val) if val is not None else "" for val in row]
                batch_data.append(row_data)
            
            # 一括でデータを追加
            worksheet.update(f'A{next_row}', batch_data)
            
            # API制限対策のため少し待機
            time.sleep(2)
            
            print(f"✅ {len(unknown_contacts)}件のデータを{next_row}行目から一括追加しました")
        
        print(f"🎉 完了: {len(unknown_contacts)}件の未取得データを{sheet_name}シートに追加しました")
        
    except Exception as e:
        print(f"❌ Google Sheetsエクスポートエラー: {e}")
        print("CSVファイルにエクスポートします...")
        export_unknown_contacts_to_csv(df)

def export_unknown_contacts_to_csv(df, filename=None):
    """
    CSVファイルにエクスポートする関数（フォールバック用）
    """
    if filename is None:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"unknown_contacts_{timestamp}.csv"
    
    # 問い合わせURLが未取得のデータをフィルタリング
    unknown_contacts = df[
        df["contact_url"].isna() | 
        (df["contact_url"].str.strip() == "") |
        (df["contact_url"] == "None")
    ].copy()
    
    if len(unknown_contacts) == 0:
        print("✅ エクスポートする未取得データはありません")
        return
    
    # CSVファイルに保存
    unknown_contacts.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"📁 CSVファイルに保存しました: {filename}")
    print(f"�� 保存件数: {len(unknown_contacts)}件")

print("✅ 改善版エクスポート関数定義完了（API制限対策付き）")
print("使用方法: export_unknown_contacts_to_gsheet_improved(contact_url_filled_df, failure_storage_SPREADSHEET_ID, '問い合わせURL未取得')")

# %% [markdown] id="twlHm1UABx98"
#

# %% [markdown] id="jivy57lYhj0k"
# # 営業文章生成

# %% [markdown] id="vAuEPnLZFE-h"
# ## 準備

# %% id="dXNYHOYfekro"
import pandas as pd
from datetime import date

# %% [markdown] id="ora6nrXgZ7fM"
# ## 会社の情報取得
#

# %% id="RehXryURvVyo"
import re, json

def classify_business_details(api_key: str, hp_url: str,
                              model: str="gemini-2.0-flash",
                              temperature: float=0.0,
                              timeout: int=300) -> dict:
    # ← ここで timeout をまとめて効かせる（request_options は使わない）
    client = genai.Client(api_key=api_key, http_options={'api_version': 'v1alpha'})

    prompt = PROMPT_CLASSIFY.format(hp_url=hp_url,vocab_list=", ".join(BUSINESS_TYPE_VOCAB))
    # print("="*80)
    # print(prompt)
    # print("="*80)

    system_instruction=(
        "公式サイトの一次情報のみを根拠に抽出し、"
        "以下キーだけのJSON文字列を返す。余計な文やマークダウンは一切禁止："
        "company_display_name, business_type, other_label, strengths, values, "
        "address_text, evidence, confidence"
    ),

    # ツール併用時は response_mime_type/response_schema は付けない（400回避）

    # 検索ツールを有効化（あなたの例と同じ書式）
    config = gtypes.GenerateContentConfig(
        system_instruction=system_instruction,
        tools=[{"google_search": {}}],
        temperature=temperature,
    )

    # 実行
    resp = client.models.generate_content(
        model=model,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            tools=[{"google_search": {}}],
            temperature=temperature,
        ),
        contents=prompt
    )

    raw = (resp.text or "").strip()
    raw = re.sub(r"^```json\s*|\s*```$", "", raw).strip()
    return raw



# %% [markdown] id="Yf0DdHRe920T"
# #### ミニテスト

# %% colab={"base_uri": "https://localhost:8080/"} id="2ZAAuEpGqG-t" outputId="0a68d9cc-cf31-47b4-fc43-68806e823007"
urls = [
    # "https://www.2and4-auto.jp/",
    # "https://www.cardrshuu.com/#contact",
    # "http://www.nomiyama-car.com/contact/",
    # "https://h-bpc.com/",
    # "https://tauros-japan.com/",
    # "https://www.saitomotor-hirosaki.com/",
    # "https://kurumayakoubou.jp/",
]
for url in urls:
  prompt = PROMPT_CLASSIFY.format(hp_url=url,vocab_list=", ".join(BUSINESS_TYPE_VOCAB))
  for i in range(1):
    resp = openai.responses.create(
      model="gpt-5-mini",  # 例：コスト重視なら mini / 品質重視なら gpt-5
      input=prompt,
      tools=[{"type": "web_search"}]  # 内蔵Web検索を有効化
    )
    print(resp.output_text)
  print("="*80)

# %% [markdown] id="LZWwGZ-C986o"
# ## 営業文章作成

# %% id="BbghYR8v14iG"
# pip install openai  # 未導入なら
import os, re
from openai import OpenAI

def generate_sales_copy_with_infomation(
    company_info: dict,
    prompt_template: str,
    *,
    model: str = "gpt-5-mini",   # 例: GPT-5 mini 系。環境に合わせて変更可
    temperature: float = 1.0,
    timeout: int = 120,
    api_key: str | None = None,
) -> str:
    """
    Web検索は使わず、与えられた会社情報とテンプレから営業文章を生成する。
    - company_info: {
        "company_name", "business_type", "other_label", "strengths",
        "values", "address_text", "evidence", "confidence"
      }
    - prompt_template: 例示の営業テンプレ（{business_type} 等のプレースホルダを含む）
    戻り値: 日本語の営業文章（改行維持）
    """

    # 1) 会社情報の前処理（欠損フォールバック）
    company_name   = (company_info.get("company_name") or "").strip() or "貴社"
    business_type  = (company_info.get("business_type") or "").strip() or "その他"
    other_label    = (company_info.get("other_label") or "").strip()
    strengths      = (company_info.get("strengths") or "").strip()
    values         = (company_info.get("values") or "").strip()
    address_text   = (company_info.get("address_text") or "").strip()

    # "その他" は other_label→なければ汎称
    bt_final = business_type if business_type != "その他" else (other_label or "店舗")

    # 2) OpenAI クライアント
    client = OpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY", ""))

    # 3) 会話設定（敬意句は定型にせず自然文で）
    system_msg = (
        "あなたは日本語のB2B営業ライターです。"
        "入力として会社情報（一次情報由来の要約）と営業テンプレートが与えられます。"
        "営業テンプレートは完全にフォローする必要はありません。より自然な日本語にしてください。"
        "ただし、営業テンプレートの最後にある、こちらの連絡情報は必ず正確に過不足なく反映させてください。"
        "テンプレートのプレースホルダを埋め、括弧内の指示部分は事実ベースの自然な一文で置換してください。"
        "引用番号や生のURLリンクは本文に入れないでください（署名欄に含まれる固定URLは可）。"
        "文体は丁寧、誇張は避け、改行・段落構成は保ってください。"
    )

    # 4) ユーザー入力：会社情報＋テンプレ
    user_msg = f"""
# 会社情報（一次情報の要約）
company_name: {company_name}
business_type: {bt_final}
strengths: {strengths}
values: {values}
address_text: {address_text}

# 重要なルール
- business_type はそのまま「{{business_type}}」へ差し込み（言い換え不可）。
- 「（ここに{{strengths}}や{{values}}より、この事業所を称える文章をいれて）」の部分は、
  strengths/values から読み取れる具体を1〜2点だけ織り込んだ**自然な1文**で置換すること。
- 引用番号やURLリンクは本文に入れない。
- 出力は本文のみ（コードブロック禁止）。

# 営業テンプレート
        {
            prompt_template
        #  .format(
        #     business_type=bt_final,
        #     company_name=company_name,
        #     strengths=strengths or "（強み情報は未取得）",
        #     values=values or "（理念情報は未取得）",
        #     address_text=address_text)
        }
    """.strip()

    # print("prompt:","="*80)
    # print(user_msg)
    # print("="*80)

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role":"system", "content": system_msg},
            {"role":"user",   "content": user_msg},
        ],
        temperature=temperature,
        timeout=timeout,
    )

    text = resp.choices[0].message.content.strip()

    # フェンス除去 & 余計なコードブロック対策
    text = re.sub(r"^```(?:\w+)?\s*|\s*```$", "", text, flags=re.S).strip()
    return text


# %%
import sys
print("Python executable:", sys.executable)
print("Python version:", sys.version)
print("Python path:", sys.path[:3])  # 最初の3つのパスを表示

# %% [markdown] id="KUT2pDUh-PoX"
# #### テスト

# %% colab={"base_uri": "https://localhost:8080/"} id="QuL4XRFB6Dbf" outputId="dbfda394-5be8-4f7e-a155-1e4232a0ef7d"
import json
urls = [
    # "https://www.2and4-auto.jp/",
    # "https://www.cardrshuu.com/#contact",
    # "http://www.nomiyama-car.com/contact/",
    # "https://h-bpc.com/",
    # "https://tauros-japan.com/",
    # "https://www.saitomotor-hirosaki.com/",
    # "https://kurumayakoubou.jp/",
]
for url in urls:
  resp = openai.responses.create(
    model="gpt-5-mini",  # 例：コスト重視なら mini / 品質重視なら gpt-5
    input=PROMPT_CLASSIFY.format(hp_url=url,vocab_list=", ".join(BUSINESS_TYPE_VOCAB)),
    tools=[{"type": "web_search"}]  # 内蔵Web検索を有効化
  )
  comp_data = json.loads(resp.output_text)
  text = generate_sales_copy_with_infomation(comp_data, PROMPT, model="gpt-5-mini")
  print(text)
  # print("="*80)

# %% [markdown] id="bzGD3GSP6DJM"
# # 実行モジュール

# %% id="LN7rLf5OHTCO"
# 事前: !pip -q install tqdm openai
import json, re, time
from typing import Iterable, Optional
import pandas as pd
from openai import OpenAI
from tqdm.auto import tqdm

def _extract_json(text: str) -> str:
    s = text.strip()
    s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s, flags=re.S).strip()
    m = re.search(r"\{[\s\S]*\}$", s)
    return (m.group(0) if m else s)

def fill_sales_copy_with_gpt(
    df: pd.DataFrame,
    *,
    url_col: str = "hp_url",
    out_col: str = "sales_copy",
    model: str = "gpt-5-mini",
    classify_prompt_template: str = None,   # 例: PROMPT_CLASSIFY
    sales_prompt_template: str = None,      # 例: PROMPT
    business_vocab: Optional[Iterable[str]] = None,  # 例: BUSINESS_TYPE_VOCAB
    overwrite: bool = True,
    sleep_sec: float = 0.8,
    openai_api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    各行: hp_url -> (分類JSON) -> generate_sales_copy_with_infomation -> sales_copy に格納
    進捗バーは1本だけ表示。
    """
    if out_col not in df.columns:
        df.loc[:, out_col] = ""

    if classify_prompt_template is None or sales_prompt_template is None or business_vocab is None:
        raise ValueError("classify_prompt_template / sales_prompt_template / business_vocab を指定してください。")

    client = OpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY", ""))
    mask = df[url_col].notna() & df[url_col].astype(str).str.strip().ne("")
    idxs = df[mask].index
    vocab_str = ", ".join(business_vocab)

    # --- 進捗バー ---
    for i in tqdm(idxs, total=len(idxs), desc="営業文生成", unit="社"):
        if (not overwrite) and isinstance(df.at[i, out_col], str) and df.at[i, out_col].strip():
            continue

        url = str(df.at[i, url_col]).strip()
        try:
            # 1) 分類（JSON生成・Web検索ON）
            prompt_cls = classify_prompt_template.format(hp_url=url, vocab_list=vocab_str)
            # print("reach here ==============")
            resp = client.responses.create(
                model=model,
                input=prompt_cls,
                tools=[{"type": "web_search"}],
            )
            comp_json = _extract_json(resp.output_text)
            comp_data = json.loads(comp_json)
            # print("reach fetching comp data ==============")

            # 2) 営業文生成（検索なし）
            text = generate_sales_copy_with_infomation(
                company_info=comp_data,
                prompt_template=sales_prompt_template,
                model=model,
                temperature=1.0,
            )
            # print("text",text)
            df.at[i, out_col] = (text or "").strip()

        except Exception:
            df.at[i, out_col] = ""
        time.sleep(sleep_sec)

    return df
