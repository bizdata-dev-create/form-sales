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
# # 事前設定

# %% colab={"base_uri": "https://localhost:8080/"} id="ptwJFOn7p4DD" outputId="47bde079-b9f8-48eb-c130-0a0339c0aa49"
# ─── 必要なライブラリのインストール ─────────────────────────
# !pip install google-api-python-client beautifulsoup4
# !pip -q install openpyxl odfpy
# !pip -q install gspread gspread_dataframe

# %% id="4y2w5yroWJuW"
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery
# プロジェクト直下の .env を特定して上書き読み込み
project_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".env").exists())
env_file = str(project_root / ".env")
load_dotenv(dotenv_path=env_file, override=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
CSE_ID = os.getenv("CSE_ID", "")
GCLOUD_PROJECT_ID = os.getenv("GClOUD_PROJECT_ID", "")
print("GCLOUD_PROJECT_ID:",GCLOUD_PROJECT_ID)
import os

# %%
import sys
print(sys.executable)

# %% [markdown]
# # 問い合わせURL取得

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
cse = make_search_client(GOOGLE_API_KEY, CSE_ID)

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
        
        # サービスアカウントキーファイルのパスを確認（VM対応）
        project_root = get_form_sales_root()
        service_account_key_path = project_root / "secrets" / "form-sales-log-bffd68dc6996.json"
        
        if not service_account_key_path.exists():
            print(f"❌ サービスアカウントキーファイルが見つかりません: {service_account_key_path}")
            return export_unknown_contacts_to_csv(df)
        
        # サービスアカウントキーファイルを使用
        credentials = Credentials.from_service_account_file(
            str(service_account_key_path), 
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

# %% [markdown]
# ## dfの格納

# %%
from pathlib import Path
import os, re
import pandas as pd

# Detect form-sales root on both VM and local

def get_form_sales_root() -> Path:
    env = os.getenv("FORM_SALES_ROOT")
    if env:
        p = Path(env)
        if p.exists():
            return p
    # Prefer module location to avoid CWD-induced duplicates like 'form-sales/form-sales'
    try:
        this_file = Path(__file__).resolve()
        # .../form-sales/src/form_url_fetch_msg_write_py/_01_helpers.py → root = parents[2]
        module_root = this_file.parents[2]
        if (module_root / "src").exists():
            return module_root
    except Exception:
        pass
    # Fallback: Walk up to find a directory that looks like the project root
    cwd = Path.cwd()
    for p in [cwd, *cwd.parents]:
        # 1) If this dir itself is named 'form-sales' and has 'src', use it
        if p.name == "form-sales" and (p / "src").exists():
            return p
        # 2) If a child named 'form-sales' exists and has 'src', use it
        child = p / "form-sales"
        if child.exists() and child.is_dir() and (child / "src").exists():
            return child
    return cwd

def resolve_incoming_dir() -> Path:
    """Resolve incoming directory each call to avoid stale env values and ensure it exists."""
    env_dir = os.getenv("INCOMING_DIR")
    base = Path(env_dir) if env_dir else (get_form_sales_root() / "data" / "targets" / "incoming")
    # Normalize and ensure existence
    base = base.resolve()
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        # If creation fails (e.g., permission), continue; caller will error clearly
        pass
    return base

CLIENT_FILE_REGEX = re.compile(r"^(?P<client_id>[A-Za-z0-9_-]+)_\d{8}\.csv$", re.IGNORECASE)

def find_latest_incoming_csv(directory: Path) -> tuple[str, Path]:
    """
    Find the latest CSV matching pattern '<clientid>_YYYYMMDD.csv' in the given directory.
    Returns (client_id, file_path).
    """
    if not directory.exists():
        raise FileNotFoundError(f"Incoming directory not found: {directory}")
    candidates: list[tuple[str, Path]] = []
    for p in directory.glob("*.csv"):
        m = CLIENT_FILE_REGEX.match(p.name)
        if m:
            candidates.append((m.group("client_id"), p))
    if not candidates:
        raise FileNotFoundError(f"No CSV like '<clientid>_YYYYMMDD.csv' in: {directory}")
    # latest by modified time
    candidates.sort(key=lambda t: t[1].stat().st_mtime, reverse=True)
    return candidates[0]

def load_incoming_df() -> tuple[str, pd.DataFrame, Path]:
    """
    Resolve the incoming CSV path in a VM-friendly way and return (client_id, df, path).
    Priority:
      1) ENV INCOMING_DIR if provided
      2) auto-detected 'form-sales/data/targets/incoming'
    """
    incoming_dir = resolve_incoming_dir()
    client_id, p = find_latest_incoming_csv(incoming_dir)
    # read with UTF-8 BOM tolerant
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")
    ext = p.suffix.lower()
    df = pd.read_excel(p) if ext in {".xlsx", ".xls"} else pd.read_csv(p, encoding="utf-8-sig")
    return client_id, df, p


# %% [markdown]
# ## 営業文章生成

# %%
from datetime import datetime
import os
import json
import time
from typing import Optional, Iterable
import pandas as pd
from tqdm import tqdm
from openai import OpenAI
# 事前: !pip -q install tqdm openai

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
    進捗バーは1本だけ表示。営業文生成時に "record_created_at" を "YYYY-MM-DD HH:MM:SS" で記録。
    """
    print(f"🔍 営業文生成開始: {len(df)}件のデータを処理します")
    
    # 空のDataFrameの場合は早期リターン
    if df.empty:
        print("⚠️ DataFrameが空のため、営業文生成をスキップします")
        return df
    
    # 出力列とタイムスタンプ列の用意
    if out_col not in df.columns:
        df.loc[:, out_col] = ""
    record_col = "record_created_at"
    if record_col not in df.columns:
        df.loc[:, record_col] = pd.NaT

    if (
        classify_prompt_template is None
        or sales_prompt_template is None
        or business_vocab is None
    ):
        raise ValueError(
            "classify_prompt_template / sales_prompt_template / business_vocab を指定してください。"
        )

    client = OpenAI(api_key=openai_api_key or os.getenv("OPENAI_API_KEY", ""))
    api_key = openai_api_key or os.getenv("OPENAI_API_KEY", "")
    print(f"🔑 OpenAI API クライアント初期化完了: {api_key[:10] if api_key else 'None'}...")
    
    mask = df[url_col].notna() & df[url_col].astype(str).str.strip().ne("")
    idxs = df[mask].index
    vocab_str = ", ".join(business_vocab)
    
    print(f"📊 処理対象: {len(idxs)}件 (URL列: {url_col})")
    print(f"📚 ビジネス語彙: {vocab_str[:100]}...")

    # --- 進捗バー ---
    for i in tqdm(idxs, total=len(idxs), desc="営業文生成", unit="社"):
        print(f"\n🏢 処理中: 行 {i} (URL: {df.at[i, url_col][:50] if df.at[i, url_col] else 'None'}...)")
        
        # 既存の出力があり、かつ上書きしない場合はスキップ
        if (not overwrite) and isinstance(df.at[i, out_col], str) and df.at[i, out_col].strip():
            print(f"⏭️ 行 {i}: 既存の営業文があるためスキップ")
            continue

        url = str(df.at[i, url_col]).strip()
        try:
            print(f"🔍 行 {i}: 1) 分類処理開始 (URL: {url[:50]}...)")
            
            # 1) 分類（JSON生成・Web検索ON）
            prompt_cls = classify_prompt_template.format(hp_url=url, vocab_list=vocab_str)
            print(f"📝 行 {i}: 分類プロンプト生成完了 (長さ: {len(prompt_cls)}文字)")
            
            resp = client.responses.create(
                model=model,
                input=prompt_cls,
                tools=[{"type": "web_search"}],
            )
            print(f"✅ 行 {i}: OpenAI API 分類レスポンス受信完了")
            
            comp_json = _extract_json(resp.output_text)
            print(f"📋 行 {i}: JSON抽出完了 (長さ: {len(comp_json)}文字)")
            
            comp_data = json.loads(comp_json)
            print(f"🔍 行 {i}: JSON解析完了: {list(comp_data.keys())}")

            print(f"✍️ 行 {i}: 2) 営業文生成開始")
            # 2) 営業文生成（検索なし）
            text = generate_sales_copy_with_infomation(
                company_info=comp_data,
                prompt_template=sales_prompt_template,
                model=model,
                temperature=1.0,
            )
            text_str = (text or "").strip()
            print(f"📝 行 {i}: 営業文生成完了 (長さ: {len(text_str)}文字)")
            
            df.at[i, out_col] = text_str

            # 生成できた場合のみ作成日時を記録（日本時間、秒精度）
            if text_str:
                df.at[i, record_col] = pd.Timestamp.now(tz='Asia/Tokyo').floor("S")
                print(f"✅ 行 {i}: 営業文生成成功、日本時間タイムスタンプ設定")
            else:
                # 営業文が生成できなかった場合は現在時刻を設定（BigQueryエラー回避）
                df.at[i, record_col] = pd.Timestamp.now(tz='Asia/Tokyo').floor("S")
                print(f"⚠️ 行 {i}: 営業文が空、日本時間タイムスタンプのみ設定")

        except Exception as e:
            # 失敗時は出力を空にし、作成日時は現在時刻を設定（BigQueryエラー回避）
            print(f"❌ 行 {i}: エラー発生: {type(e).__name__}: {str(e)}")
            import traceback
            print(f"📋 行 {i}: 詳細エラー: {traceback.format_exc()}")
            
            df.at[i, out_col] = ""
            df.at[i, record_col] = pd.Timestamp.now(tz='Asia/Tokyo').floor("S")
            print(f"🔄 行 {i}: エラー復旧完了、日本時間タイムスタンプ設定")
        
        time.sleep(sleep_sec)
        print(f"⏱️ 行 {i}: 処理完了、{sleep_sec}秒待機")

    print(f"🎉 営業文生成完了: {len(df)}件のデータを処理しました")
    return df

# %% [markdown]
# ## Big query書き込み

# %%
# %pip install -U google-cloud-bigquery google-cloud-bigquery-storage pandas-gbq db-dtypes pyarrow

# %%
# import google.auth
# creds, adc_proj = google.auth.default()
# from google.cloud import bigquery
# client = bigquery.Client()
# print("ADC:", adc_proj, "Client:", client.project)  # 両方が 469308 側になっていればOK

# %%
# # BigQuery: プロジェクト/テーブル設定とスキーマ確認 (①カラムを洗い出す)
# import os
# import pandas as pd


# # 469308 側を既定に。必要なら書き換え可
# GCLOUD_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCLOUD_PROJECT") or "test-250817-469308"
# DATASET_ID = "dev"           # ここを実環境に合わせて変更可
# TABLE_ID = "sales_list"      # ここを実環境に合わせて変更可
# TABLE_FQN = f"{GCLOUD_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# client = bigquery.Client(project=GCLOUD_PROJECT_ID, location="asia-northeast1")

# # スキーマ取得
# _table = client.get_table(TABLE_FQN)
# schema_df = pd.DataFrame([
#     {"name": f.name, "type": f.field_type, "mode": f.mode, "description": f.description}
#     for f in _table.schema
# ])
# print("TABLE:", TABLE_FQN)
# print(schema_df)



# %%
# from google.cloud import bigquery
# import pandas as pd

# # 既存の PROJECT_ID/DATASET_ID/TABLE_ID/TABLE_FQN は上のセルの値を流用
# sent_at_value = "2020-01-01 00:00:00"  # DATETIME用（タイムゾーンなし）

# data = [
#     {
#         "client_id": "TEST-001",
#         "recipient_company_name": "株式会社テスト1",
#         "hp_url": "https://example.com/1",
#         "contact_url": "https://example.com/1/contact",
#         "sales_copy": "ご挨拶1",
#         "record_created_at": sent_at_value,
#         "sent_at": sent_at_value,
#         "send_status": "draft",
#     },
#     {
#         "client_id": "TEST-002",
#         "recipient_company_name": "株式会社テスト2",
#         "hp_url": "https://example.com/2",
#         "contact_url": "https://example.com/2/contact",
#         "sales_copy": "ご挨拶2",
#         "record_created_at": sent_at_value,
#         "sent_at": sent_at_value,
#         "send_status": "draft",
#     },
#     {
#         "client_id": "TEST-003",
#         "recipient_company_name": "株式会社テスト3",
#         "hp_url": "https://example.com/3",
#         "contact_url": "https://example.com/3/contact",
#         "sales_copy": "ご挨拶3",
#         "record_created_at": sent_at_value,
#         "sent_at": sent_at_value,
#         "send_status": "draft",
#     },
# ]

# df = pd.DataFrame(data)
# insert_df = df[[c for c in df.columns if c in schema_df["name"].tolist()]].copy()

# # 念のため DATETIME として解釈させる（tz なし＝naive）
# insert_df["sent_at"] = pd.to_datetime(insert_df["sent_at"]).dt.tz_localize(None)
# insert_df["record_created_at"] = pd.to_datetime(insert_df["record_created_at"]).dt.tz_localize(None)

# client = bigquery.Client(project=GCLOUD_PROJECT_ID, location="asia-northeast1")
# job = client.load_table_from_dataframe(
#     insert_df,
#     TABLE_FQN,
#     bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
# )
# job.result()
# print("loaded rows:", len(insert_df))

# # 検証（今入れた client_id だけ取得）
# keys = insert_df["client_id"].tolist()
# qry = f"""
# SELECT client_id, recipient_company_name, hp_url, contact_url, sales_copy, sent_at, record_created_at, send_status
# FROM `{TABLE_FQN}`
# WHERE client_id IN UNNEST(@keys)
# ORDER BY client_id
# """
# res = client.query(
#     qry,
#     job_config=bigquery.QueryJobConfig(
#         query_parameters=[bigquery.ArrayQueryParameter("keys", "STRING", keys)]
#     ),
# ).result().to_dataframe()
# print(res)

# %%
def prepare_contact_url_filled_df_for_bq(
    df: pd.DataFrame,
    *,
    client_id: Optional[str] = None,
    send_status_value: str = "未送信",
    sent_at_value: str = "2020-01-01 00:00:00",
) -> pd.DataFrame:
    """
    contact_url_filled_df（営業文生成後のDF）を BigQuery投入用に整形する。

    実施内容:
      - 列名変更: "company_name" → "recipient_company_name"
      - 列追加:  "client_id"（指定があれば全行に設定、未指定時は空文字）
      - 列追加:  "send_status"（全行 "未送信"）
      - 列追加:  "sent_at"（全行 "2020-01-01 00:00:00"）

    既存列がある場合も、"send_status" と "sent_at" は指定の定数で上書きする。
    元の DataFrame は変更せず、加工後のコピーを返す。
    """
    out = df.copy()

    # 1) 列名変更（存在する場合のみ）
    if "company_name" in out.columns and "recipient_company_name" not in out.columns:
        out = out.rename(columns={"company_name": "recipient_company_name"})

    # 2) client_id 列の用意と設定
    if "client_id" not in out.columns:
        out.loc[:, "client_id"] = ""
    if client_id is not None:
        out.loc[:, "client_id"] = str(client_id)

    # 3) send_status / sent_at を定数で付与（上書き）
    out.loc[:, "send_status"] = send_status_value
    out.loc[:, "sent_at"] = sent_at_value

    return out


# %% [markdown]
# ## BQへ格納

# %%
# from __future__ import annotations

from typing import Iterable, Mapping, Optional
import pandas as pd
from google.cloud import bigquery


def load_sales_list_df_to_bq(
    df: pd.DataFrame,
    *,
    project_id: str,
    dataset_id: str = "dev",
    table_id: str = "sales_list",
    location: str = "asia-northeast1",
    write_disposition: str = "WRITE_APPEND",
    require_all_columns: bool = True,
) -> int:
    """
    Load a DataFrame to BigQuery table `{project_id}.{dataset_id}.{table_id}` with
    schema alignment for the following columns:
      - client_id (STRING, REQUIRED)
      - recipient_company_name (STRING, REQUIRED)
      - hp_url (STRING, REQUIRED)
      - contact_url (STRING, REQUIRED)
      - sales_copy (STRING, REQUIRED)
      - record_created_at (DATETIME, REQUIRED)
      - sent_at (DATETIME, REQUIRED)
      - send_status (STRING, REQUIRED)

    Notes on authentication for VM:
    - If this runs on a GCE/Cloud Run/Composer VM with an attached service account,
      the BigQuery client will automatically use Application Default Credentials (ADC).
      No human login is required if the service account has `roles/bigquery.dataEditor`
      (or appropriate) on the target dataset/table.
    - Locally, you can also set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON.

    Returns: number of rows loaded.
    """
    required_columns = [
        "client_id",
        "recipient_company_name",
        "hp_url",
        "contact_url",
        "sales_copy",
        "record_created_at",
        "sent_at",
        "send_status",
    ]

    # 1) 必須列の存在チェック
    missing = [c for c in required_columns if c not in df.columns]
    if missing and require_all_columns:
        raise ValueError(f"Missing required columns: {missing}")

    # 2) スキーマ順にそろえたコピーを作成（余分列は落とす）
    use_cols = [c for c in required_columns if c in df.columns]
    insert_df = df[use_cols].copy()

    # 3) 型の正規化
    string_cols = [
        "client_id",
        "recipient_company_name",
        "hp_url",
        "contact_url",
        "sales_copy",
        "send_status",
    ]
    for col in string_cols:
        if col in insert_df:
            insert_df[col] = insert_df[col].astype("string").fillna("")

    # DATETIME（tz なし）へ変換。タイムゾーン情報がある場合は日本時間に変換してからtzを除去
    for col in ["record_created_at", "sent_at"]:
        if col in insert_df:
            series = (
                insert_df[col]
                .replace({"None": None})
                .astype("string")
                .where(lambda s: s.str.strip().ne(""), None)
            )
            insert_df[col] = pd.to_datetime(series, errors="coerce")
            
            # タイムゾーン情報がある場合は日本時間に変換してからtzを除去
            if insert_df[col].dt.tz is not None:
                print(f"🕐 タイムゾーン情報を処理中: {col}")
                # 日本時間に変換してからタイムゾーン情報を除去
                insert_df[col] = insert_df[col].dt.tz_convert('Asia/Tokyo').dt.tz_localize(None)
            
            # BigQuery DATETIME は tz なし、NaT は許容しないので最終チェック
            if insert_df[col].isna().any():
                bad_idx = insert_df[col][insert_df[col].isna()].index.tolist()
                raise ValueError(f"Invalid DATETIME in column '{col}' at rows: {bad_idx[:10]} ...")

    table_fqn = f"{project_id}.{dataset_id}.{table_id}"

    client = bigquery.Client(project=project_id, location=location)
    job = client.load_table_from_dataframe(
        insert_df,
        table_fqn,
        job_config=bigquery.LoadJobConfig(write_disposition=write_disposition),
    )
    job.result()
    return len(insert_df)


