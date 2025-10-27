# -*- coding: utf-8 -*-
"""
QKB lead finder — Render-ready Flask app
- Scrapes https://format.qkb.gov.al/kerko-per-subjekt/ by posting keywords into "sektoriIVeprimtarise"
- Normalizes rows (NIPT, name, trade_name, sector, owners, etc.)
- Optionally fetches each subject's "simple" PDF and extracts email/phone
Deploy on Render:
  - requirements.txt
  - Procfile          (web: gunicorn app:app)
  - app.py            (this file)
"""
import os
import re
import io
import csv
import ast
import json
import time
import base64
import pathlib
import tempfile
from datetime import datetime
from typing import List, Tuple, Dict, Any

import requests
from requests.adapters import HTTPAdapter, Retry
from flask import Flask, request, Response, send_file, redirect
import pandas as pd
from bs4 import BeautifulSoup

try:
    import pdfplumber  # heavy, but works on Render
except Exception as e:
    pdfplumber = None

# -----------------------
# Config
# -----------------------
DEFAULT_CITY = "Tiranë"
SEARCH_URL = "https://format.qkb.gov.al/kerko-per-subjekt/"
DOC_URL = "https://format.qkb.gov.al/wp-content/themes/twentytwentyfive-child/modules/search/national-registry/subject/search-for-subject-get-documents.php"
EXPORT_DIR = os.environ.get("EXPORT_DIR", "exports")
os.makedirs(EXPORT_DIR, exist_ok=True)

# Conservative headers + UA
BASE_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://format.qkb.gov.al",
    "Referer": "https://format.qkb.gov.al/kerko-per-subjekt/",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (compatible; QKB-Scraper/1.0; +https://abissnet.cloud)",
}

# Robust regex for JSON.parse("...") pattern used by the site
JSON_PARSE_RX = re.compile(r'JSON\\.parse\\(\\s*([\"\'])([\\s\\S]*?)\\1\\s*\\)')

KEYWORDS = [
    # Gaming / PlayStation / LAN
    "gaming", "sallë gaming", "sallë lojrash", "lan center", "cybercafe", "internet cafe",
    "internet café", "playstation", "sallë playstation", "ps4 lounge", "ps5 lounge",
    "gaming lounge", "gaming hall", "gaming room", "videogames shop", "console gaming",
    "arcade", "arcade hall", "esports", "e-sport center", "turne lojrash",

    # PC halls / Internet cafes
    "sallë pc", "pc hall", "pc room", "qendër interneti", "akses internet", "kompjuter center",
    "pc rental", "pc gaming", "kompjuter publik",

    # Barber / Beauty
    "berber", "barber shop", "barbershop", "salon berber", "sallon flokësh", "hair salon",
    "sallon bukurie", "estetike", "sallon kozmetike",

    # Call centers / BPO / Support
    "call center", "qendër thirrjesh", "customer service center", "customer care",
    "customer support", "suport klienti", "contact center", "helpdesk", "bpo", "outsourcing",
    "back office", "telemarketing", "technical support center",

    # Coworking / Shared offices / Hubs
    "coworking", "co-working", "cowork space", "shared office", "hapësirë bashkëpunimi",
    "business hub", "startup hub", "incubator", "accelerator", "studio office", "office rental",
    "flex office", "hotdesk", "business center", "innovation hub",

    # Taxi companies with ops centers
    "kompani taksish", "kompani taksi", "operim taksi", "taxi dispatch", "dispatch center",
    "qendër operative", "fleet management", "fleet operations", "call center taksi",

    # Tech / ICT companies
    "kompani it", "it services", "software house", "zhvillim software", "dev shop",
    "web development", "mobile development", "system integrator", "network integrator",
    "ict services", "teknologji informacioni", "cloud services", "cloud provider",
    "hosting provider", "managed services", "msp", "data center", "datacenter", "colocation",

    # Hosting / VPS / Server / Maintenance
    "host", "hosting", "web hosting", "vps", "vps hosting", "server hosting", "server farm",
    "server maintenance", "mirëmbajtje serverash", "cloud hosting", "dedicated servers",
    "rack space", "server room",

    # Dark fiber / Dedicated internet / Bandwidth
    "dark fiber", "fibra optike", "fiber optic provider", "infra fiber", "internet i dedikuar",
    "dedicated internet", "bandwidth provider", "leased line", "lambda services",
    "metro ethernet", "fibra për biznes",

    # DDoS / Security
    "ddos protection", "mbrojtje ddos", "security services", "cybersecurity", "managed security",
    "firewall services", "mitigation services", "anti-ddos", "security operations center",
    "soc", "incident response",

    # Telephony / VoIP / Fixed lines
    "voip", "voip provider", "voip services", "telefonia fikse", "telefoni fiks",
    "pbx provider", "hosted pbx", "virtual numbers", "sip trunk", "ip telephony",
    "telephony services", "call routing",

    # High-bandwidth verticals
    "studio radio", "studio tv", "qendër media", "media center", "streaming studio",
    "post-produksion", "post produksion", "rendering farm", "video hosting",
    "event operator", "data analytics", "financial trading", "crypto mining",
    "research lab", "e-learning center", "edukim online",

    # Long-tail të dobishme
    "sallë gaming për events", "e-sport arena", "lan party venue", "ps5 playroom",
    "pc gaming rental", "internet cafe with pcs", "barber studio", "modern barber",
    "inbound call center", "outbound call center", "customer care center",
    "shared workspace for startups", "meeting room rental", "virtual office tiranë",
    "taxi dispatch center", "fleet operations tiranë", "web agency tiranë",
    "cloud hosting provider tiranë", "dedicated fiber business", "dark fiber lease",
    "server maintenance contract", "managed voip services",
]

app = Flask(__name__)

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(BASE_HEADERS)
    return s

def _split_people(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(";") if x.strip()]

def parse_rows_from_response(html_text: str) -> List[Dict[str, Any]]:
    """
    The QKB endpoint returns HTML with a JS 'JSON.parse("...")'. Extract safely, unescape and loads.
    """
    m = JSON_PARSE_RX.search(html_text or "")
    if not m:
        raise RuntimeError("Embedded JSON not found via JSON.parse(...)")
    quote = m.group(1)
    inner = m.group(2)
    # Convert JS string literal -> Python string (unescape) using ast.literal_eval on a quoted string
    json_text = ast.literal_eval(quote + inner + quote)
    rows = json.loads(json_text)
    # normalize a few fields
    for r in rows:
        r["registered_at"] = (r.get("dataERegjistrimit") or "").replace("\\/", "/")
        r["owners"] = _split_people(r.get("adminOrtakAksionar"))
    return rows

def normalize_dataframe(rows: List[Dict[str, Any]], keyword: str) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame([{"_keyword": keyword}]).assign(nipt=pd.NA, name=pd.NA)
    df = df.rename(columns={
        "nipti": "nipt",
        "emriISubjektit": "name",
        "emriTregtar": "trade_name",
        "sektoriIVeprimtarise": "sector",
        "formaLigjore": "legal_form",
        "statusiISubjektit": "status",
        "qyteti": "city",
        "shtetesia": "citizenship",
    })
    if "registered_at" in df.columns:
        df["registered_at"] = pd.to_datetime(df["registered_at"], format="%d/%m/%Y", errors="coerce")
    cols = ["nipt","name","trade_name","sector","owners","legal_form","status","city","citizenship","registered_at"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols].assign(_keyword=keyword)

def search_keyword(session: requests.Session, kw: str, city: str, qarku: str = "") -> pd.DataFrame:
    payload = {
        "orderColumn": "0",
        "orderDir": "asc",
        "nipt": "",
        "emriISubjektit": "",
        "emriTregtar": "",
        "formeLigjore": "",
        "pronesia": "",
        "dataNga": "",
        "dataNe": "",
        "numriId": "",
        "administrator": "",
        "aksionerOrtak": "",
        "sektoriIVeprimtarise": kw,
        "qarku": qarku,
        "qyteti": city,
        "adresa": "",
    }
    r = session.post(SEARCH_URL, data=payload, timeout=40)
    r.raise_for_status()
    rows = parse_rows_from_response(r.text)
    return normalize_dataframe(rows, kw)

def extract_contacts_for_nipt(session: requests.Session, nipt: str) -> Tuple[str, str]:
    """
    Downloads the 'simple' PDF for a NIPT and tries to extract E-mail and Telefon from the text.
    Returns (email, phone) — any may be None if not found.
    """
    if not nipt:
        return (None, None)
    try:
        payload = {"nipt": nipt, "docType": "simple"}
        r = session.post(DOC_URL, data=payload, timeout=60)
        r.raise_for_status()
        data = r.json() if r.headers.get("Content-Type","").startswith("application/json") else json.loads(r.text)
        pdf_bytes = base64.b64decode(data.get("data",""))
        # parse PDF text
        email, phone = None, None
        if pdfplumber:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    pages_text.append(page.extract_text() or "")
                combined = "\\n".join(pages_text)
                # tolerant patterns
                m_email = re.search(r"E-?mail\\s*:?\\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,})", combined, flags=re.I)
                m_phone = re.search(r"Telefon\\s*:?\\s*([0-9 +()\\-]+)", combined, flags=re.I)
                email = m_email.group(1).strip() if m_email else None
                phone = m_phone.group(1).strip() if m_phone else None
        return (email, phone)
    except Exception:
        return (None, None)

def run_scrape(keywords: List[str], city: str, qarku: str, delay: float, contacts: bool, max_contacts: int) -> Tuple[pd.DataFrame, str]:
    s = make_session()
    frames = []
    for i, kw in enumerate(keywords, 1):
        try:
            df_kw = search_keyword(s, kw, city, qarku)
            frames.append(df_kw)
        except Exception as e:
            frames.append(pd.DataFrame([{"_keyword": kw, "_error": str(e)}]))
        time.sleep(max(0.0, delay))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if "nipt" in df.columns:
        df = df.drop_duplicates(subset=["nipt", "_keyword"], keep="first")
    # optional contacts
    if contacts and not df.empty and "nipt" in df.columns:
        emails, phones = [], []
        # cap number of PDFs to avoid timeouts
        n = 0
        for nipt in df["nipt"].tolist():
            if nipt and n < max_contacts:
                e, p = extract_contacts_for_nipt(s, nipt)
                emails.append(e); phones.append(p)
                n += 1
            else:
                emails.append(None); phones.append(None)
        df["email"] = emails
        df["telefon"] = phones
    # save
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"qkb_export_{city.replace(' ', '_')}_{ts}.csv"
    fpath = os.path.join(EXPORT_DIR, fname)
    df.to_csv(fpath, index=False, encoding="utf-8")
    return df, fpath

def html_page(body: str) -> str:
    return f"""<!doctype html>
<html lang="sq">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QKB Lead Finder</title>
  <style>
    :root {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, 'Helvetica Neue', Arial, 'Noto Sans', 'Liberation Sans', sans-serif; }}
    body {{ margin: 24px; }}
    .wrap {{ max-width: 1100px; margin: 0 auto; }}
    h1 {{ font-size: 28px; margin-bottom: 0.2rem; }}
    p.meta {{ color: #666; margin-top: 0; }}
    form input, form select, form textarea {{ width: 100%; padding: 10px; margin-top: 6px; margin-bottom: 14px; }}
    button {{ padding: 10px 16px; cursor: pointer; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: repeat(4, 1fr); }}
    .grid > div {{ border: 1px solid #e3e3e3; border-radius: 8px; padding: 10px; }}
    table {{ width:100%; border-collapse: collapse; margin-top: 16px; }}
    th, td {{ border-bottom: 1px solid #eee; text-align: left; padding: 8px; font-size: 14px; }}
    code, pre {{ background: #f7f7f8; padding: 2px 6px; border-radius: 6px; }}
    .note {{ background: #fff8e1; border: 1px solid #ffe082; padding: 10px; border-radius: 8px; }}
    .ok {{ color: #0a7; }}
  </style>
</head>
<body><div class="wrap">
{body}
</div></body>
</html>"""

@app.route("/", methods=["GET"])
def index():
    default_kw = ", ".join(KEYWORDS[:20]) + ", …"  # preview only
    body = f"""
    <h1>QKB Lead Finder</h1>
    <p class="meta">Scrape subjekte në QKB sipas fjalëkyçeve në <code>sektori i veprimtarisë</code>. Default qyteti: <b>{DEFAULT_CITY}</b>.</p>
    <form method="POST" action="/scrape">
      <label>Qyteti</label>
      <input name="city" value="{DEFAULT_CITY}" />

      <label>Qarku (opsionale)</label>
      <input name="qarku" placeholder="p.sh. Tiranë" />

      <label>Fjalëkyçet (një për rresht) – lëre bosh për listën time të paracaktuar</label>
      <textarea name="keywords" rows="6" placeholder="{default_kw}"></textarea>

      <div class="grid">
        <div>
          <label>Delay ndërmjet kërkesave (sekonda)</label>
          <input name="delay" type="number" step="0.1" value="0.4" />
        </div>
        <div>
          <label>Ekstrakto kontaktet (PDF)</label>
          <select name="contacts">
            <option value="no" selected>Jo</option>
            <option value="yes">Po</option>
          </select>
        </div>
        <div>
          <label>Maks. subjekte për kontakt (PDF)</label>
          <input name="max_contacts" type="number" value="50" />
        </div>
        <div>
          <label>Dedup global (NIPT)</label>
          <select name="dedup">
            <option value="yes" selected>Po</option>
            <option value="no">Jo</option>
          </select>
        </div>
      </div>
      <button type="submit">Start</button>
    </form>
    <div class="note">
      <b>Shënim:</b> Mos e tepro me kërkesa. Mbaj një delay ≥ 0.3s. PDF-të janë të rënda – limito <i>Maks. subjekte për kontakt</i>.
    </div>
    """
    return html_page(body)

@app.route("/scrape", methods=["POST"])
def scrape():
    city = (request.form.get("city") or DEFAULT_CITY).strip()
    qarku = (request.form.get("qarku") or "").strip()
    delay = float(request.form.get("delay") or 0.4)
    contacts = (request.form.get("contacts") or "no").lower() == "yes"
    max_contacts = int(request.form.get("max_contacts") or 50)
    dedup = (request.form.get("dedup") or "yes").lower() == "yes"

    raw_kw = (request.form.get("keywords") or "").strip()
    if raw_kw:
        kws = [k.strip() for k in raw_kw.splitlines() if k.strip()]
    else:
        kws = KEYWORDS

    df, fpath = run_scrape(kws, city, qarku, delay, contacts, max_contacts)
    # optional global dedup by NIPT only (collapse multiple keywords per company)
    if dedup and "nipt" in df.columns:
        df = df.sort_values(["nipt", "_keyword"]).drop_duplicates(subset=["nipt"], keep="first")
        dedup_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fpath = os.path.join(EXPORT_DIR, f"qkb_export_{city.replace(' ', '_')}_{dedup_ts}_dedup.csv")
        df.to_csv(fpath, index=False, encoding="utf-8")

    # HTML summary
    head = df.head(25).to_html(index=False, justify="left")
    size = os.path.getsize(fpath)
    body = f"""
    <h1>OK</h1>
    <p class="ok">Gati. Rreshta: <b>{len(df)}</b>. <a href="/download?path={fpath}">Shkarko CSV</a> ({size} bytes)</p>
    <details open><summary>Preview (25)</summary>{head}</details>
    <p><a href="/">↩︎ Kthehu</a></p>
    """
    return html_page(body)

@app.route("/download", methods=["GET"])
def download():
    # naive path pass-through (we only serve from EXPORT_DIR)
    fpath = request.args.get("path", "")
    fpath = os.path.abspath(fpath)
    if not fpath or not fpath.startswith(os.path.abspath(EXPORT_DIR)):
        return Response("invalid path", status=400)
    if not os.path.exists(fpath):
        return Response("not found", status=404)
    return send_file(fpath, as_attachment=True, download_name=os.path.basename(fpath), mimetype="text/csv")

if __name__ == "__main__":
    # Local dev
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), debug=True)
