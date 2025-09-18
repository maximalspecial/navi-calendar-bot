import os
import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import pytz

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== Config =====
LIQUIPEDIA_URL = "https://liquipedia.net/counterstrike/Natus_Vincere"
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"
# Переводити з UTC? Для Liquipedia частіше вже вказаний UTC/CE(S)T — ми детектимо це по суфіксу, тому залишаємо True (безпечно)
SCRAPED_TIME_IS_UTC = True

READ_TIMEOUT = 45
CONNECT_TIMEOUT = 10
TOTAL_RETRIES = 5
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429,500,502,503,504)

UA_POOL = [
    # Liquipedia просить нормальний UA — залишимо людський
    "navi-calendar-bot/1.0 (+https://github.com/; contact: calendar-bot@navi.example)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
]

TZ = pytz.timezone(TIMEZONE)
TZ_ABBR = {
    # Базові зони, що трапляються на Liquipedia
    "UTC"  : pytz.utc,
    "GMT"  : pytz.utc,
    "CET"  : pytz.timezone("CET"),
    "CEST" : pytz.timezone("CET"),   # CET сам обчислить літній/зимовий
    "EET"  : pytz.timezone("EET"),
    "EEST" : pytz.timezone("EET"),
}

# ===== HTTP =====
def make_session():
    s = requests.Session()
    retry = Retry(
        total=TOTAL_RETRIES, connect=TOTAL_RETRIES, read=TOTAL_RETRIES,
        backoff_factor=BACKOFF_FACTOR, status_forcelist=STATUS_FORCELIST,
        allowed_methods=frozenset(["GET","HEAD"]),
        raise_on_status=False, respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("https://", adapter); s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,uk;q=0.8,ru;q=0.7",
        "Connection": "close",
    })
    return s

def fetch_html(url: str) -> str | None:
    sess = make_session()
    time.sleep(random.uniform(0.3, 1.0))
    try:
        r = sess.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if r.status_code != 200:
            print(f"[WARN] HTTP {r.status_code} for {url}")
            return None
        print(f"[INFO] Fetched {url} ({len(r.text)} bytes)")
        return r.text
    except requests.RequestException as e:
        print(f"[ERROR] Fetch failed {url}: {e}")
        return None

# ===== Parsing Liquipedia =====
MONTHS = {m.lower(): i for i, m in enumerate([
    "", "January","February","March","April","May","June",
    "July","August","September","October","November","December"
])}
ABBR = {"Jan":"January","Feb":"February","Mar":"March","Apr":"April","Jun":"June","Jul":"July","Aug":"August","Sep":"September","Sept":"September","Oct":"October","Nov":"November","Dec":"December","May":"May"}

def _norm_month(name: str) -> str:
    name = (name or "").strip()
    if not name: return ""
    cap = name[0].upper() + name[1:].lower()
    return ABBR.get(cap, cap)

def _parse_liquipedia_datetime(text: str, year_hint: int) -> tuple[datetime, str] | None:
    """
    Очікуємо рядки типу:
    'September 18, 2025 - 18:00 UTC'
    'Sep 18, 2025 - 21:00 CEST'
    'September 18 - 20:00 CEST' (без року → підставимо поточний/наступний)
    Повертає (aware dt у Kyiv, оригінальний суфікс таймзони).
    """
    t = " ".join((text or "").split())
    # Витягуємо Month Day[, Year] - HH:MM TZ
    m = re.search(r'([A-Za-z]{3,9})\s+(\d{1,2})(?:,\s*(\d{4}))?\s*[-–]\s*(\d{1,2}):(\d{2})\s*([A-Za-z]{2,4})', t)
    if not m:
        return None
    mon_name = _norm_month(m.group(1))
    day = int(m.group(2))
    year = int(m.group(3)) if m.group(3) else year_hint
    hh = int(m.group(4)); mm = int(m.group(5))
    tz_abbr = m.group(6).upper()

    month = MONTHS.get(mon_name.lower(), 0)
    if month == 0:
        return None

    # якщо року нема — якщо така дата вже в минулому, +1 рік
    today_kiev = datetime.now(TZ).date()
    if not m.group(3):
        cand = date(today_kiev.year, month, day)
        if cand < today_kiev:
            year = today_kiev.year + 1
        else:
            year = today_kiev.year

    # формуємо aware-час у вхідній зоні
    tz_in = TZ_ABBR.get(tz_abbr, pytz.utc)
    try:
        dt_in = tz_in.localize(datetime(year, month, day, hh, mm))
    except Exception:
        return None

    # в Kyiv
    dt_kyiv = dt_in.astimezone(TZ)
    return dt_kyiv, tz_abbr

def parse_upcoming_from_liquipedia():
    html = fetch_html(LIQUIPEDIA_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    body_text = soup.get_text(" ", strip=True)

    # 1) спробуємо знайти секцію "Upcoming Matches" і взяти перший блок NaVi vs X (Bo3)
    # Візьмемо всі фрагменти навколо "Upcoming Matches" (±1500 символів)
    up_idx = body_text.lower().find("upcoming matches")
    blocks = []
    if up_idx != -1:
        start = max(0, up_idx - 500)
        end = min(len(body_text), up_idx + 3000)
        blocks.append(body_text[start:end])
    else:
        # fallback: беремо увесь текст (Liquipedia стабільна, але на різних мовах/теми можуть мінятись)
        blocks.append(body_text)

    matches = []
    year_hint = datetime.now(TZ).year

    for blk in blocks:
        # Витягуємо пари команд + формат
        # Напр.: "NAVI Natus Vincere vs 9INE (Bo3) September 18, 2025 - 18:00 UTC StarSeries Fall 2025"
        for m in re.finditer(r'([A-Za-z0-9\'\-\.\s]+?)\s+vs\s+([A-Za-z0-9\'\-\.\s]+?)(?:\s*\(Bo\d\))?', blk, flags=re.I):
            team1 = m.group(1).strip()
            team2 = m.group(2).strip()

            # фільтруємо сторонні матчі (на всякий випадок)
            if "natus vincere" not in team1.lower() and "navi" not in team1.lower():
                if "natus vincere" not in team2.lower() and "navi" not in team2.lower():
                    continue

            # шукатимемо поруч формат (Bo3)
            span_start = max(0, m.start() - 200)
            span_end = min(len(blk), m.end() + 200)
            window = blk[span_start:span_end]
            bo = ""
            mbo = re.search(r'\(Bo(\d)\)', window, flags=re.I)
            if mbo:
                bo = f"Bo{mbo.group(1)}"

            # шукатимемо дату/час у ширшому вікні
            span_start = max(0, m.end())
            span_end = min(len(blk), m.end() + 600)
            timewin = blk[span_start:span_end]

            # основний шаблон дати/часу
            dt_parsed = None
            for dt_match in re.finditer(r'([A-Za-z]{3,9}\s+\d{1,2}(?:,\s*\d{4})?\s*[-–]\s*\d{1,2}:\d{2}\s*[A-Za-z]{2,4})', timewin):
                label = dt_match.group(0)
                got = _parse_liquipedia_datetime(label, year_hint)
                if got:
                    dt_kyiv, tz_abbr = got
                    dt_end = dt_kyiv + timedelta(hours=2)
                    matches.append({
                        "summary": f"{team1} vs {team2}" + (f" ({bo})" if bo else ""),
                        "start_dt_str": dt_kyiv.strftime("%Y-%m-%dT%H:%M:%S"),
                        "end_dt_str": dt_end.strftime("%Y-%m-%dT%H:%M:%S"),
                        "link": LIQUIPEDIA_URL,
                        "bo": bo,
                        "note": f"src: Liquipedia ({tz_abbr})"
                    })
                    break  # досить першого збігу для цієї пари

    print(f"[INFO] Liquipedia parsed {len(matches)} upcoming.")
    return matches

# ===== Calendar =====
def create_events(service, matches):
    created = 0
    for m in matches:
        event = {
            "summary": m["summary"],
            "description": f"Auto-added from {LIQUIPEDIA_URL}\nSource: {m.get('note','Liquipedia')}",
            "start": {"dateTime": m["start_dt_str"], "timeZone": TIMEZONE},
            "end":   {"dateTime": m["end_dt_str"],   "timeZone": TIMEZONE},
        }
        print("[DEBUG] Prepared event:", event)
        res = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
        print(f"[CREATED] {m['summary']} → {res.get('htmlLink','')}")
        created += 1
    return created

def main():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing.")
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/calendar"])
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    print(f"[CHECK] Using Liquipedia source. TZ={TIMEZONE}")
    matches = parse_upcoming_from_liquipedia()

    # (не робимо жорстку дедуп — Liquipedia показує небагато івентів; при потребі повернемо)
    if not matches:
        print("[WARN] No upcoming matches parsed from Liquipedia.")
        return

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")

if __name__ == "__main__":
    main()
