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
LP_MATCHES_URL = "https://liquipedia.net/counterstrike/Liquipedia:Matches"
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"
EVENT_DURATION_HOURS = 2
LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "30"))  # щоб не створювати занадто далекі івенти
STRICT_DUP_CHECK = (os.environ.get("STRICT_DUP_CHECK", "true").lower() in ("1","true","yes"))

UA_POOL = [
    "navi-calendar-bot/1.0 (+https://github.com/; contact: calendar-bot@navi.example)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

READ_TIMEOUT = 45
CONNECT_TIMEOUT = 10
TOTAL_RETRIES = 5
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429,500,502,503,504)

TZ_LOCAL = pytz.timezone(TIMEZONE)
TZ_ABBR = {
    "UTC": pytz.utc, "GMT": pytz.utc,
    "CET": pytz.timezone("CET"), "CEST": pytz.timezone("CET"),
    "EET": pytz.timezone("EET"), "EEST": pytz.timezone("EET"),
}

MONTHS = {m.lower(): i for i, m in enumerate([
    "", "January","February","March","April","May","June",
    "July","August","September","October","November","December"
])}
ABBR = {"Jan":"January","Feb":"February","Mar":"March","Apr":"April","Jun":"June","Jul":"July",
        "Aug":"August","Sep":"September","Sept":"September","Oct":"October",
        "Nov":"November","Dec":"December","May":"May"}

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

def _norm_month(name: str) -> str:
    if not name: return ""
    cap = name[0].upper() + name[1:].lower()
    return ABBR.get(cap, cap)

def parse_liquipedia_datetime(label: str) -> datetime | None:
    """
    'September 18, 2025 - 18:00 UTC' → aware dt у Europe/Kyiv
    'Sep 18 - 20:00 CEST' → рік підставимо (поточний або наступний)
    """
    t = " ".join((label or "").split())
    m = re.search(r'([A-Za-z]{3,9})\s+(\d{1,2})(?:,\s*(\d{4}))?\s*[-–]\s*(\d{1,2}):(\d{2})\s*([A-Za-z]{2,4})', t)
    if not m:
        return None
    mon = _norm_month(m.group(1))
    day = int(m.group(2))
    year = int(m.group(3)) if m.group(3) else None
    hh = int(m.group(4)); mm = int(m.group(5))
    tzabbr = m.group(6).upper()

    month = MONTHS.get(mon.lower(), 0)
    if month == 0: return None

    today = datetime.now(TZ_LOCAL).date()
    if year is None:
        y = today.year
        cand = date(y, month, day)
        if cand < today:
            y = y + 1
    else:
        y = year

    tz_in = TZ_ABBR.get(tzabbr, pytz.utc)
    try:
        dt_in = tz_in.localize(datetime(y, month, day, hh, mm))
    except Exception:
        return None
    return dt_in.astimezone(TZ_LOCAL)

def parse_upcoming_from_liquipedia_matches():
    """
    Скануємо агрегатор і збираємо лише матчі з участю NAVI/Natus Vincere.
    """
    html = fetch_html(LP_MATCHES_URL)
    if not html:
        return []

    # Пошукаємо всі появи 'NAVI' або 'Natus Vincere' і навколо кожної вікном дістанемо дані
    matches = []
    navip = list(re.finditer(r'(?:\bNAVI\b|Natus\s+Vincere)', html, flags=re.I))
    print(f"[INFO] NAVI mentions on page: {len(navip)}")

    for i, m in enumerate(navip, start=1):
        start = max(0, m.start() - 800)
        end = min(len(html), m.end() + 1200)
        chunk = html[start:end]

        # Команди: в обидва боки — 'NAVI ... vs Opp' або 'Opp vs NAVI ...'
        m_vs = (re.search(r'([A-Za-z0-9\'\-\.\s]{2,40})\s+vs\s+([A-Za-z0-9\'\-\.\s]{2,40})', chunk, flags=re.I) or
                re.search(r'([A-Za-z0-9\'\-\.\s]{2,40})\s+vs\s+([A-Za-z0-9\'\-\.\s]{2,40})', chunk[::-1], flags=re.I))
        if not m_vs:
            continue
        # беремо з прямого пошуку (розворот використовувався як запасний)
        m_vs = re.search(r'([A-Za-z0-9\'\-\.\s]{2,40})\s+vs\s+([A-Za-z0-9\'\-\.\s]{2,40})', chunk, flags=re.I)
        team1, team2 = m_vs.group(1).strip(), m_vs.group(2).strip()

        # Переконаємось, що одна з команд — NAVI/Natus Vincere
        if not (re.search(r'(?:\bNAVI\b|Natus\s+Vincere)', team1, flags=re.I) or
                re.search(r'(?:\bNAVI\b|Natus\s+Vincere)', team2, flags=re.I)):
            # іноді 'NAVI' стоїть не в самих назвах команд, а поруч у HTML — тоді форсуємо
            if re.search(r'(?:\bNAVI\b|Natus\s+Vincere)', chunk, flags=re.I):
                if "Natus Vincere" not in (team1 + team2):
                    if re.search(r'(?:\bNAVI\b|Natus\s+Vincere)', team1, flags=re.I):
                        pass
                    elif re.search(r'(?:\bNAVI\b|Natus\s+Vincere)', team2, flags=re.I):
                        pass
                    else:
                        # якщо все ж ні — підставимо NAVI як одну з команд
                        if "Natus Vincere" not in team1 and "NAVI" not in team1.upper():
                            team1 = "Natus Vincere"
            else:
                continue

        # Формат: (Bo3) поруч
        mbo = re.search(r'\(Bo(\d)\)', chunk, flags=re.I)
        bo = f"Bo{mbo.group(1)}" if mbo else ""

        # Дата/час: найближчий шаблон 'Month D[, YYYY] - HH:MM TZ'
        m_dt = re.search(r'([A-Za-z]{3,9}\s+\d{1,2}(?:,\s*\d{4})?\s*[-–]\s*\d{1,2}:\d{2}\s*[A-Za-z]{2,4})', chunk)
        if not m_dt:
            continue
        dt_local = parse_liquipedia_datetime(m_dt.group(1))
        if not dt_local:
            continue

        # Фільтр за горизонтом
        now_local = datetime.now(TZ_LOCAL)
        if dt_local < now_local - timedelta(hours=3):
            continue
        if dt_local > now_local + timedelta(days=LOOKAHEAD_DAYS):
            continue

        end_local = dt_local + timedelta(hours=EVENT_DURATION_HOURS)
        summary = f"{team1} vs {team2}" + (f" ({bo})" if bo else "")
        matches.append({
            "summary": summary,
            "start_dt_str": dt_local.strftime("%Y-%m-%dT%H:%M:%S"),
            "end_dt_str": end_local.strftime("%Y-%m-%dT%H:%M:%S"),
            "link": LP_MATCHES_URL,
        })
        print(f"[OK] Found: {summary} @ {dt_local.isoformat()} ({TIMEZONE})")

    print(f"[INFO] Total NAVI matches parsed: {len(matches)}")
    return matches

def has_duplicate_event(service, calendar_id, start_dt, summary):
    time_min = (start_dt - timedelta(hours=3)).isoformat()
    time_max = (start_dt + timedelta(hours=3)).isoformat()
    items = service.events().list(
        calendarId=calendar_id, timeMin=time_min, timeMax=time_max,
        singleEvents=True, orderBy="startTime", q=" vs ".join(summary.split(" vs ")[:2])
    ).execute().get("items", [])
    for ev in items:
        if ev.get("summary","").startswith(" vs ".join(summary.split(" vs ")[:2])):
            print(f"[SKIP] Duplicate near: {ev.get('summary')} at {ev.get('start',{}).get('dateTime')}")
            return True
    return False

def create_events(service, matches):
    created = 0
    for m in matches:
        start_dt = TZ_LOCAL.localize(datetime.strptime(m["start_dt_str"], "%Y-%m-%dT%H:%M:%S"))
        if STRICT_DUP_CHECK and has_duplicate_event(service, CALENDAR_ID, start_dt, m["summary"]):
            continue
        event = {
            "summary": m["summary"],
            "description": f"Auto-added from {LP_MATCHES_URL}",
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
    creds = Credentials.from_service_account_info(json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/calendar"])
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    print(f"[CHECK] Source: Liquipedia:Matches; TZ={TIMEZONE}")
    matches = parse_upcoming_from_liquipedia_matches()
    if not matches:
        print("[WARN] No NAVI matches parsed from Liquipedia:Matches.")
        return

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")

if __name__ == "__main__":
    main()
