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
BO3_URL = "https://bo3.gg/teams/natus-vincere/matches"
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"
SCRAPED_TIME_IS_UTC = (os.environ.get("SCRAPED_TIME_IS_UTC", "true").lower() in ("1","true","yes"))

READ_TIMEOUT = 45
CONNECT_TIMEOUT = 10
TOTAL_RETRIES = 5
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429,500,502,503,504)

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
]

MONTHS_EN = {
    "jan":"Jan","feb":"Feb","mar":"Mar","apr":"Apr","may":"May","jun":"Jun",
    "jul":"Jul","aug":"Aug","sep":"Sep","sept":"Sep","oct":"Oct","nov":"Nov","dec":"Dec",
    "january":"January","february":"February","march":"March","april":"April","june":"June",
    "july":"July","august":"August","september":"September","october":"October","november":"November","december":"December",
}

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

def fetch_html(url):
    sess = make_session()
    time.sleep(random.uniform(0.3,1.2))
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

def normalize_month_day(text:str) -> str:
    t = (text or "").strip().replace(".", " ")
    t = re.sub(r"\s+"," ", t)
    # ручна нормалізація, щоб 'sep' → 'Sep'
    parts = t.split()
    if not parts: return ""
    m = parts[0].lower()
    if m in MONTHS_EN:
        parts[0] = MONTHS_EN[m]
    return " ".join(parts).title()  # зробить 'Sep 18' / 'September 18'

def infer_year(href:str, month:int, day:int, tz) -> int:
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})$", href or "")
    if m: return int(m.group(3))
    today = datetime.now(tz).date()
    cand = date(today.year, month, day)
    return today.year if cand >= today else today.year + 1

def parse_iso_from_detail(html:str):
    m = re.search(r'"startDate"\s*:\s*"([^"]+)"', html, flags=re.I)
    iso = None
    if m: iso = m.group(1)
    else:
        m = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+\-]\d{2}:\d{2}))', html)
        if m: iso = m.group(1)
    if not iso: return None
    iso = iso.replace("Z","+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except Exception:
        return None
    if dt.tzinfo is None: dt = pytz.utc.localize(dt)
    return dt.astimezone(pytz.utc)

def parse_hhmm_and_md_from_text(text:str):
    # час
    m_time = re.search(r'\b(\d{1,2}:\d{2})\b', text)
    # дата як Month Day
    m_date = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2}\b', text, flags=re.I)
    if not (m_time and m_date): return None
    return normalize_month_day(m_date.group(0)), m_time.group(1)

def parse_detail_datetime(detail_url, href_tail, tz_local):
    html = fetch_html(detail_url)
    if not html: return None
    iso = parse_iso_from_detail(html)
    if iso:
        return iso.astimezone(tz_local)
    fallback = parse_hhmm_and_md_from_text(html)
    if not fallback: return None
    date_text, time_text = fallback
    parsed_md = None
    for fmt in ("%b %d","%B %d"):
        try:
            parsed_md = datetime.strptime(date_text, fmt); break
        except ValueError: continue
    if not parsed_md: return None
    y = infer_year(href_tail, parsed_md.month, parsed_md.day, tz_local)
    hh, mm = time_text.split(":")
    start_naive = datetime(y, parsed_md.month, parsed_md.day, int(hh), int(mm))
    return (pytz.utc.localize(start_naive).astimezone(tz_local)
            if SCRAPED_TIME_IS_UTC else tz_local.localize(start_naive))

def parse_rows():
    html = fetch_html(BO3_URL)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select(".table-row")
    print(f"[INFO] .table-row count = {len(rows)}")

    tz_local = pytz.timezone(TIMEZONE)
    matches = []

    for idx, row in enumerate(rows, start=1):
        # посилання (будь-яке на матч)
        a = row.select_one('a[href*="/matches/"]')
        if not a:
            print(f"[DEBUG] Row {idx}: no match link -> skip")
            continue
        href = a.get("href","")
        link = "https://bo3.gg" + href

        # команди
        teams = [el.get_text(strip=True) for el in row.select(".team-name")]
        team1, team2 = ("Natus Vincere","TBD")
        if len(teams) >= 2: team1, team2 = teams[0], teams[1]
        elif len(teams) == 1: team1 = teams[0]

        # формат
        bo_el = row.select_one(".bo-type")
        bo = bo_el.get_text(strip=True) if bo_el else ""

        # турнір
        tour_el = row.select_one(".tournament-name")
        tournament = tour_el.get_text(strip=True) if tour_el else ""

        # час/дата з DOM або з plain-text
        time_el = row.select_one(".date .time")
        date_el = row.select_one(".date")
        time_text = time_el.get_text(strip=True) if time_el else None
        date_text = None
        if date_el:
            raw = date_el.get_text(" ", strip=True)
            if time_text: raw = raw.replace(time_text,"").strip()
            date_text = normalize_month_day(raw) if raw else None

        if not (date_text and time_text):
            # спробуємо з усього тексту рядка
            raw_text = row.get_text(" ", strip=True)
            maybe = parse_hhmm_and_md_from_text(raw_text)
            if maybe:
                date_text, time_text = maybe
                print(f"[INFO] Row {idx}: extracted from raw text -> date='{date_text}', time='{time_text}'")

        start_local = None
        if date_text and time_text:
            parsed_md = None
            for fmt in ("%b %d","%B %d"):
                try:
                    parsed_md = datetime.strptime(date_text, fmt); break
                except ValueError: continue
            if parsed_md:
                y = infer_year(href, parsed_md.month, parsed_md.day, tz_local)
                try:
                    hh, mm = time_text.split(":")
                    start_naive = datetime(y, parsed_md.month, parsed_md.day, int(hh), int(mm))
                    start_local = (pytz.utc.localize(start_naive).astimezone(tz_local)
                                   if SCRAPED_TIME_IS_UTC else tz_local.localize(start_naive))
                except Exception as e:
                    print(f"[WARN] Row {idx}: time build failed: {e}")

        if start_local is None:
            start_local = parse_detail_datetime(link, href, tz_local)
            print(f"[FALLBACK] Row {idx}: detail parsed = {bool(start_local)}; url={link}")

        if start_local is None:
            print(f"[SKIP] Row {idx}: no datetime parsed; link={link}")
            continue

        end_local = start_local + timedelta(hours=2)
        start_dt_str = start_local.strftime("%Y-%m-%dT%H:%M:%S")
        end_dt_str = end_local.strftime("%Y-%m-%dT%H:%M:%S")

        summary = f"{team1} vs {team2}" + (f" ({bo})" if bo else "")
        if tournament: summary += f" — {tournament}"

        matches.append({
            "summary": summary,
            "start_dt_str": start_dt_str,
            "end_dt_str": end_dt_str,
            "link": link,
            "tournament": tournament,
            "bo": bo
        })
        print(f"[OK] Row {idx} parsed: {summary} @ {start_dt_str} ({TIMEZONE})")

    return matches

def has_duplicate_event(service, calendar_id, start_dt, summary):
    # розширимо вікно до 6 годин (раптом змінився час)
    time_min = (start_dt - timedelta(hours=3)).isoformat()
    time_max = (start_dt + timedelta(hours=3)).isoformat()
    items = service.events().list(
        calendarId=calendar_id, timeMin=time_min, timeMax=time_max,
        singleEvents=True, orderBy="startTime",
        q=" vs ".join(summary.split(" vs ")[:2])  # тільки команди
    ).execute().get("items", [])
    for ev in items:
        if ev.get("summary","").startswith((" vs ".join(summary.split(" vs ")[:2]))):
            print(f"[SKIP] Duplicate around this time: {ev.get('summary')}")
            return True
    return False

def create_events(service, matches):
    tz_local = pytz.timezone(TIMEZONE)
    created = 0
    for m in matches:
        start_dt = tz_local.localize(datetime.strptime(m["start_dt_str"], "%Y-%m-%dT%H:%M:%S"))
        if has_duplicate_event(service, CALENDAR_ID, start_dt, m["summary"]):
            continue
        event = {
            "summary": m["summary"],
            "description": f"Auto-added from {BO3_URL}\nMatch page: {m['link']}",
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
    if not creds_json: raise RuntimeError("GOOGLE_CREDENTIALS_JSON is missing.")
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/calendar"])
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    matches = parse_rows()
    print(f"[INFO] Parsed {len(matches)} matches total.")
    if not matches:
        print("[WARN] Nothing to create this run."); return

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")

if __name__ == "__main__":
    main()
