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

# На CI HTML часто віддає UTC-час → конвертуємо в Kyiv (true = UTC→Kyiv)
SCRAPED_TIME_IS_UTC = (os.environ.get("SCRAPED_TIME_IS_UTC", "true").lower() in ("1","true","yes"))

# На час діагностики відключимо перевірку дублів (щоб подія точно створилась)
STRICT_DUP_CHECK = (os.environ.get("STRICT_DUP_CHECK", "false").lower() in ("1","true","yes"))

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

# ===== HTTP helpers =====
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

# ===== parsing utils =====
def normalize_month_day(text:str) -> str:
    t = (text or "").strip().replace(".", " ")
    t = re.sub(r"\s+"," ", t)
    parts = t.split()
    if not parts: return ""
    m = parts[0].lower()
    if m in MONTHS_EN:
        parts[0] = MONTHS_EN[m]
    return " ".join(parts).title()  # 'sep 18' -> 'Sep 18'

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
    m_time = re.search(r'\b(\d{1,2}:\d{2})\b', text)
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

# ===== main list parser =====
def parse_rows():
    html = fetch_html(BO3_URL)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    # Забираємо будь-які рядки матчів
    rows = soup.select(".table-row, [class*='table-row']")
    print(f"[INFO] Row count = {len(rows)}")

    tz_local = pytz.timezone(TIMEZONE)
    matches = []
    candidates_log = []

    for idx, row in enumerate(rows, start=1):
        # 1) посилання на матч (максимально вільний селектор)
        a = row.select_one('a[href*="/matches/"]')
        href = a.get("href","") if a else ""
        link = "https://bo3.gg" + href if href else BO3_URL  # навіть без лінку створимо подію

        # 2) команди (або з .team-name, або з plain text)
        teams = [el.get_text(strip=True) for el in row.select(".team-name")]
        if len(teams) >= 2:
            team1, team2 = teams[0], teams[1]
        else:
            # спроба витягти з тексту
            raw_text = row.get_text(" ", strip=True)
            m_vs = re.search(r'([A-Za-z0-9 .\-]+)\s+vs\s+([A-Za-z0-9 .\-]+)', raw_text, flags=re.I)
            if m_vs:
                team1, team2 = m_vs.group(1).strip(), m_vs.group(2).strip()
            else:
                # якщо не бачимо NaVi у рядку — пропускаємо
                if "Natus Vincere" not in raw_text and "NAVI" not in raw_text.upper():
                    continue
                team1, team2 = "Natus Vincere", "TBD"

        # 3) формат (Bo3)
        bo_el = row.select_one(".bo-type")
        bo = bo_el.get_text(strip=True) if bo_el else ""
        # 4) турнір
        tour_el = row.select_one(".tournament-name")
        tournament = tour_el.get_text(strip=True) if tour_el else ""

        # 5) час/дата: спочатку DOM...
        time_el = row.select_one(".date .time")
        date_el = row.select_one(".date")
        time_text = time_el.get_text(strip=True) if time_el else None
        date_text = None
        if date_el:
            raw = date_el.get_text(" ", strip=True)
            if time_text: raw = raw.replace(time_text,"").strip()
            date_text = normalize_month_day(raw) if raw else None

        # ... якщо нема — з plain text усього рядка
        if not (date_text and time_text):
            raw_all = row.get_text(" ", strip=True)
            maybe = parse_hhmm_and_md_from_text(raw_all)
            if maybe:
                date_text, time_text = maybe
                print(f"[INFO] Row {idx}: extracted from raw text → date='{date_text}', time='{time_text}'")

        # збережемо кандидат-лог
        candidates_log.append((idx, team1, team2, bo, tournament, date_text, time_text, href))

        # якщо все ще бракує часу/дати — підемо в детальну (якщо є URL)
        start_local = None
        tz_local = pytz.timezone(TIMEZONE)
        if date_text and time_text:
            parsed_md = None
            for fmt in ("%b %d","%B %d"):
                try:
                    parsed_md = datetime.strptime(date_text, fmt); break
                except ValueError: continue
            if parsed_md:
                try:
                    y = infer_year(href, parsed_md.month, parsed_md.day, tz_local)
                    hh, mm = time_text.split(":")
                    start_naive = datetime(y, parsed_md.month, parsed_md.day, int(hh), int(mm))
                    start_local = (pytz.utc.localize(start_naive).astimezone(tz_local)
                                   if SCRAPED_TIME_IS_UTC else tz_local.localize(start_naive))
                except Exception as e:
                    print(f"[WARN] Row {idx}: time compose failed: {e}")

        if start_local is None and href:
            start_local = parse_detail_datetime(link, href, tz_local)
            print(f"[FALLBACK] Row {idx}: detail parsed = {bool(start_local)}; url={link}")

        if start_local is None:
            # Ні часу/дати — пропускаємо
            continue

        end_local = start_local + timedelta(hours=2)
        start_dt_str = start_local.strftime("%Y-%m-%dT%H:%M:%S")
        end_dt_str   = end_local.strftime("%Y-%m-%dT%H:%M:%S")

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
        print(f"[OK] Row {idx} parsed: {summary} @ {start_dt_str} ({TIMEZONE}) → {link}")

    # друк усіх кандидатів (для діагностики)
    print("[CANDIDATES]")
    for idx, t1, t2, bo, tour, d, tm, href in candidates_log:
        print(f"  Row {idx}: teams=('{t1}','{t2}') bo='{bo}' tour='{tour}' date='{d}' time='{tm}' href='{href}'")

    return matches

# ===== Calendar helpers =====
def has_duplicate_event(service, calendar_id, start_dt, summary):
    # розширене вікно (±3h)
    time_min = (start_dt - timedelta(hours=3)).isoformat()
    time_max = (start_dt + timedelta(hours=3)).isoformat()
    items = service.events().list(
        calendarId=calendar_id, timeMin=time_min, timeMax=time_max,
        singleEvents=True, orderBy="startTime",
        q=" vs ".join(summary.split(" vs ")[:2])
    ).execute().get("items", [])
    for ev in items:
        if ev.get("summary","").startswith(" vs ".join(summary.split(" vs ")[:2])):
            print(f"[SKIP] Duplicate around this time: {ev.get('summary')} at {ev.get('start',{}).get('dateTime')}")
            return True
    return False

def create_events(service, matches):
    tz_local = pytz.timezone(TIMEZONE)
    created = 0
    for m in matches:
        start_dt = tz_local.localize(datetime.strptime(m["start_dt_str"], "%Y-%m-%dT%H:%M:%S"))
        if STRICT_DUP_CHECK:
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

# ===== entrypoint =====
def main():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json: raise RuntimeError("GOOGLE_CREDENTIALS_JSON is missing.")
    info = json.loads(creds_json)
    print(f"[CHECK] SCRAPED_TIME_IS_UTC={SCRAPED_TIME_IS_UTC}, STRICT_DUP_CHECK={STRICT_DUP_CHECK}")

    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/calendar"])
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    matches = parse_rows()
    print(f"[INFO] Parsed {len(matches)} matches total.")
    if not matches:
        print("[WARN] Nothing parsed → nothing to create.")
        return

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")

if __name__ == "__main__":
    main()
