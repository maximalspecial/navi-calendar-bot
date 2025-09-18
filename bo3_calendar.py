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

# ========= Конфіг =========
BO3_URL = "https://bo3.gg/teams/natus-vincere/matches"
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"
SCRAPED_TIME_IS_UTC = (os.environ.get("SCRAPED_TIME_IS_UTC", "true").lower() in ("1", "true", "yes"))

# HTTP
READ_TIMEOUT = 45
CONNECT_TIMEOUT = 10
TOTAL_RETRIES = 5
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429, 500, 502, 503, 504)
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
]

# ========= HTTP helpers =========
def make_session():
    s = requests.Session()
    retry = Retry(
        total=TOTAL_RETRIES,
        connect=TOTAL_RETRIES,
        read=TOTAL_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=STATUS_FORCELIST,
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,uk;q=0.8,ru;q=0.7",
        "Connection": "close",
    })
    return s

def fetch_html(url: str) -> str | None:
    sess = make_session()
    time.sleep(random.uniform(0.3, 1.2))
    try:
        resp = sess.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if resp.status_code != 200:
            print(f"[WARN] HTTP {resp.status_code} for {url}")
            return None
        print(f"[INFO] Fetched {url} ({len(resp.text)} bytes)")
        return resp.text
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Fetch failed for {url}: {e}")
        return None

# ========= parsing utils =========
def _normalize_month_day(text: str) -> str:
    """'sep 18' -> 'Sep 18' ; 'september 18' -> 'September 18'."""
    t = (text or "").strip().replace(".", " ")
    t = re.sub(r"\s+", " ", t)
    return t.title()

def _infer_year_from_href_or_today(href: str, month: int, day: int, tz) -> int:
    """Якщо в href кінцівка ...-DD-MM-YYYY — беремо її; інакше — рік (сьогодні/наступний)."""
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})$", href or "")
    if m:
        return int(m.group(3))
    today_local = datetime.now(tz).date()
    candidate = date(today_local.year, month, day)
    if candidate < today_local:
        return today_local.year + 1
    return today_local.year

def _parse_iso_from_detail(html: str) -> datetime | None:
    """
    Шукаємо ISO datetime у detail-сторінці:
    - JSON-LD: "startDate":"2025-09-18T12:30:00Z"
    - meta/property
    Повертаємо timezone-aware UTC datetime.
    """
    # JSON-LD
    m = re.search(r'"startDate"\s*:\s*"([^"]+)"', html, flags=re.IGNORECASE)
    if not m:
        # інші ключі
        m = re.search(r'"(date|start|start_time|startDate|datetime)"\s*:\s*"([^"]+)"', html, flags=re.IGNORECASE)
        if m:
            iso = m.group(2)
        else:
            # Прямі ISO у html
            m = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+\-]\d{2}:\d{2}))', html)
            if not m:
                return None
            iso = m.group(1)
    else:
        iso = m.group(1)

    iso_clean = iso.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_clean)
    except Exception:
        return None

    # Якщо naive — трактуємо як UTC
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    return dt.astimezone(pytz.utc)

def _parse_hhmm_and_md_from_detail(html: str) -> tuple[str,str] | None:
    """
    На детальній сторінці як fallback:
    - час HH:MM
    - дата як 'Sep 18'/'August 20' тощо
    """
    # HH:MM
    m_time = re.search(r'\b(\d{1,2}:\d{2})\b', html)
    if not m_time:
        return None
    time_text = m_time.group(1)

    # Month day (англ.)
    m_date = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2}\b', html, flags=re.IGNORECASE)
    if not m_date:
        return None
    date_text = _normalize_month_day(m_date.group(0))
    return date_text, time_text

def _detail_parse_datetime(detail_url: str, href_tail: str, tz_local: pytz.timezone) -> datetime | None:
    html = fetch_html(detail_url)
    if not html:
        return None

    # 1) JSON-LD / ISO
    iso_dt_utc = _parse_iso_from_detail(html)
    if iso_dt_utc:
        # Конвертуємо у локальний Київ
        return iso_dt_utc.astimezone(tz_local)

    # 2) Ручне парсення HH:MM + Month Day
    md = _parse_hhmm_and_md_from_detail(html)
    if not md:
        return None
    date_text, time_text = md
    normalized = _normalize_month_day(date_text)
    parsed_md = None
    for fmt in ("%b %d", "%B %d"):
        try:
            parsed_md = datetime.strptime(normalized, fmt)
            break
        except ValueError:
            continue
    if not parsed_md:
        return None

    year_int = _infer_year_from_href_or_today(href_tail, parsed_md.month, parsed_md.day, tz_local)
    try:
        hh, mm = time_text.split(":")
        start_naive = datetime(year_int, parsed_md.month, parsed_md.day, int(hh), int(mm))
    except Exception:
        return None

    # На детальній сторінці час майже завжди локальний для турніру/відвідувача, але щоб бути консистентними:
    if SCRAPED_TIME_IS_UTC:
        return pytz.utc.localize(start_naive).astimezone(tz_local)
    else:
        return tz_local.localize(start_naive)

# ========= основний парсер списку =========
def parse_matches_with_fallback():
    """
    1) Парсимо список .table-row — якщо є і дата, і час → використовуємо їх.
    2) Якщо чогось бракує — відкриваємо детальну сторінку матчу та витягуємо дату/час там.
    """
    html = fetch_html(BO3_URL)
    if not html:
        print("[WARN] No HTML fetched, skipping parse.")
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select(".table-row")
    print(f"[INFO] Rows matched: {len(rows)}")

    matches = []
    tz_local = pytz.timezone(TIMEZONE)

    for idx, row in enumerate(rows, start=1):
        a = row.select_one('a.c-global-match-link.table-cell[href]')
        if not a:
            continue
        href = a.get("href", "")
        link = "https://bo3.gg" + href

        # Команди
        teams = [el.get_text(strip=True) for el in row.select(".team-name")]
        team1, team2 = "Natus Vincere", "TBD"
        if len(teams) >= 2:
            team1, team2 = teams[0], teams[1]
        elif len(teams) == 1:
            team1 = teams[0]

        # Додаткова інфа
        bo_el = row.select_one(".bo-type")
        bo = bo_el.get_text(strip=True) if bo_el else ""
        tour_el = row.select_one(".table-cell.tournament .tournament-name")
        tournament = tour_el.get_text(strip=True) if tour_el else ""

        # Зі списку:
        time_el = row.select_one(".date .time")
        time_text = time_el.get_text(strip=True) if time_el else None
        date_el = row.select_one(".date")
        date_text = None
        if date_el:
            raw = date_el.get_text(" ", strip=True)
            if time_text:
                raw = raw.replace(time_text, "").strip()
            date_text = _normalize_month_day(raw) if raw else None

        # Спроба №1: достатньо даних у списку
        start_local = None
        if date_text and time_text:
            parsed_md = None
            for fmt in ("%b %d", "%B %d"):
                try:
                    parsed_md = datetime.strptime(date_text, fmt)
                    break
                except ValueError:
                    continue
            if parsed_md:
                year_int = _infer_year_from_href_or_today(href, parsed_md.month, parsed_md.day, tz_local)
                try:
                    hh, mm = time_text.split(":")
                    start_naive = datetime(year_int, parsed_md.month, parsed_md.day, int(hh), int(mm))
                    if SCRAPED_TIME_IS_UTC:
                        start_local = pytz.utc.localize(start_naive).astimezone(tz_local)
                    else:
                        start_local = tz_local.localize(start_naive)
                except Exception as e:
                    print(f"[WARN] Row {idx} time compose failed: {e}")

        # Спроба №2: детальна сторінка
        if start_local is None:
            start_local = _detail_parse_datetime(link, href, tz_local)
            print(f"[FALLBACK] Row {idx} → detail parsed: {bool(start_local)}; url={link}")

        if start_local is None:
            # не вдалося дістати дату/час — пропускаємо
            continue

        end_local = start_local + timedelta(hours=2)
        start_dt_str = start_local.strftime("%Y-%m-%dT%H:%M:%S")
        end_dt_str   = end_local.strftime("%Y-%m-%dT%H:%M:%S")

        summary_main = f"{team1} vs {team2}"
        if bo:
            summary_main += f" ({bo})"
        summary = summary_main + (f" — {tournament}" if tournament else "")

        matches.append({
            "summary": summary,
            "start_dt_str": start_dt_str,
            "end_dt_str": end_dt_str,
            "link": link,
            "tournament": tournament,
            "bo": bo
        })

    return matches

# ========= Calendar helpers =========
def has_duplicate_event(service, calendar_id, start_dt, summary):
    """Перевірка на дублі в інтервалі [-1h, +3h] від старту."""
    time_min = (start_dt - timedelta(hours=1)).isoformat()
    time_max = (start_dt + timedelta(hours=3)).isoformat()
    items = service.events().list(
        calendarId=calendar_id,
        timeMin=time_min,
        timeMax=time_max,
        singleEvents=True,
        orderBy="startTime",
        q=summary.split(" — ")[0]
    ).execute().get("items", [])

    for ev in items:
        if ev.get("summary") == summary:
            return True
    return False

def create_events(service, matches):
    created = 0
    tz_local = pytz.timezone(TIMEZONE)

    for m in matches:
        start_dt = datetime.strptime(m["start_dt_str"], "%Y-%m-%dT%H:%M:%S")
        start_dt = tz_local.localize(start_dt)

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
        print(f"[OK] Created: {m['summary']} → {res.get('htmlLink','')}")
        created += 1

    return created

# ========= entrypoint =========
def main():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing.")
    info = json.loads(creds_json)

    tz_local = pytz.timezone(TIMEZONE)
    print(f"[CHECK] SCRAPED_TIME_IS_UTC={SCRAPED_TIME_IS_UTC}")

    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/calendar"])
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    matches = parse_matches_with_fallback()
    print(f"[INFO] Parsed {len(matches)} matches total.")

    if not matches:
        print("[WARN] No matches parsed this run.")
        return

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")

if __name__ == "__main__":
    main()
