import os
import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Джерело
BO3_URL = "https://bo3.gg/teams/natus-vincere/matches"

# Таймзона та календар
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"

# Якщо HTML дає час у UTC (типово на CI), конвертуємо в Europe/Kyiv
SCRAPED_TIME_IS_UTC = (os.environ.get("SCRAPED_TIME_IS_UTC", "true").lower() in ("1", "true", "yes"))

# HTTP налаштування
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

TODAY_ALIASES = {"today", "сьогодні", "сегодня"}
TOMORROW_ALIASES = {"tomorrow", "завтра"}

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

def _parse_date_text_to_date(date_text: str, year_hint: int) -> tuple[int,int,int]:
    """
    Приймає 'Aug 31' / 'August 31' / 'Today' / 'Сьогодні' / 'Tomorrow' / 'Завтра'
    Повертає (year, month, day) у Києві.
    """
    tz_local = pytz.timezone(TIMEZONE)
    now_local = datetime.now(tz_local).date()
    lower = date_text.strip().lower()

    if lower in TODAY_ALIASES:
        d = now_local
        return (d.year, d.month, d.day)
    if lower in TOMORROW_ALIASES:
        d = now_local + timedelta(days=1)
        return (d.year, d.month, d.day)

    # спробуємо англ. назви місяців
    for fmt in ("%b %d", "%B %d"):
        try:
            tmp = datetime.strptime(date_text.strip(), fmt)
            # tmp має 1900-й рік -> замінюємо на підказаний
            return (year_hint, tmp.month, tmp.day)
        except ValueError:
            continue

    # fallback: повернемо сьогодні
    print(f"[WARN] Could not parse date label '{date_text}', fallback to today.")
    return (now_local.year, now_local.month, now_local.day)

def parse_upcoming_matches():
    """
    Парсить майбутні/поточні матчі NaVi з bo3.gg.
    Повертає список словників зі строковими dateTime без офсету.
    """
    html = fetch_html(BO3_URL)
    if not html:
        print("[WARN] No HTML fetched, skipping parse.")
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Візьмемо не лише upcoming, а й live/ongoing/today/featured
    rows = soup.select(
        ".table-row.table-row--upcoming, "
        ".table-row.table-row--live, "
        ".table-row.table-row--ongoing, "
        ".table-row.table-row--today, "
        ".table-row.table-row--featured"
    )
    print(f"[INFO] Rows matched: {len(rows)}")

    matches = []
    tz_local = pytz.timezone(TIMEZONE)

    for idx, row in enumerate(rows, start=1):
        row_classes = row.get("class", [])
        a = row.select_one('a.c-global-match-link.table-cell[href]')
        if not a:
            print(f"[DEBUG] Row {idx}: no main link; classes={row_classes}")
            continue

        href = a["href"]
        link = "https://bo3.gg" + href

        # Команди
        teams = [el.get_text(strip=True) for el in row.select(".team-name")]
        team1, team2 = "Natus Vincere", "TBD"
        if len(teams) >= 2:
            team1, team2 = teams[0], teams[1]
        elif len(teams) == 1:
            team1 = teams[0]

        # Формат серії
        bo_el = row.select_one(".bo-type")
        bo = bo_el.get_text(strip=True) if bo_el else ""

        # Турнір
        tour_el = row.select_one(".table-cell.tournament .tournament-name")
        tournament = tour_el.get_text(strip=True) if tour_el else ""

        # Час і дата (raw для дебагу)
        time_el = row.select_one(".date .time")
        time_text = time_el.get_text(strip=True) if time_el else None

        date_el = row.select_one(".date")
        date_text = None
        if date_el:
            raw = date_el.get_text(" ", strip=True)
            if time_text:
                raw = raw.replace(time_text, "").strip()
            date_text = raw

        # Рік із URL типу ...-DD-MM-YYYY
        year_int = None
        m = re.search(r"(\d{2})-(\d{2})-(\d{4})$", href)
        if m:
            year_int = int(m.group(3))
        if not year_int:
            # Якщо місяць у підписі вже пройшов і ми наприкінці року — можна доробити логику на +1 рік,
            # але базово лишаємо поточний.
            year_int = datetime.now(tz_local).year

        print(f"[DEBUG] Row {idx}: classes={row_classes}, href={href}, date_text={date_text}, time_text={time_text}, year={year_int}")

        if not (date_text and time_text):
            print(f"[WARN] Row {idx}: missing date/time for {team1} vs {team2} @ {link}")
            continue

        # Розкладаємо date_text у (y,m,d), підтримуючи Today/Tomorrow/локалізовані варіанти
        y, m, d = _parse_date_text_to_date(date_text, year_int)

        # Будуємо naive datetime з розпаршеного часу (наприклад, '12:30')
        try:
            hh, mm = time_text.split(":")
            start_naive = datetime(y, m, d, int(hh), int(mm))
        except Exception as e:
            print(f"[ERROR] Row {idx}: time parse failed '{time_text}' → {e}")
            continue

        # Конвертація часових поясів
        if SCRAPED_TIME_IS_UTC:
            # інтерпретуємо як UTC і переводимо у Київ
            start_local = pytz.utc.localize(start_naive).astimezone(tz_local)
            print(f"[TZ] Row {idx}: UTC→Kyiv {start_local.isoformat()}")
        else:
            start_local = tz_local.localize(start_naive)
            print(f"[TZ] Row {idx}: as Kyiv {start_local.isoformat()}")

        end_local = start_local + timedelta(hours=2)

        # Для Google Calendar передаємо dateTime БЕЗ офсету
        start_dt_str = start_local.strftime("%Y-%m-%dT%H:%M:%S")
        end_dt_str = end_local.strftime("%Y-%m-%dT%H:%M:%S")

        # Заголовок
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
        ev_summary = ev.get("summary", "")
        ev_start = ev.get("start", {}).get("dateTime")
        if ev_summary == summary and ev_start:
            print(f"[SKIP] Duplicate found: {summary} at {ev_start}")
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
        html_link = res.get("htmlLink", "")
        print(f"[OK] Created: {m['summary']} → {html_link}")
        created += 1

    return created

def main():
    # Авторизація
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing. Add it to GitHub Secrets.")
    info = json.loads(creds_json)

    client_email = info.get("client_email", "UNKNOWN_SERVICE_ACCOUNT")
    print(f"[CHECK] Service Account: {client_email}")
    print(f"[CHECK] SCRAPED_TIME_IS_UTC={SCRAPED_TIME_IS_UTC}")

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/calendar"]
    )
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    # Перевірка доступу до календаря
    try:
        info_cal = service.calendars().get(calendarId=CALENDAR_ID).execute()
        print(f"[CHECK] Using calendar: {info_cal.get('summary')} (id={info_cal.get('id')})")
    except Exception:
        print(f"[ERROR] Calendar '{CALENDAR_ID}' not accessible.\n"
              f"• Перевір, що ID без пробілів/переносів (наприклад, 'primary' або точний ID з налаштувань)\n"
              f"• Поділись календарем із {client_email} з правами 'Make changes to events'")
        return

    matches = parse_upcoming_matches()
    print(f"[INFO] Found {len(matches)} rows after parse.")

    if not matches:
        print("[WARN] No matches parsed (maybe label 'Today'/'Tomorrow', live-only rows, or network).")
        return

    for m in matches:
        print(f"  - {m['summary']} @ {m['start_dt_str']} ({TIMEZONE}) → {m['link']}")

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")

if __name__ == "__main__":
    main()
