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
READ_TIMEOUT = 45     # секунд
CONNECT_TIMEOUT = 10  # секунд
TOTAL_RETRIES = 5
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429, 500, 502, 503, 504)

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
]


def make_session():
    """HTTP сесія з ретраями і бекофом."""
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
    """Завантажити HTML з ретраями. Повертає text або None (без виключень)."""
    sess = make_session()
    # Невелика випадкова пауза (трохи схоже на користувача)
    time.sleep(random.uniform(0.3, 1.2))
    try:
        resp = sess.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if resp.status_code != 200:
            print(f"[WARN] HTTP {resp.status_code} for {url}")
            return None
        # іноді корисно глянути розмір
        print(f"[INFO] Fetched {url} ({len(resp.text)} bytes)")
        return resp.text
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Fetch failed for {url}: {e}")
        return None


def parse_upcoming_matches():
    """
    Парсить майбутні матчі NaVi з bo3.gg.
    Повертає список словників:
      {
        summary: "...",
        start_dt_str: "YYYY-MM-DDTHH:MM:SS",   # без офсету
        end_dt_str:   "YYYY-MM-DDTHH:MM:SS",   # без офсету
        link: "...",
        tournament: "...",
        bo: "Bo3"/"Bo1"/...
      }
    """
    html = fetch_html(BO3_URL)
    if not html:
        print("[WARN] No HTML fetched, skipping parse.")
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select(".table-row.table-row--upcoming")
    matches = []

    for row in rows:
        # Посилання на матч
        a = row.select_one('a.c-global-match-link.table-cell[href]')
        if not a:
            continue
        href = a["href"]
        link = "https://bo3.gg" + href

        # Команди
        teams = [el.get_text(strip=True) for el in row.select(".team-name")]
        if len(teams) >= 2:
            team1, team2 = teams[0], teams[1]
        elif len(teams) == 1:
            team1, team2 = teams[0], "TBD"
        else:
            team1, team2 = "Natus Vincere", "TBD"

        # Формат серії
        bo_el = row.select_one(".bo-type")
        bo = bo_el.get_text(strip=True) if bo_el else ""

        # Турнір
        tour_el = row.select_one(".table-cell.tournament .tournament-name")
        tournament = tour_el.get_text(strip=True) if tour_el else ""

        # Час і дата
        time_el = row.select_one(".date .time")
        time_text = time_el.get_text(strip=True) if time_el else None

        date_el = row.select_one(".date")
        date_text = None
        if date_el:
            raw = date_el.get_text(" ", strip=True)
            if time_text:
                raw = raw.replace(time_text, "").strip()  # "Aug 31" / "August 31"
            date_text = raw

        # Рік із URL типу ...-DD-MM-YYYY (в кінці)
        year_int = None
        m = re.search(r"(\d{2})-(\d{2})-(\d{4})$", href)
        if m:
            year_int = int(m.group(3))
        if not year_int:
            year_int = datetime.now().year

        if not (date_text and time_text):
            print(f"[WARN] Skip: missing date/time for {team1} vs {team2} @ {link}")
            continue

        # Парсимо "Aug 31 2025 12:30"
        dt_str = f"{date_text} {year_int} {time_text}"
        start_naive = None
        for fmt in ("%b %d %Y %H:%M", "%B %d %Y %H:%M"):
            try:
                start_naive = datetime.strptime(dt_str, fmt)
                break
            except ValueError:
                continue
        if not start_naive:
            print(f"[ERROR] Date parse failed for '{dt_str}' (teams: {team1} vs {team2})")
            continue

        tz_local = pytz.timezone(TIMEZONE)
        if SCRAPED_TIME_IS_UTC:
            # HTML час інтерпретуємо як UTC і конвертуємо в Київ
            start_local = pytz.utc.localize(start_naive).astimezone(tz_local)
            print(f"[TZ] Interpreted scraped time as UTC → {start_local.isoformat()}")
        else:
            # Трактуємо як локальний київський
            start_local = tz_local.localize(start_naive)
            print(f"[TZ] Interpreted scraped time as Europe/Kyiv → {start_local.isoformat()}")

        end_local = start_local + timedelta(hours=2)

        # Для Google Calendar передаємо dateTime БЕЗ офсету, таймзону okремо.
        start_dt_str = start_local.strftime("%Y-%m-%dT%H:%M:%S")
        end_dt_str = end_local.strftime("%Y-%m-%dT%H:%M:%S")

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
        # Для пошуку дублів потрібен aware-datetime
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
        # Не продовжуємо, бо немає сенсу
        return

    # Парсимо матчі
    matches = parse_upcoming_matches()
    print(f"[INFO] Found {len(matches)} upcoming matches.")

    # Якщо не змогли отримати HTML або матчі — не валимо пайплайн
    if not matches:
        print("[WARN] No matches parsed (network issue or schedule empty). Skipping this run gracefully.")
        return

    for m in matches:
        print(f"  - {m['summary']} @ {m['start_dt_str']} ({TIMEZONE})")

    # Створюємо події
    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")


if __name__ == "__main__":
    main()
