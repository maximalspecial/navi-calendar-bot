import os
import re
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# Джерело даних
BO3_URL = "https://bo3.gg/teams/natus-vincere/matches"

# Таймзона та календар
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"

# Якщо HTML дає час у UTC (типова ситуація для серверного HTML),
# конвертуємо його в Europe/Kyiv. За замовчуванням — true.
SCRAPED_TIME_IS_UTC = (os.environ.get("SCRAPED_TIME_IS_UTC", "true").lower() in ("1", "true", "yes"))


def parse_upcoming_matches():
    """
    Парсить майбутні матчі NaVi з bo3.gg.
    Повертає список словників:
      {
        summary: "Natus Vincere vs M80 (Bo3) — Tournament",
        start_dt_str: "YYYY-MM-DDTHH:MM:SS",   # без офсету!
        end_dt_str:   "YYYY-MM-DDTHH:MM:SS",   # без офсету!
        link: "https://bo3.gg/matches/...",
        tournament: "...",
        bo: "Bo3"
      }
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NaViCalendarBot/1.0)"}
    resp = requests.get(BO3_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

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

        # Формат серії (Bo3 тощо)
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
                raw = raw.replace(time_text, "").strip()  # залишаємо "Aug 31"/"August 31"
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

        # Склеюємо рядок дати-часу, який парсимо у naive datetime (без tzinfo)
        # приклади: "Aug 31 2025 12:30" або "August 31 2025 12:30"
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
            # Інтерпретуємо розпарсений час як UTC і конвертуємо у Київ
            start_local = pytz.utc.localize(start_naive).astimezone(tz_local)
            print(f"[TZ] Interpreted scraped time as UTC → {start_local.isoformat()}")
        else:
            # Інтерпретуємо як локальний київський
            start_local = tz_local.localize(start_naive)
            print(f"[TZ] Interpreted scraped time as Europe/Kyiv → {start_local.isoformat()}")

        end_local = start_local + timedelta(hours=2)

        # Для Google Calendar передаємо dateTime БЕЗ офсету, таймзону окремо.
        start_dt_str = start_local.strftime("%Y-%m-%dT%H:%M:%S")
        end_dt_str   = end_local.strftime("%Y-%m-%dT%H:%M:%S")

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
    """
    Перевірка на дублі: шукаємо події з подібною назвою у вікні [-1h, +3h] від старту.
    """
    time_min = (start_dt - timedelta(hours=1)).isoformat()
    time_max = (start_dt + timedelta(hours=3)).isoformat()
    items = service.events().list(
        calendarId=calendar_id,
        timeMin=time_min,
        timeMax=time_max,
        singleEvents=True,
        orderBy="startTime",
        q=summary.split(" — ")[0]  # шукаємо за основною частиною (команди)
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
    # Авторизація: JSON сервісного акаунта у секреті GOOGLE_CREDENTIALS_JSON
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
        raise

    # Парсимо матчі
    matches = parse_upcoming_matches()
    print(f"[INFO] Found {len(matches)} upcoming matches.")
    for m in matches:
        print(f"  - {m['summary']} @ {m['start_dt_str']} ({TIMEZONE})")

    # Створюємо події
    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")


if __name__ == "__main__":
    main()
