import os
import re
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

BO3_URL = "https://bo3.gg/teams/natus-vincere/matches"
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"


def parse_upcoming_matches():
    """
    Скрейпить майбутні матчі NaVi з bo3.gg та повертає список словників:
    {summary, start, end, link, tournament, bo}
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NaViCalendarBot/1.0)"}
    resp = requests.get(BO3_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    rows = soup.select(".table-row.table-row--upcoming")
    matches = []

    for row in rows:
        a = row.select_one('a.c-global-match-link.table-cell[href]')
        if not a:
            continue
        href = a["href"]
        link = "https://bo3.gg" + href

        teams = [el.get_text(strip=True) for el in row.select(".team-name")]
        if len(teams) >= 2:
            team1, team2 = teams[0], teams[1]
        elif len(teams) == 1:
            team1, team2 = teams[0], "TBD"
        else:
            team1, team2 = "Natus Vincere", "TBD"

        bo_el = row.select_one(".bo-type")
        bo = bo_el.get_text(strip=True) if bo_el else ""

        tour_el = row.select_one(".table-cell.tournament .tournament-name")
        tournament = tour_el.get_text(strip=True) if tour_el else ""

        time_el = row.select_one(".date .time")
        time_text = time_el.get_text(strip=True) if time_el else None

        date_el = row.select_one(".date")
        date_text = None
        if date_el:
            raw = date_el.get_text(" ", strip=True)
            if time_text:
                raw = raw.replace(time_text, "").strip()  # залишимо тільки "Aug 31"/"August 31"
            date_text = raw

        # Рік беремо з кінця href, якщо є ...-DD-MM-YYYY
        year_int = None
        m = re.search(r"(\d{2})-(\d{2})-(\d{4})$", href)
        if m:
            year_int = int(m.group(3))
        if not year_int:
            year_int = datetime.now().year

        if not (date_text and time_text):
            print(f"[WARN] Skip: missing date/time for {team1} vs {team2} @ {link}")
            continue

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

        tz = pytz.timezone(TIMEZONE)
        start = tz.localize(start_naive)
        end = start + timedelta(hours=2)

        summary_main = f"{team1} vs {team2}"
        if bo:
            summary_main += f" ({bo})"
        summary = summary_main + (f" — {tournament}" if tournament else "")

        matches.append({
            "summary": summary,
            "start": start,
            "end": end,
            "link": link,
            "tournament": tournament,
            "bo": bo
        })

    return matches


def has_duplicate_event(service, calendar_id, start_dt, summary):
    """
    Перевіряємо, чи є схожа подія біля цього часу (щоб не дублювати).
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
    for m in matches:
        event = {
            "summary": m["summary"],
            "description": f"Auto-added from {BO3_URL}\nMatch page: {m['link']}",
            "start": {"dateTime": m["start"].isoformat(), "timeZone": TIMEZONE},
            "end": {"dateTime": m["end"].isoformat(), "timeZone": TIMEZONE},
        }

        print("[DEBUG] Prepared event:", event)

        if has_duplicate_event(service, CALENDAR_ID, m["start"], m["summary"]):
            continue

        res = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
        html_link = res.get("htmlLink", "")
        print(f"[OK] Created: {m['summary']} → {html_link}")
        created += 1
    return created


def main():
    # Авторизація: JSON ключ сервісного акаунта передаємо через env GOOGLE_CREDENTIALS_JSON
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing. Add it to GitHub Secrets.")
    info = json.loads(creds_json)

    # Уточнюю логін сервісного акаунта (для підказки, кому давати доступ у Calendar)
    client_email = info.get("client_email", "UNKNOWN_SERVICE_ACCOUNT")
    print(f"[CHECK] Service Account: {client_email}")

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/calendar"]
    )
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    # Префлайт: перевіримо доступ до календаря до створення подій
    try:
        info_cal = service.calendars().get(calendarId=CALENDAR_ID).execute()
        print(f"[CHECK] Using calendar: {info_cal.get('summary')} (id={info_cal.get('id')})")
    except Exception as e:
        print(f"[ERROR] Calendar '{CALENDAR_ID}' not accessible.\n"
              f"• Перевір, що ID без пробілів/переносів (наприклад, 'primary' або твій email)\n"
              f"• Поділись своїм календарем на {client_email} з правами 'Make changes to events'")
        raise

    matches = parse_upcoming_matches()
    print(f"[INFO] Found {len(matches)} upcoming matches.")
    for m in matches:
        print(f"  - {m['summary']} @ {m['start'].isoformat()}")

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")


if __name__ == "__main__":
    main()
