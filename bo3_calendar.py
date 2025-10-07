import os, re, json, time
from datetime import datetime, timedelta, date
from typing import Optional, Tuple
import pytz

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

# ========== CONFIG ==========
BO3_URL = "https://bo3.gg/teams/natus-vincere/matches"
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"
EVENT_DURATION_HOURS = 2
TODAY_RECHECK_INTERVAL_SECONDS = int(os.environ.get("TODAY_RECHECK_INTERVAL_SECONDS", "3600"))

# Фільтри часу
LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "60"))          # події не далі цього горизонту
PAST_GRACE_MINUTES = int(os.environ.get("PAST_GRACE_MINUTES", "0"))   # скільки хвилин минулого дозволяємо

MONTHS = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"sept":9,"oct":10,"nov":11,"dec":12,
    "january":1,"february":2,"march":3,"april":4,"june":6,"july":7,"august":8,
    "september":9,"october":10,"november":11,"december":12
}

TZ_LOCAL = pytz.timezone(TIMEZONE)

def _infer_year_from_href(href: str, month: int, day: int) -> int:
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})$", href or "")
    if m:
        return int(m.group(3))
    today = datetime.now(TZ_LOCAL).date()
    cand = date(today.year, month, day)
    return today.year if cand >= today else today.year + 1

def _norm_month_day(text: str):
    if not text: return None
    t = re.sub(r"\s+", " ", text.replace(".", " ")).strip()
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2})", t)
    if not m: return None
    mon = m.group(1).lower()
    day = int(m.group(2))
    if mon not in MONTHS: return None
    return MONTHS[mon], day

def scrape_matches():
    """
    Відкриваємо сторінку у Chromium (headless), чекаємо рендеру й дістаємо матчі з DOM.
    Повертаємо ТІЛЬКИ сьогоднішні/майбутні (з урахуванням PAST_GRACE_MINUTES).
    """
    out = []
    now_local = datetime.now(TZ_LOCAL)
    horizon = now_local + timedelta(days=LOOKAHEAD_DAYS)
    grace_cutoff = now_local - timedelta(minutes=PAST_GRACE_MINUTES)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            locale="en-US",
            timezone_id=TIMEZONE,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
            viewport={"width": 1366, "height": 900},
        )
        page = context.new_page()
        page.set_default_timeout(30000)

        page.goto(BO3_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)
        for _ in range(5):
            if page.locator(".table-row").count() > 0:
                break
            page.wait_for_timeout(800)

        rows = page.locator(".table-row")
        count = rows.count()
        print(f"[INFO] Rendered rows: {count}")

        for i in range(count):
            row = rows.nth(i)

            # посилання на матч
            link_el = row.locator('a[href*="/matches/"]').first
            href = ""
            try:
                href = link_el.get_attribute("href") or ""
            except:
                href = ""
            match_link = ("https://bo3.gg" + href) if href else BO3_URL

            # команди
            team_names = row.locator(".team-name")
            tcount = team_names.count()
            team1 = team_names.nth(0).inner_text().strip() if tcount >= 1 else ""
            team2 = team_names.nth(1).inner_text().strip() if tcount >= 2 else "TBD"

            # формат (Bo3)
            bo = ""
            try:
                bo_text = row.locator(".bo-type").first.inner_text().strip()
                if re.match(r"Bo\d", bo_text, flags=re.I):
                    bo = bo_text
            except:
                pass

            # турнір
            tournament = ""
            try:
                tournament = row.locator(".tournament-name").first.inner_text().strip()
            except:
                pass

            # час/дата
            time_text = ""
            date_text = ""
            try:
                time_text = row.locator(".date .time").first.inner_text().strip()
            except:
                pass
            try:
                date_block = row.locator(".date").first.inner_text()
                if date_block:
                    date_text = date_block.replace(time_text, "").strip()
            except:
                pass

            if not (time_text and date_text):
                raw = row.inner_text().strip()
                mt = re.search(r'\b(\d{1,2}:\d{2})\b', raw)
                md = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}', raw, re.I)
                if mt and md:
                    time_text = mt.group(1)
                    date_text = md.group(0)

            if not (time_text and date_text):
                continue

            md = _norm_month_day(date_text)
            if not md:
                continue
            mon, day = md

            try:
                hh, mm = [int(x) for x in time_text.split(":")]
            except:
                continue

            year = _infer_year_from_href(href, mon, day)
            start_local = TZ_LOCAL.localize(datetime(year, mon, day, hh, mm))
            end_local = start_local + timedelta(hours=EVENT_DURATION_HOURS)

            # === ФІЛЬТР ЧАСУ: тільки сьогодні/майбутні (з грейсом) ===
            if start_local < grace_cutoff:
                # старіше ніж дозволений грейс — скіпаємо
                continue
            if start_local > horizon:
                # занадто далеко в майбутньому — скіпаємо
                continue

            summary = f"{team1} vs {team2}" + (f" ({bo})" if bo else "")
            if tournament:
                summary += f" — {tournament}"

            out.append({
                "summary": summary,
                "start_dt_str": start_local.strftime("%Y-%m-%dT%H:%M:%S"),
                "end_dt_str": end_local.strftime("%Y-%m-%dT%H:%M:%S"),
                "link": match_link,
            })

        context.close()
        browser.close()
    return out

def _base_summary(summary: str) -> str:
    return " vs ".join(summary.split(" vs ")[:2])

def _parse_event_start(event) -> Optional[datetime]:
    start_raw = (event.get("start", {}) or {}).get("dateTime")
    if not start_raw:
        return None
    try:
        dt = datetime.fromisoformat(start_raw)
        if dt.tzinfo is None:
            return TZ_LOCAL.localize(dt)
        return dt.astimezone(TZ_LOCAL)
    except Exception as exc:
        print(f"[WARN] Failed to parse existing event start '{start_raw}': {exc}")
        return None

def find_existing_event(service, calendar_id, summary: str, start_dt: datetime) -> Tuple[Optional[dict], Optional[float]]:
    base = _base_summary(summary)
    now_local = datetime.now(TZ_LOCAL)
    time_min = (now_local - timedelta(days=7)).isoformat()
    time_max = (now_local + timedelta(days=max(LOOKAHEAD_DAYS, 7))).isoformat()
    items = service.events().list(
        calendarId=calendar_id,
        timeMin=time_min,
        timeMax=time_max,
        singleEvents=True,
        orderBy="startTime",
        q=base,
    ).execute().get("items", [])

    best = None
    best_diff = None
    for ev in items:
        ev_summary = ev.get("summary", "")
        if not ev_summary.startswith(base):
            continue
        ev_start = _parse_event_start(ev)
        if not ev_start:
            continue
        diff = abs((ev_start - start_dt).total_seconds())
        if best is None or diff < best_diff:
            best = ev
            best_diff = diff
    return best, best_diff

def create_events(service, matches):
    created = 0
    updated = 0
    for m in matches:
        start_dt = TZ_LOCAL.localize(datetime.strptime(m["start_dt_str"], "%Y-%m-%dT%H:%M:%S"))
        existing, diff = find_existing_event(service, CALENDAR_ID, m["summary"], start_dt)
        if existing:
            if diff is not None and diff <= 5 * 60 and existing.get("summary") == m["summary"]:
                print(f"[SKIP] Up-to-date event exists: {existing.get('summary')} at {existing.get('start',{}).get('dateTime')}")
                continue

            event_id = existing.get("id")
            if not event_id:
                print(f"[WARN] Existing event without ID, creating new one: {m['summary']}")
            else:
                patch_body = {
                    "summary": m["summary"],
                    "description": f"Auto-added from {BO3_URL}\nMatch page: {m['link']}",
                    "start": {"dateTime": m["start_dt_str"], "timeZone": TIMEZONE},
                    "end":   {"dateTime": m["end_dt_str"],   "timeZone": TIMEZONE},
                }
                service.events().patch(calendarId=CALENDAR_ID, eventId=event_id, body=patch_body).execute()
                updated += 1
                print(f"[UPDATED] {m['summary']} → {existing.get('htmlLink','')}")
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
    return created, updated

def _has_match_today(matches):
    today = datetime.now(TZ_LOCAL).date()
    for m in matches:
        try:
            start_date = datetime.strptime(m["start_dt_str"], "%Y-%m-%dT%H:%M:%S").date()
        except Exception as exc:
            print(f"[WARN] Failed to parse match start '{m.get('start_dt_str')}' for today check: {exc}")
            continue
        if start_date == today:
            return True
    return False

def sync_matches(service):
    print(f"[CHECK] Source: bo3.gg (Playwright); TZ={TIMEZONE}")
    print(f"[CHECK] Filters: LOOKAHEAD_DAYS={LOOKAHEAD_DAYS}, PAST_GRACE_MINUTES={PAST_GRACE_MINUTES}")
    matches = scrape_matches()
    print(f"[INFO] Parsed matches after filtering: {len(matches)}")
    if not matches:
        print("[WARN] Nothing parsed (or everything filtered out).")
        return matches

    created, updated = create_events(service, matches)
    print(f"[DONE] Created {created} events, updated {updated} events.")
    return matches

def main():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing.")
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/calendar"])
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    iteration = 1
    while True:
        print(f"[RUN] Sync iteration {iteration}")
        matches = sync_matches(service)
        if not matches:
            break

        if _has_match_today(matches):
            next_run = TODAY_RECHECK_INTERVAL_SECONDS
            print(
                f"[INFO] Detected match scheduled for today. Sleeping {next_run // 60} minutes before re-check."
            )
            time.sleep(next_run)
            iteration += 1
            continue

        break

if __name__ == "__main__":
    main()
