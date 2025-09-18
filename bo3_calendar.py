import os, re, json, time, random
from datetime import datetime, timedelta, timezone
import pytz
import requests
import cloudscraper
from bs4 import BeautifulSoup

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== CONFIG ==================
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"
EVENT_DURATION_HOURS = 2
LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "60"))
STRICT_DUP_CHECK = (os.environ.get("STRICT_DUP_CHECK", "true").lower() in ("1","true","yes"))

HLTV_DESKTOP = "https://www.hltv.org/matches"
HLTV_MOBILE  = "https://m.hltv.org/matches"
TEAM_PATTERNS = (r"Natus\s+Vincere", r"\bNAVI\b")

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
]
READ_TIMEOUT = 45
CONNECT_TIMEOUT = 15
TOTAL_RETRIES = 4
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429,500,502,503,504)

TZ_LOCAL = pytz.timezone(TIMEZONE)

# ================== HTTP ==================
def make_retrying_session():
    s = requests.Session()
    retry = Retry(
        total=TOTAL_RETRIES, connect=TOTAL_RETRIES, read=TOTAL_RETRIES,
        backoff_factor=BACKOFF_FACTOR, status_forcelist=STATUS_FORCELIST,
        allowed_methods=frozenset(["GET","HEAD"]),
        raise_on_status=False, respect_retry_after_header=True,
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
    """
    Спершу пробуємо через cloudscraper (Cloudflare bypass),
    якщо не вдалось — звичайний requests з ретраями.
    """
    # 1) cloudscraper
    try:
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        scraper.headers.update({
            "User-Agent": random.choice(UA_POOL),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,uk;q=0.8,ru;q=0.7",
        })
        time.sleep(random.uniform(0.6, 1.4))
        r = scraper.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if r.status_code == 200 and r.text and not re.search(r"Attention Required|cloudflare|cf-browser-verification", r.text, re.I):
            print(f"[INFO] Fetched via cloudscraper {url} ({len(r.text)} bytes)")
            return r.text
        else:
            print(f"[WARN] cloudscraper got HTTP {r.status_code} or challenge page for {url}")
    except Exception as e:
        print(f"[WARN] cloudscraper failed for {url}: {e}")

    # 2) fallback: requests + retries
    sess = make_retrying_session()
    try:
        time.sleep(random.uniform(0.4, 1.0))
        r2 = sess.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if r2.status_code == 200:
            print(f"[INFO] Fetched via requests {url} ({len(r2.text)} bytes)")
            return r2.text
        else:
            print(f"[WARN] HTTP {r2.status_code} for {url}")
            return None
    except requests.RequestException as e:
        print(f"[ERROR] Fetch failed {url}: {e}")
        return None

# ================== PARSERS ==================
def unix_ms_to_kyiv(ms: int) -> datetime:
    return datetime.fromtimestamp(ms/1000.0, tz=timezone.utc).astimezone(TZ_LOCAL)

def parse_time_str_to_section_date(time_str: str, section_date: datetime | None) -> datetime | None:
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", time_str or "")
    if not m:
        return None
    hh, mm = int(m.group(1)), int(m.group(2))
    base = section_date or TZ_LOCAL.localize(datetime.now(TZ_LOCAL).replace(hour=0, minute=0, second=0, microsecond=0))
    try:
        return TZ_LOCAL.localize(datetime(base.year, base.month, base.day, hh, mm))
    except Exception:
        return None

def extract_section_date(text: str) -> datetime | None:
    t = " ".join((text or "").split())
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', t)
    if m:
        y, mo, d = map(int, m.groups())
        try: return TZ_LOCAL.localize(datetime(y, mo, d, 0, 0))
        except Exception: return None
    m = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,\s*(\d{4}))?', t, re.I)
    if m:
        month_map = {m:i for i,m in enumerate(["","January","February","March","April","May","June","July","August","September","October","November","December"])}
        mo = month_map[m.group(1).capitalize()]
        day = int(m.group(2))
        year = int(m.group(3)) if m.group(3) else datetime.now(TZ_LOCAL).year
        try: return TZ_LOCAL.localize(datetime(year, mo, day, 0, 0))
        except Exception: return None
    return None

def parse_hltv_matches_page(html: str, base_url: str):
    soup = BeautifulSoup(html, "html.parser")
    matches = []
    now_local = datetime.now(TZ_LOCAL)
    horizon = now_local + timedelta(days=LOOKAHEAD_DAYS)

    # знайдемо заголовки секцій з датами
    sections = []
    for h in soup.select("h2, h3, .eventDayHeader, .eventDayHeadline, .match-day"):
        txt = h.get_text(" ", strip=True)
        if re.search(r'\d{4}-\d{2}-\d{2}', txt) or re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}', txt, re.I):
            sections.append((h, extract_section_date(txt)))
    if not sections:
        sections = [(soup, None)]

    for head, section_date in sections:
        parent = head if head is soup else head.parent

        # Усі матчі в секції: елементи з data-unix або характерні блоки
        items = parent.select('[data-unix]')
        if not items:
            items = parent.select('.match, .upcomingMatch, .matchbox, .standard-box')

        for el in items:
            # час
            start_local = None
            if el.has_attr("data-unix") and str(el.get("data-unix","")).isdigit():
                start_local = unix_ms_to_kyiv(int(el["data-unix"]))
            else:
                # пошук видимого HH:MM
                for node in el.select(".time, .matchTime, .match-time, .when, time"):
                    s = node.get_text(" ", strip=True)
                    dt = parse_time_str_to_section_date(s, section_date)
                    if dt:
                        start_local = dt; break

            if not start_local:
                continue
            if start_local < now_local - timedelta(hours=3) or start_local > horizon:
                continue

            # команди
            raw_text = el.get_text(" ", strip=True)
            teams = [t.get_text(" ", strip=True) for t in el.select(".team, .opponent, .teamname, .text-ellipsis, .team1, .team2")]
            teams = [t for t in teams if 1 <= len(t) <= 40]
            uniq = []
            for t in teams:
                if t and t not in uniq:
                    uniq.append(t)
            team1 = team2 = None
            if len(uniq) >= 2:
                team1, team2 = uniq[0], uniq[1]
            else:
                mvs = re.search(r'([A-Za-z0-9\'\-\.\s]{2,40})\s+vs\s+([A-Za-z0-9\'\-\.\s]{2,40})', raw_text, re.I)
                if mvs:
                    team1, team2 = mvs.group(1).strip(), mvs.group(2).strip()
            if not (team1 and team2):
                continue

            # фільтр тільки NaVi
            joined = f"{team1} {team2}"
            if not (re.search(TEAM_PATTERNS[0], joined, re.I) or re.search(TEAM_PATTERNS[1], joined, re.I)):
                continue

            # формат та турнір
            bo = ""
            mbo = re.search(r'\b(?:bo\s*?(\d)|best\s*of\s*(\d))\b', raw_text, re.I)
            if mbo:
                bo = f"Bo{mbo.group(1) or mbo.group(2)}"
            tournament = ""
            ev = el.select_one(".event-name, .event, a[href^='/events/']")
            if ev:
                tournament = ev.get_text(" ", strip=True)

            # лінк на матч
            a = el.select_one("a[href^='/matches/']")
            match_link = f"https://www.hltv.org{a['href']}" if a and a.has_attr("href") else base_url

            end_local = start_local + timedelta(hours=EVENT_DURATION_HOURS)
            matches.append({
                "summary": f"{team1} vs {team2}" + (f" ({bo})" if bo else "") + (f" — {tournament}" if tournament else ""),
                "start_dt_str": start_local.strftime("%Y-%m-%dT%H:%M:%S"),
                "end_dt_str": end_local.strftime("%Y-%m-%dT%H:%M:%S"),
                "link": match_link
            })

    return matches

def get_navi_matches():
    # 1) desktop через cloudscraper/requests
    html = fetch_html(HLTV_DESKTOP)
    if html:
        m = parse_hltv_matches_page(html, HLTV_DESKTOP)
        print(f"[INFO] HLTV desktop parsed: {len(m)}")
        if m:
            return m
    # 2) mobile fallback
    htmlm = fetch_html(HLTV_MOBILE)
    if htmlm:
        m2 = parse_hltv_matches_page(htmlm, HLTV_MOBILE)
        print(f"[INFO] HLTV mobile parsed: {len(m2)}")
        if m2:
            return m2
    return []

# ================== CALENDAR ==================
def has_duplicate_event(service, calendar_id, start_dt, summary):
    time_min = (start_dt - timedelta(hours=3)).isoformat()
    time_max = (start_dt + timedelta(hours=3)).isoformat()
    items = service.events().list(
        calendarId=calendar_id, timeMin=time_min, timeMax=time_max,
        singleEvents=True, orderBy="startTime",
        q=" vs ".join(summary.split(" vs ")[:2])
    ).execute().get("items", [])
    for ev in items:
        if ev.get("summary","").startswith(" vs ".join(summary.split(" vs ")[:2])):
            print(f"[SKIP] Duplicate: {ev.get('summary')} at {ev.get('start',{}).get('dateTime')}")
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
            "description": f"Auto-added from {HLTV_DESKTOP}\nMatch page: {m['link']}",
            "start": {"dateTime": m["start_dt_str"], "timeZone": TIMEZONE},
            "end":   {"dateTime": m["end_dt_str"],   "timeZone": TIMEZONE},
        }
        print("[DEBUG] Prepared event:", event)
        res = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
        print(f"[CREATED] {m['summary']} → {res.get('htmlLink','')}")
        created += 1
    return created

# ================== ENTRY ==================
def main():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing.")
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/calendar"])
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    print(f"[CHECK] Source: HLTV /matches via cloudscraper; TZ={TIMEZONE}")
    matches = get_navi_matches()
    print(f"[INFO] Total NAVI matches parsed: {len(matches)}")
    if not matches:
        print("[WARN] No NAVI matches parsed from HLTV /matches (desktop+mobile).")
        return

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")

if __name__ == "__main__":
    main()
