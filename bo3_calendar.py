import os, re, json, time, random, requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import pytz

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

HLTV_GLOBAL = "https://www.hltv.org/matches"
HLTV_GLOBAL_M = "https://m.hltv.org/matches"
TEAM_NAMES = (r"Natus\s+Vincere", r"\bNAVI\b")

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
]
READ_TIMEOUT = 45
CONNECT_TIMEOUT = 10
TOTAL_RETRIES = 5
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429,500,502,503,504)

TZ_LOCAL = pytz.timezone(TIMEZONE)

# ================== HTTP ==================
def make_session():
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
    sess = make_session()
    time.sleep(random.uniform(0.3,1.0))
    try:
        r = sess.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if r.status_code != 200:
            print(f"[WARN] HTTP {r.status_code} for {url}")
            return None
        txt = r.text
        print(f"[INFO] Fetched {url} ({len(txt)} bytes)")
        # детект Cloudflare/челендж
        if re.search(r"Attention Required|cloudflare|cf-browser-verification", txt, flags=re.I):
            print(f"[WARN] Looks like a bot-protection/challenge page at {url}")
        return txt
    except requests.RequestException as e:
        print(f"[ERROR] Fetch failed {url}: {e}")
        return None

# ================== PARSERS ==================
def unix_ms_to_kyiv(ms: int) -> datetime:
    dt_utc = datetime.fromtimestamp(ms/1000.0, tz=timezone.utc)
    return dt_utc.astimezone(TZ_LOCAL)

def parse_time_str_to_today(time_str: str) -> tuple[datetime, bool] | None:
    """
    '21:00' -> сьогодні о 21:00 за локальним (Kyiv). HLTV /matches групує по днях,
    але у HTML цей день є текстом заголовка секції; тут ми просто будуємо 'сьогодні',
    а нижче підкоригуємо датою секції, якщо вдалось її прочитати.
    Повертає (datetime, is_naive), де is_naive=True якщо дата без дня.
    """
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", time_str)
    if not m: return None
    hh, mm = int(m.group(1)), int(m.group(2))
    today = datetime.now(TZ_LOCAL)
    dt = TZ_LOCAL.localize(datetime(today.year, today.month, today.day, hh, mm))
    return dt, True

def extract_section_date(header_text: str) -> datetime | None:
    """
    Із заголовка секції типу 'Thursday - 2025-09-18' або 'Thursday, September 18' дістаємо дату.
    """
    t = " ".join((header_text or "").split())
    # ISO у хедері (часто є yyyy-mm-dd)
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', t)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return TZ_LOCAL.localize(datetime(y, mo, d, 0, 0))
        except Exception:
            return None
    # Англ формат 'September 18, 2025' або 'September 18'
    m = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:,\s*(\d{4}))?', t, flags=re.I)
    if m:
        month_name, day, year = m.group(1), int(m.group(2)), m.group(3)
        month_map = {m:i for i,m in enumerate(["","January","February","March","April","May","June","July","August","September","October","November","December"])}
        mo = month_map[month_name.capitalize()]
        y  = int(year) if year else datetime.now(TZ_LOCAL).year
        try:
            return TZ_LOCAL.localize(datetime(y, mo, day, 0, 0))
        except Exception:
            return None
    return None

def parse_hltv_matches_page(html: str, base_url: str):
    """
    Парсимо /matches (desktop або mobile):
    - шукаємо заголовки секцій (дати),
    - всередині — блоки матчів з командами та часом (data-unix або текст HH:MM),
    - фільтруємо тільки матчі з NAVI.
    """
    soup = BeautifulSoup(html, "html.parser")
    matches = []
    now_local = datetime.now(TZ_LOCAL)
    horizon = now_local + timedelta(days=LOOKAHEAD_DAYS)

    # HLTV часто має секції із заголовком дня. Для desktop: h2.eventDayHeader або подібне.
    # Для mobile можуть бути прості <div class="standard-box ..."> з датою в тексті.
    # Візьмемо всі хедери, що містять дату, а якщо не знайдемо — просто пройдемо всі матчі без прив’язки до дня.
    sections = []

    # 1) ймовірні хедери
    for h in soup.select("h2, h3, .match-day, .eventday-headline, .eventDayHeadline, .eventDayHeader"):
        txt = h.get_text(" ", strip=True)
        if re.search(r'\d{4}-\d{2}-\d{2}', txt) or re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}', txt, flags=re.I):
            sections.append((h, extract_section_date(txt)))

    # 2) якщо немає розмічених секцій, візьмемо один «віртуальний» розділ із усім документом
    if not sections:
        sections = [(soup, None)]

    for head, section_date in sections:
        # межі секції = від header до наступного header того ж рівня
        parent = head if head is soup else head.parent
        # усередині шукаємо матчі — елементи, що мають data-unix або час + команди
        items = (parent.select('[data-unix]') or parent.select('.match, .upcomingMatch, .upcoming, .match-day, .matchbox'))
        # якщо це soup увесь — додатково спробуємо знайти всі елементи з data-unix
        if head is soup:
            extra = soup.select('[data-unix]')
            if extra:
                items = list(dict.fromkeys(items + extra))  # унікалізація

        for el in items:
            # 1) час
            start_local = None
            if el.has_attr("data-unix") and str(el.get("data-unix","")).isdigit():
                start_local = unix_ms_to_kyiv(int(el["data-unix"]))
            else:
                # текстовий час у дочірніх елементах
                t_candidate = None
                for node in el.select(".time, .matchTime, .match-time, .when, time"):
                    s = node.get_text(" ", strip=True)
                    if re.match(r'^\d{1,2}:\d{2}$', s):
                        t_candidate = s
                        break
                if t_candidate:
                    parsed = parse_time_str_to_today(t_candidate)
                    if parsed:
                        start_local, is_naive = parsed
                        # якщо відомий заголовок секції з датою — підставимо дату з нього
                        if is_naive and section_date:
                            start_local = TZ_LOCAL.localize(datetime(
                                section_date.year, section_date.month, section_date.day,
                                start_local.hour, start_local.minute
                            ))

            if not start_local:
                continue

            # фільтр за часом
            if start_local < now_local - timedelta(hours=3):  # минулі — не беремо
                continue
            if start_local > horizon:
                continue

            # 2) команди
            raw_text = el.get_text(" ", strip=True)
            # спроба знайти дві команди поблизу
            teams = [t.get_text(" ", strip=True) for t in el.select(".team, .opponent, .teamname, .text-ellipsis, .team1, .team2")]
            teams = [t for t in teams if 1 <= len(t) <= 40]
            # унікалізуємо збереженням порядку
            uniq = []
            for t in teams:
                if t and t not in uniq:
                    uniq.append(t)
            team1, team2 = None, None
            if len(uniq) >= 2:
                team1, team2 = uniq[0], uniq[1]
            else:
                mvs = re.search(r'([A-Za-z0-9\'\-\.\s]{2,40})\s+vs\s+([A-Za-z0-9\'\-\.\s]{2,40})', raw_text, flags=re.I)
                if mvs:
                    team1, team2 = mvs.group(1).strip(), mvs.group(2).strip()
            if not (team1 and team2):
                continue

            # 3) фільтр тільки NaVi
            joined = f"{team1} {team2}"
            if not (re.search(TEAM_NAMES[0], joined, flags=re.I) or re.search(TEAM_NAMES[1], joined, flags=re.I)):
                continue

            # 4) формат (Bo3) і турнір
            bo = ""
            mbo = re.search(r'\b(?:bo\s*?(\d)|best\s*of\s*(\d))\b', raw_text, flags=re.I)
            if mbo:
                bo = f"Bo{mbo.group(1) or mbo.group(2)}"
            tournament = ""
            ev = el.select_one(".event-name, .event, a[href^='/events/']")
            if ev:
                tournament = ev.get_text(" ", strip=True)

            # 5) лінк на матч
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
    # 1) desktop
    html = fetch_html(HLTV_GLOBAL)
    if html:
        m = parse_hltv_matches_page(html, HLTV_GLOBAL)
        print(f"[INFO] HLTV desktop parsed: {len(m)}")
        if m: return m
    # 2) mobile fallback
    htmlm = fetch_html(HLTV_GLOBAL_M)
    if htmlm:
        m2 = parse_hltv_matches_page(htmlm, HLTV_GLOBAL_M)
        print(f"[INFO] HLTV mobile parsed: {len(m2)}")
        if m2: return m2
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
            "description": f"Auto-added from {HLTV_GLOBAL}\nMatch page: {m['link']}",
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

    print(f"[CHECK] Source: HLTV /matches; TZ={TIMEZONE}")
    matches = get_navi_matches()
    print(f"[INFO] Total NAVI matches parsed: {len(matches)}")
    if not matches:
        print("[WARN] No NAVI matches parsed from HLTV /matches (desktop+mobile).")
        return

    created = create_events(service, matches)
    print(f"[DONE] Created {created} events.")

if __name__ == "__main__":
    main()
