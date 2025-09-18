import os
import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import pytz

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============ CONFIG ============
HLTV_URL = "https://www.hltv.org/team/4608/natus-vincere#tab-matchesBox"
TIMEZONE = "Europe/Kyiv"
CALENDAR_ID = (os.environ.get("CALENDAR_ID") or "").strip() or "primary"
EVENT_DURATION_HOURS = 2
LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "60"))
STRICT_DUP_CHECK = (os.environ.get("STRICT_DUP_CHECK", "true").lower() in ("1","true","yes"))

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

TZ_LOCAL = pytz.timezone(TIMEZONE)

# ============ HTTP ============
def make_session():
    s = requests.Session()
    retry = Retry(
        total=TOTAL_RETRIES, connect=TOTAL_RETRIES, read=TOTAL_RETRIES,
        backoff_factor=BACKOFF_FACTOR, status_forcelist=STATUS_FORCELIST,
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False, respect_retry_after_header=True,
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
    time.sleep(random.uniform(0.3, 1.0))
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

# ============ HLTV parsing ============
def _unix_ms_to_kyiv(ms: int) -> datetime:
    dt_utc = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt_utc.astimezone(TZ_LOCAL)

def _extract_text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""

def parse_hltv_upcoming():
    """
    Парсимо секцію матчів зі сторінки команди HLTV:
    - шукаємо всі блоки з мітками часу data-unix (мілісекунди)
    - піднімаємось до контейнера матчу та витягуємо команди, BO, турнір
    """
    html = fetch_html(HLTV_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # На HLTV таймери часто у span/div із атрибутом data-unix
    timers = soup.select("[data-unix]")
    print(f"[INFO] HLTV timer elements (data-unix) found: {len(timers)}")

    matches = []
    now_local = datetime.now(TZ_LOCAL)
    horizon = now_local + timedelta(days=LOOKAHEAD_DAYS)

    for idx, timer in enumerate(timers, start=1):
        unix_raw = timer.get("data-unix")
        if not unix_raw or not unix_raw.isdigit():
            continue
        start_local = _unix_ms_to_kyiv(int(unix_raw))

        # фільтр часу
        if start_local < now_local - timedelta(hours=3):
            continue
        if start_local > horizon:
            continue

        # піднімемось угору у пошуках контейнера матчу (5 рівнів достатньо)
        node = timer
        container = None
        for _ in range(6):
            if node is None:
                break
            # у контейнері зазвичай є лінк на /matches/.. і блоки з назвами команд/івенту
            if node.select_one('a[href^="/matches/"]'):
                container = node
                break
            node = node.parent
        if container is None:
            continue

        # команди: на HLTV часто .team, .opponent, .text-ellipsis тощо
        team_names = [ _extract_text(t) for t in container.select(".team, .opponent, .text-ellipsis") ]
        # відсіяти сміття, залишити короткі назви команд
        team_names = [t for t in (n.strip() for n in team_names) if 1 <= len(t) <= 40]
        # інколи в контейнері багато елементів — спробуємо знайти дві різні назви
        team1, team2 = None, None
        seen = []
        for t in team_names:
            if t and t not in seen:
                seen.append(t)
        if len(seen) >= 2:
            team1, team2 = seen[0], seen[1]
        else:
            # fallback: regex по тексту
            raw = container.get_text(" ", strip=True)
            m_vs = re.search(r'([A-Za-z0-9\'\-\.\s]{2,40})\s+vs\s+([A-Za-z0-9\'\-\.\s]{2,40})', raw, flags=re.I)
            if m_vs:
                team1, team2 = m_vs.group(1).strip(), m_vs.group(2).strip()
        if not (team1 and team2):
            continue

        # фільтруємо лише матчі з участю NAVI
        if "navi" not in (team1 + team2).lower() and "natus vincere" not in (team1 + team2).lower():
            continue

        # формат: шукаємо "bo3"/"Best of 3" поруч
        bo = ""
        raw_cont = container.get_text(" ", strip=True)
        m_bo = re.search(r'\b(?:bo\s*?(\d)|best\s*of\s*(\d))\b', raw_cont, flags=re.I)
        if m_bo:
            bo_num = m_bo.group(1) or m_bo.group(2)
            bo = f"Bo{bo_num}"

        # турнір: часто є лінк із класами event-name / a[href^="/events/"]
        event_el = container.select_one('.event-name, a[href^="/events/"]')
        tournament = _extract_text(event_el)

        end_local = start_local + timedelt
