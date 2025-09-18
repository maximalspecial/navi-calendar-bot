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

def _normalize_month_day(text: str) -> str:
    """
    Нормалізує 'sep 18' → 'Sep 18', 'september 18' → 'September 18', прибирає зайві крапки/пробіли.
    """
    t = (text or "").strip().replace(".", " ")
    t = re.sub(r"\s+", " ", t)
    # титл-кейс дає 'Sep 18'/'September 18' навіть із 'sep 18'
    return t.title()

def _infer_year_from_href_or_today(href: str, month: int, day: int, tz) -> int:
    """
    Якщо в href є ...-DD-MM-YYYY — беремо його. Інакше — беремо поточний рік у Києві.
    Якщо така дата вже минула → переносимо на наступний рік.
    """
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})$", href or "")
    if m:
        return int(m.group(3))

    today_local = datetime.now(tz).date()
    candidate = date(today_local.year, month, day)
    if candidate < today_local:
        return today_local.year + 1
    return today_local.year

def parse_upcoming_matches():
    """
    Парсить матчі NaVi з bo3.gg (будь-які .table-row).
    Повертає список словників зі строковими dateTime без офсету.
    """
    html = fetch_html(BO3_URL)
    if not html:
        print("[WARN] No HTML fetched, skipping parse.")
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Забираємо всі рядки матчів
    rows = soup.select(".table-row")
    print(f"[INFO] Rows matched (any .table-row): {len(rows)}")

    matches = []
    tz_local = pytz.timezone(TIMEZONE)

    for idx, row in enumerate(rows, start=1):
        row_classes = row.get("class", [])
        a = row.select_one('a.c-global-match-link.table-cell[href]')
        if not a:
            # можливі інші типи рядків (без посилання на матч)
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

        # Формат серії
        bo_el = row.select_one(".bo-type")
        bo = bo_el.get_text(strip=True) if bo_el else ""

        # Турнір
        tour_el = row.select_one(".table-cell.tournament .tournament-name")
        tournament = tour_el.get_text(strip=True) if tour_el else ""

        # Час і дата (можуть бути 'sep 18' без року і у нижньому регістрі)
        time_el = row.select_one(".date .time")
        time_text = time_el.get_text(strip=True) if time_el else None

        date_el = row.select_one(".date")
        date_text = None
        if date_el:
            raw = date_el.get_text(" ", strip=True)
            if time_text:
                raw = raw.replace(time_text, "").strip()
            date_text = _normalize_month_day(raw)  # -> 'Sep 18' / 'September 18'

        print(f"[DEBUG] Row {idx}: classes={row_classes}, href={href}, date_text={date_text}, time_text={time_text}")

        if not (date_text and time_text):
            continue  # без дати чи часу сенсу немає

        # Парсимо місяць/день із нормалізованого тексту
        parsed_md = None
        for fmt in ("%b %d", "%B %d"):
            try:
                parsed_md = datetime.strptime(date_text, fmt)
                break
            except ValueError:
                continue
        if not parsed_md:
            print(f"[WARN] Row {idx}: could not parse month/day '{date_text}'")
            continue

        # Визначаємо рік
        year_int = _infer_year_from_href_or_today(href, parsed_md.month, parsed_md.day, tz_local)

        # Будуємо naive datetime з розпаршеного часу
        try:
            hh, mm = time_text.split(":")
            start_naive = datetime(year_int, parsed_md.month, parsed_md.day, int(hh), int(mm))
        except Exception as e:
            print(f"[ERROR] Row {idx}: time parse failed '{time_text}' → {e}")
            continue

        # Конвертація часових поясів
        if SCRAPED_TIME_IS_UTC:
            start_local = pytz.utc.localize(start_naive).astimezone(tz_local)
            print(f"[TZ] Row {idx}: UTC→Kyiv {start_local.isoformat()}")
        else:
            start_local = tz_local.localize(start_naive)
            print(f"[TZ] Row {idx}: as Kyiv {start_local.isoformat()}")

        end_local = start_local + timedelta(hours=2)

        # Для Google Calendar — dateTime БЕЗ офсету
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
