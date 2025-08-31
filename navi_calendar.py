import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def main():
    # Дані календаря
    CALENDAR_ID = os.environ["CALENDAR_ID"]

    # Авторизація через Service Account
    creds = Credentials.from_service_account_info(
        eval(os.environ["GOOGLE_CREDENTIALS_JSON"]),
        scopes=["https://www.googleapis.com/auth/calendar"]
    )
    service = build("calendar", "v3", credentials=creds)

    # Парсимо сайт
    url = "https://navi.gg/ua/matches/cs2"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Знаходимо перший матч
    match = soup.select_one("a.match-card")
    if not match:
        print("Матчів не знайдено")
        return

    title = match.select_one(".match-card__title").get_text(strip=True)
    date = match.select_one(".match-card__date").get_text(strip=True)

    # Дістаємо посилання, щоб витягнути час
    match_url = "https://navi.gg" + match["href"]
    r2 = requests.get(match_url)
    soup2 = BeautifulSoup(r2.text, "html.parser")
    time_text = soup2.select_one(".match-info__date").get_text(strip=True)

    # Об’єднуємо дату й час
    dt = datetime.strptime(time_text, "%d %B %Y, %H:%M")
    start = dt.isoformat()
    end = (dt + timedelta(hours=2)).isoformat()

    # Створюємо подію
    event = {
        "summary": f"NAVI CS2: {title}",
        "start": {"dateTime": start, "timeZone": "Europe/Kyiv"},
        "end": {"dateTime": end, "timeZone": "Europe/Kyiv"},
    }
    service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
    print("Подія створена:", title)

if __name__ == "__main__":
    main()
