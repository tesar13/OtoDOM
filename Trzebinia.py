import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
import os
import random
import json

BASE_URL = "https://www.otodom.pl"
SEARCH_URL = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/malopolskie/chrzanowski/trzebinia/trzebinia?distanceRadius=15&limit=72&ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC"
LOCATION_NAME = "Trzebinia"  # Używamy tylko "Trzebinia" w nazwie pliku
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_DIR = os.path.join(BASE_DIR, "data_excel")
os.makedirs(EXCEL_DIR, exist_ok=True)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Funkcja wysyłająca plik na Telegrama
def send_to_telegram(file_path, max_retries=5, delay=10):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    for attempt in range(1, max_retries + 1):
        try:
            with open(file_path, "rb") as f:
                files = {"document": f}
                data = {"chat_id": TELEGRAM_CHAT_ID}
                response = requests.post(url, data=data, files=files, timeout=60)
            if response.status_code == 200:
                print("Plik Excel został pomyślnie wysłany na Telegrama.")
                return
            else:
                print(f"Błąd wysyłania na Telegrama (próba {attempt}): {response.status_code} – {response.text}")
        except Exception as e:
            print(f"Wyjątek podczas wysyłania na Telegrama (próba {attempt}): {e}")
        
        if attempt < max_retries:
            print(f"Ponawiam za {delay} sekund...")
            time.sleep(delay)
    
    print("Nie udało się wysłać pliku po wszystkich próbach.")

# ---------- requests session ----------
session = requests.Session()
retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[403, 429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

def random_delay(a=2.0, b=4.0):
    time.sleep(random.uniform(a, b))

def extract_next_data(html, url=""):
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        raise RuntimeError(
            f"Brak __NEXT_DATA__ w HTML. "
            f"Najprawdopodobniej wersja antybot lub niepełny HTML. URL: {url}"
        )
    return json.loads(script.string)

# ---------- MAIN ----------
def main():
    collected = []
    
    # Pobierz pierwszą stronę
    r = session.get(SEARCH_URL, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        print(f"Błąd pobierania strony głównej: {r.status_code}")
        return
    
    data = extract_next_data(r.text)
    total_pages = data["props"]["pageProps"]["data"]["searchAds"]["pagination"]["totalPages"]
    offers = data["props"]["pageProps"]["data"]["searchAds"]["items"]
    print(f"Znaleziono ofert na pierwszej stronie: {len(offers)} | Całkowita liczba stron: {total_pages}")
    
    process_offers(offers, collected)
    
    # Paginacja
    for page in range(2, total_pages + 1):
        page_url = f"{SEARCH_URL}&page={page}"
        random_delay(4, 8)
        r = session.get(page_url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            print(f"Błąd pobierania strony {page}: {r.status_code}")
            continue
        data = extract_next_data(r.text)
        offers = data["props"]["pageProps"]["data"]["searchAds"]["items"]
        print(f"Strona {page}: Znaleziono ofert {len(offers)}")
        process_offers(offers, collected)
    
    if collected:
        df = pd.DataFrame(collected)
        today = datetime.datetime.now().strftime("%d.%m")  # Format DD.MM
        sheet_name = today  # Nazwa arkusza = data
        filename = f"{LOCATION_NAME}_{today}.xlsx"
        full_path = os.path.join(EXCEL_DIR, filename)
        
        # Zapis do Excela z własną nazwą arkusza
        with pd.ExcelWriter(full_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Zapisano {len(df)} ofert → {filename}")
        
        # Wysyłka na Telegrama
        send_to_telegram(full_path)
    else:
        print("Brak ofert do zapisania.")

def process_offers(offers, collected):
    for idx, offer in enumerate(offers):
        offer_id = offer.get("id")
        slug = offer.get("slug")

        base36_id = slug.split('-ID')[-1] if '-ID' in slug else ''
        if base36_id:
            try:
                offer_id = int(base36_id, 36)  # Konwersja base36 na int (poprawne ID)
            except ValueError:
                print(f"Błąd konwersji base36 dla slug: {slug}. Używam fallback.")
                offer_id = offer.get("id")
            else:
                offer_id = offer.get("id")  # Fallback, jeśli brak '-ID' w slug (rzadkie)
        
        # Cena i waluta
        price_dict = offer.get("totalPrice", {}) or offer.get("price", {})
        price = price_dict.get("value") or price_dict.get("amount")
        currency = price_dict.get("currency")
        
        # Lokalizacja – bezpieczne wyciągnięcie
        location_address = offer.get("location", {}).get("address", {})
        location_city = ""
        location_street = ""
        
        if location_address:
            city_obj = location_address.get("city")
            if city_obj and isinstance(city_obj, dict):
                location_city = city_obj.get("name", "")
            
            street_obj = location_address.get("street")
            if street_obj and isinstance(street_obj, dict):
                location_street = street_obj.get("name", "")
        
        location = location_city
        if location_street:
            location = location_city + ", " + location_street
        
        # Powierzchnia
        area = offer.get("areaInSquareMeters")
        
        # Data dodania
        created_at_first = offer.get("createdAtFirst")
        data_dodania = ""
        if created_at_first:
            created_at_first = created_at_first.rstrip('Z')
            try:
                dt = datetime.datetime.fromisoformat(created_at_first)
                data_dodania = dt.date().strftime("%Y-%m-%d")
            except ValueError:
                data_dodania = created_at_first[:10]
        
        
        # Szczegóły z strony szczegółów
        ogrzewanie = ""
        pietro = ""
        pokoje = ""
        rok_budowy = ""
        wyposazenie = ""
        opis = ""
        rynek = ""
        detail_url = f"https://www.otodom.pl/pl/oferta/{slug.lstrip('/')}"
        random_delay(2, 4)

        def extract_floor_from_ad(ad):
            """
            Szuka wartości floor_no:: w całej strukturze ogłoszenia (rekurencyjnie)
            Zwraca np. '3/4', '0', '2/5' albo ''
            """

            found_floor = None
            found_total = ""

            def walk(obj):
                nonlocal found_floor, found_total

                if isinstance(obj, dict):
                    for v in obj.values():
                        walk(v)

                elif isinstance(obj, list):
                    for item in obj:
                        walk(item)

                elif isinstance(obj, str):
                    if obj.startswith("floor_no::"):
                        if "ground_floor" in obj:
                            found_floor = "0"
                        elif "floor_" in obj:
                            found_floor = obj.split("floor_")[-1]

                    elif obj.startswith("/") and found_floor:
                        found_total = obj

            walk(ad)

            if found_floor:
                return f"{found_floor}{found_total}".strip()

            return ""
        
        try:
            r_detail = session.get(detail_url, headers=HEADERS, timeout=30)
            if r_detail.status_code == 200:
                data_detail = extract_next_data(r_detail.text, detail_url)
                ad = data_detail["props"]["pageProps"]["ad"]

                detail_id = ad.get("id")
                if detail_id and detail_id != offer_id:
                    offer_id = detail_id  # Nadpisz na ID z detali, jeśli dostępne i różne
                
                # Opis - oczyść z HTML
                html_desc = ad.get("description", "").strip()
                opis = BeautifulSoup(html_desc, 'html.parser').get_text(separator=' ', strip=True)
                
                # Słownik dla rynku
                rynek_map = {
                    "PRIMARY": "pierwotny",
                    "SECONDARY": "wtórny"
                }
                rynek = rynek_map.get(ad.get("market", ""), "")

            
                # ---------- PIĘTRO (global search – odporne) ----------
                pietro = extract_floor_from_ad(ad)


                # ---------- OGRZEWANIE (characteristics → localizedValue) ----------
                ogrzewanie = ""

                for char in ad.get("characteristics", []):
                    if char.get("key") == "heating":
                        ogrzewanie = char.get("localizedValue", "")
                        break


                # ---------- WYPOSAŻENIE (jeśli dostępne) ----------
                for group in ad.get("featuresByCategory", []):
                    label = group.get("label", "").lower()

                    if "wyposaż" in label:
                        wyposazenie = ", ".join(group.get("values", []))


                # Pobierz z topInformation i additionalInformation (dla pokoi, roku budowy)
                info_sections = ad.get("topInformation", []) + ad.get("additionalInformation", [])
                for info in info_sections:
                    label = info.get("label")
                    values = info.get("values", [])
                    if label == "rooms_num" and values:
                        pokoje = values[0]  # Np. "2"
                    elif label == "build_year" and values:
                        rok_budowy = values[0]  # Np. "1990"
                

                
        except Exception as e:
            print(f"Błąd przy pobieraniu szczegółów dla ID {offer_id}: {e}")
            opis = "Błąd pobierania opisu"
        
        collected.append({
            "ID": offer_id,
            "Link": detail_url,
            "Cena": price,
            "Waluta": currency,
            "Lokalizacja": location,
            "Powierzchnia": area,
            "Ogrzewanie": ogrzewanie,
            "Piętro": pietro,
            "Pokoje": pokoje,
            "Rok budowy": rok_budowy,
            "Wyposażenie": wyposazenie,
            "Opis": opis,
            "Rynek": rynek,
            "Data dodania": data_dodania
        })
        
        random_delay(1.5, 3.5)

if __name__ == "__main__":
    main()



