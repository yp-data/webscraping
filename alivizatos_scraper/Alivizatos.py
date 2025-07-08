import requests
import time
import re
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from tabulate import tabulate

# PostgreSQL Connection Details
DB_PARAMS = {
    "dbname": "vehicle_listing_db",
    "user": "postgres",
    "password": "enter your password",  # Change this to your actual password
    "host": "localhost",
    "port": "5432"
}

# Headers to avoid bot detection
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

BASE_URL = "https://alivizatos.gr"
PAGINATION_URL = "https://alivizatos.gr/search/cars/?current-page={}&sort-by=newest"

# Cleaning function
def clean_text(text):
    """Removes newlines, excess spaces, and special characters from text."""
    return re.sub(r'[\n\r\t]+', ' ', text).strip() if text else None

# Function to fetch car URLs
def get_car_urls():
    car_urls = []
    page = 1

    while True:
        try:
            print(f"Fetching page {page}...")
            response = requests.get(PAGINATION_URL.format(page), headers=HEADERS, timeout=5)
            response.raise_for_status()  # Raise error for bad responses
        except requests.exceptions.Timeout:
            print(f"Timeout on page {page}, stopping...")
            break
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        cars = soup.find_all("a", class_="vehica-car-card-link")

        if not cars:
            print("No more cars found, stopping pagination.")
            break

        for car in cars:
            car_url = car["href"]
            if not car_url.startswith("http"):
                car_url = BASE_URL + car_url
            car_urls.append(car_url)

        page += 1
        time.sleep(2)

    return car_urls

# Function to extract Offer ID
def extract_offer_id(soup):
    offer_id_div = soup.find("div", class_="vehica-car-offer-id")
    return clean_text(offer_id_div.text.replace("Offer ID #", "")) if offer_id_div else None

# Function to extract car title
def get_title(soup):
    title_div = soup.find("div", class_="vehica-car-name")
    return clean_text(title_div.text) if title_div else None

# Function to extract car price
def get_price(soup):
    price_div = soup.find("div", class_="vehica-car-price")
    return clean_text(price_div.text) if price_div else None

# Function to extract attributes
def get_car_attributes(soup):
    attributes = {}
    attribute_divs = soup.find_all("div", class_="vehica-grid__element")

    for div in attribute_divs:
        key_div = div.find("div", class_="vehica-car-attributes__name")
        value_div = div.find("div", class_="vehica-car-attributes__values")
        if key_div and value_div:
            key = clean_text(key_div.text)
            value = clean_text(value_div.text)
            attributes[key] = value

    return attributes

# Function to extract features
def get_features(soup):
    features_divs = soup.find_all("span", class_="vehica-car-list__element__inner")
    features = [clean_text(feature.text) for feature in features_divs if feature.text.strip()]
    return " || ".join(features) if features else None

# Function to scrape car details
def scrape_car_details(car_url):
    try:
        response = requests.get(car_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {car_url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    car_data = {
        "adid": extract_offer_id(soup),
        "car_url": car_url,
        "title": get_title(soup),
        "price": get_price(soup),
        "features": get_features(soup),
        "date_scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    attributes = get_car_attributes(soup)

    # Attribute Mapping
    mapping = {
        "Μάρκα:": "brand",
        "Μοντέλο:": "model",
        "Κίνηση:": "drivetrain",
        "Σασμάν:": "transmission",
        "Κατάσταση:": "condition",
        "Χρονολογία:": "year",
        "Χιλιόμετρα:": "mileage",
        "Καύσιμο:": "fuel",
        "Κυβικά:": "engine_size",
        "Ίπποι:": "horsepower",
        "Καθίσματα:": "seats",
        "Πόρτες:": "doors",
        "Αερόσακοι:": "airbags",
        "Χρώμα Εξωτερικό:": "exterior_color",
        "Χρώμα Εσωτερικού:": "interior_color",
        "Κλάση ρύπων:": "emissions_class",
        "Ρύποι (co2):": "co2_emissions",
        "Κατανάλωση εντός πόλης:": "city_consumption"
    }

    for key, field in mapping.items():
        if key in attributes:
            car_data[field] = attributes[key]

    return car_data

# Function to create the PostgreSQL table
def create_table():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS car_details (
            id SERIAL PRIMARY KEY,
            adid VARCHAR(50),
            car_url TEXT UNIQUE,  -- Ensure URLs are unique
            title TEXT,
            price TEXT,
            features TEXT,
            date_scraped TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            adstatus TEXT DEFAULT 'active'  -- Add adstatus column with default value
        )
    """)

    # Ensure adstatus column exists if it wasn't before
    cur.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                           WHERE table_name='car_details' AND column_name='adstatus') 
            THEN
                ALTER TABLE car_details ADD COLUMN adstatus TEXT DEFAULT 'active';
            END IF;
        END $$;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Table 'car_details' checked/created successfully.")


def get_existing_cars():
    """Fetches existing car URLs and their adstatus from the database."""
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    cur.execute("SELECT car_url, adstatus FROM car_details")
    existing_cars = {row[0]: row[1] for row in cur.fetchall()}  # Store in dictionary
    
    cur.close()
    conn.close()
    return existing_cars

def store_in_database(car_data, existing_cars):
    """Stores new car data in PostgreSQL while handling adstatus logic."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        # Determine adstatus for the current run
        car_url = car_data["car_url"]
        
        if car_url in existing_cars:
            if existing_cars[car_url] == "inactive":
                return  # Skip inserting if it's already marked as inactive
            adstatus = "active"
        else:
            adstatus = "active"  # Default to active for new listings

        car_data["adstatus"] = adstatus  # Add adstatus to data

        # Insert into the database
        cur.execute("""
            INSERT INTO car_details (
                adid, car_url, title, price, features, date_scraped, adstatus
            ) VALUES (
                %(adid)s, %(car_url)s, %(title)s, %(price)s, %(features)s, %(date_scraped)s, %(adstatus)s
            )
        """, car_data)

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Data for {car_data['title']} inserted into PostgreSQL with status '{adstatus}'.")

    except Exception as e:
        print(f"❌ Database Insert Error: {e}")

def mark_missing_as_inactive(existing_cars, current_urls):
    """Marks car URLs that are missing in the latest run as inactive."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        missing_urls = set(existing_cars.keys()) - set(current_urls)

        for car_url in missing_urls:
            if existing_cars[car_url] != "inactive":  # Only update if not already inactive
                cur.execute("""
                    INSERT INTO car_details (car_url, date_scraped, adstatus)
                    VALUES (%s, %s, 'inactive')
                """, (car_url, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                print(f"❗ Marked {car_url} as INACTIVE.")

        conn.commit()
        cur.close()
        conn.close()
    
    except Exception as e:
        print(f"❌ Error marking missing cars as inactive: {e}")


# ✅ Main Execution
print("\n🚀 Starting Scraper...\n")

# Create table if it doesn't exist
create_table()

# Fetch existing car listings
existing_cars = get_existing_cars()

# Get car URLs from the latest scrape
car_urls = get_car_urls()
scraped_data = [scrape_car_details(car_url) for car_url in car_urls if car_url]

# Remove None values
scraped_data = [car for car in scraped_data if car is not None]

# ✅ Store Data in PostgreSQL
for car in scraped_data:
    store_in_database(car, existing_cars)

# ✅ Mark missing listings as inactive
mark_missing_as_inactive(existing_cars, car_urls)

# ✅ Display results
df = pd.DataFrame(scraped_data)
if not df.empty:
    print("\n📊 Table View of First 5 Listings:\n")
    print(tabulate(df.head(5), headers="keys", tablefmt="psql"))

# ✅ Save to CSV
df.to_csv("scraped_car_listings.csv", index=False)
print("\n✅ Data saved to 'scraped_car_listings.csv'")
