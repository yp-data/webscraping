
##version 5 - deduplication 
import time
import random
from datetime import datetime
import re
import urllib.parse
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fuzzywuzzy import fuzz
import psycopg2
import os
import json

from gdh_collect.utils.cleaning import (
    clean_name, slugify, clean_phone, normalize_address,
    extract_domain, to_float
)

# ─────────────────────────────────────────────
# Database Connection for Organization Matching
# ─────────────────────────────────────────────
# Load DB credentials from environment variables
READ_CONN = psycopg2.connect(os.getenv("READ_DB_URL"))
WRITE_CONN = psycopg2.connect(os.getenv("WRITE_DB_URL"))

# ─────────────────────────────────────────────
# 📎 Helper: Get Google Maps Share Link (Short URL)
# ─────────────────────────────────────────────
def get_share_link(driver):
    try:
        share_btn = driver.find_element(By.XPATH, '//button[@aria-label="Share"]')
        driver.execute_script("arguments[0].scrollIntoView(true);", share_btn)
        time.sleep(0.5)
        share_btn.click()
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, 'input.vrsrZe'))
        )
        link_input = driver.find_element(By.CSS_SELECTOR, 'input.vrsrZe')
        short_link = link_input.get_attribute('value')

        overlay = driver.find_element(By.CSS_SELECTOR, 'div.hoUMge')
        close_btn = overlay.find_element(By.CSS_SELECTOR, 'button[aria-label="Close"]')
        driver.execute_script("arguments[0].click();", close_btn)
        time.sleep(0.5)

        return short_link
    except:
        return driver.current_url
# ─────────────────────────────────────────────
# Helper: Dismiss Overlay if Present
# ─────────────────────────────────────────────
def dismiss_overlay(driver):
    try:
        overlay = driver.find_element(By.CSS_SELECTOR, 'div.hoUMge')
        if overlay.is_displayed():
            print("Overlay detected, dismissing...")
            close_btn = overlay.find_element(By.CSS_SELECTOR, 'button[aria-label="Close"]')
            driver.execute_script("arguments[0].click();", close_btn)
            WebDriverWait(driver, 3).until_not(
                lambda d: d.find_element(By.CSS_SELECTOR, 'div.hoUMge').is_displayed()
            )
            return True
    except:
        pass
    return False


# ─────────────────────────────────────────────
# Helper: Close Place Details Pane
# ─────────────────────────────────────────────
def close_place_details(driver):
    try:
        close_btn = driver.find_element(By.CSS_SELECTOR, 'button[jsaction="pane.place.close"]')
        close_btn.click()
        time.sleep(1)
    except:
        pass  # Sometimes details are already closed

# Helper to fetch city queries from the production database
def fetch_city_queries():
    with READ_CONN.cursor() as cur:
        cur.execute("SELECT name, state FROM cities")
        rows = cur.fetchall()
    return [f"garage door repair {city} {state}" for city, state in rows]

def fetch_existing_orgs():
    with READ_CONN.cursor() as cur:
        cur.execute("""
            SELECT id, name, phones, website, addresses
            FROM organizations
        """)
        return cur.fetchall()
    
EXISTING_ORGS = fetch_existing_orgs()

def extract_zip(address):
    match = re.search(r'\b\d{5}(?=\D*$)', address)
    return match.group(0) if match else None
# ─────────────────────────────────────────────
# Deduplication & Fuzzy Organization Matching
# ─────────────────────────────────────────────
def match_existing_org(name, phone, website, address, lat=None, lng=None):
    for org in EXISTING_ORGS:
        org_id, org_name, org_phones, org_website, org_addresses = org
        score = fuzz.token_set_ratio(name.lower(), (org_name or "").lower())
        
        if score >= 85:
            # Direct match on phone
            if phone and org_phones and phone in org_phones:
                return org
            # Domain match
            if website and org_website:
                if extract_domain(website) == extract_domain(org_website):
                    return org
            # Address match
            if address and org_addresses and address in org_addresses:
                return org
            # Geolocation proximity check (simple ~0.001 degree radius)
            if lat and lng and isinstance(org_addresses, dict):
                try:
                    org_lat = float(org_addresses.get('latitude', 0))
                    org_lng = float(org_addresses.get('longitude', 0))
                    if abs(org_lat - lat) <= 0.001 and abs(org_lng - lng) <= 0.001:
                        return org
                except:
                    pass
    return None

# Helper to check if org data has changed
def org_data_changed(existing, scraped):
    _, org_name, org_phones, org_website, org_addresses = existing
    addr = org_addresses.get('address') if isinstance(org_addresses, dict) else org_addresses
    return (
        scraped["Name"].lower() != (org_name or "").lower() or
        (scraped["Phone"] and scraped["Phone"] not in (org_phones or "")) or
        (scraped["Website"] and extract_domain(scraped["Website"]) != extract_domain(org_website)) or
        (scraped["Address"] and scraped["Address"] != (addr or ""))
    )

# ─────────────────────────────────────────────
# Database Insert Helpers
# ─────────────────────────────────────────────
def insert_organization(data):
    try:
        with WRITE_CONN.cursor() as cur:
            cur.execute("""
                INSERT INTO organizations (
                    name, name_raw, slug, phones, website, addresses,
                    google_rating, review_count, avg_rating,
                    source_ids, ids, hours,
                    google_id, review_excerpt, canonical_abbr,
                    geom_m, google_place_url, website_domain, source
                )
                VALUES (
                    %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb,
                    %s::double precision, %s::integer, %s::double precision,
                    %s::jsonb, %s::jsonb, %s::jsonb,
                    %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s
                )
                RETURNING id;
            """, (
                data["Name"],
                data["Name Raw"],
                data["Slug"],
                json.dumps([data["Phone"]]) if data["Phone"] else json.dumps([]),
                json.dumps([data["Website"]]) if data["Website"] else json.dumps([]),
                json.dumps({"address": data["Address"], "zip": data.get("ZIP", "")}),
                float(data["Rating"]) if data.get("Rating") else None,
                int(data["review_count"]) if data.get("review_count") else None,
                float(data["Rating"]) if data.get("Rating") else None,
                json.dumps({"google_place_id": data["Place ID"]}),
                json.dumps({"external": data["Place ID"]}),
                json.dumps({"hours_text": data["Hours"]}) if data.get("Hours") else json.dumps({"hours_text": "Unavailable"}),
                data.get("Place ID"),
                data.get("review_excerpt"),
                data.get("canonical_abbr") or "",
                float(data["Longitude"]) if data.get("Longitude") else None,
                float(data["Latitude"]) if data.get("Latitude") else None,
                data.get("Google Place URL"),
                data.get("Website Domain"),
                data.get("Source")
            ))
            WRITE_CONN.commit()
            return cur.fetchone()[0]
    except Exception as e:
        print(f"DB Insert Error: {e}")
        WRITE_CONN.rollback()
        return None

def insert_review(review_data, org_id):
    try:
        with WRITE_CONN.cursor() as cur:
            cur.execute("""
                INSERT INTO reviews (reviewer, rating, review, source, org_id, source_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_id) DO NOTHING;
            """, (
                review_data["reviewer"],
                review_data["rating"],
                review_data["review"],
                "google_maps",
                org_id,
                review_data["source_id"]
            ))
            WRITE_CONN.commit()
    except Exception as e:
        print(f"DB Review Insert Error: {e}")
        WRITE_CONN.rollback()

# ─────────────────────────────────────────────
# ⏰ Extract Business Hours
# ─────────────────────────────────────────────
def extract_hours(driver):
    try:
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        hours_dict = {day: "Closed" for day in day_order}

        hours_container = driver.find_element(By.CSS_SELECTOR, "div.OqCZI.WVXvdc")
        dropdown_btn = hours_container.find_element(By.CSS_SELECTOR, "div.OMl5r")
        if dropdown_btn.get_attribute("aria-expanded") == "false":
            dropdown_btn.click()
            time.sleep(0.5)

        table = hours_container.find_element(By.CSS_SELECTOR, "div.t39EBf table.eK4R0e")
        rows = table.find_elements(By.TAG_NAME, "tr")

        for row in rows:
            day = row.find_element(By.CSS_SELECTOR, 'td.ylH6lf').text.strip()
            hour_text = row.find_element(By.CSS_SELECTOR, 'td.mxowUb').text.strip()

            # Remove parentheses and holidays
            hour_text = re.sub(r'\(.*?\)', '', hour_text)  # Remove parentheses like (Independence Day)
            hour_text = re.sub(r'(Holiday hours|Hours might differ|Independence Day|Christmas|New Year\'?s Day|Labor Day|Easter|Thanksgiving).*', '', hour_text, flags=re.IGNORECASE)
            hour_text = hour_text.replace('\u202f', ' ').strip()  # Normalize non-breaking spaces
            hour_text = re.sub(r'\s{2,}', ' ', hour_text)

            if day in hours_dict and hour_text:
                hours_dict[day] = hour_text

        return json.dumps(hours_dict)
    except Exception:
        print("Hours not available for this listing.")
        return "Unavailable"
# ─────────────────────────────────────────────
# 📊 Extract Review Count
# ─────────────────────────────────────────────
def extract_review_count(driver):
    try:
        count_el = driver.find_element(By.CSS_SELECTOR, 'div.jANrlb div.fontBodySmall')
        count_text = count_el.text.strip()
        match = re.search(r'([\d,]+)', count_text)
        if match:
            return int(match.group(1).replace(',', ''))
    except:
        pass
    return None

# ─────────────────────────────────────────────
# 📊 Extract All Reviews for a Business
# ─────────────────────────────────────────────
def safe_scroll_reviews(driver):
    print(f"Scrolling reviews for: {driver.current_url}")
    print("Starting review scroll attempts...")
    try:
        dismiss_overlay(driver)
        time.sleep(0.5)
    except Exception as e:
        print(f"Overlay dismiss failed: {e}")

    try:
        tabs = driver.find_elements(By.XPATH, '//button[@role="tab"]')
        for tab in tabs:
            if "Reviews for" in tab.get_attribute("aria-label"):
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab)
                time.sleep(0.5)
                for attempt in range(3):
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, f'//button[@aria-label="{tab.get_attribute("aria-label")}"]'))
                        )
                        driver.execute_script("arguments[0].click();", tab)
                        print(f"Clicked Reviews tab on attempt {attempt + 1}")
                        break
                    except Exception as e:
                        print(f"Attempt {attempt + 1} to click Reviews tab failed: {e}")
                        time.sleep(1)
                else:
                    print("Final fallback: Forcing JS click on Reviews tab")
                    driver.execute_script("arguments[0].click();", tab)
                break

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.PPCwl'))
        )
        print("Clicked 'Reviews' tab, confirming active state...")
        time.sleep(1)
        active_tab = driver.find_element(By.CSS_SELECTOR, 'button.hh2c6[aria-selected="true"]')
        if "Reviews for" in active_tab.get_attribute("aria-label"):
            print("✅ Reviews tab is active")
            time.sleep(1)

            # REVISED SCROLL LOGIC:
            try:
                scrollable_candidates = driver.find_elements(
                    By.CSS_SELECTOR,
                    'div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde'
                )
                for candidate in scrollable_candidates:
                    if candidate.is_displayed() and candidate.find_elements(By.CSS_SELECTOR, 'div.jftiEf.fontBodyMedium'):
                        scrollable = candidate
                        break
                else:
                    print("⚠️ No valid scroll container found for reviews")
                    return False

                expected_count = extract_review_count(driver)
                print(f"Total expected reviews: {expected_count}")

                prev_count = 0
                max_attempts = 100
                scroll_pause = 1.5

                for i in range(max_attempts):
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable)
                    time.sleep(scroll_pause)

                    reviews_now = driver.find_elements(By.CSS_SELECTOR, 'div.jftiEf.fontBodyMedium')
                    # Only print every 5 scrolls or if we've loaded all expected
                    if (i + 1) % 5 == 0 or (expected_count and len(reviews_now) >= expected_count):
                        print(f"Scrolled {i+1} times, loaded {len(reviews_now)}/{expected_count} reviews...")

                    if expected_count and len(reviews_now) >= expected_count:
                        print("Loaded all expected reviews, stopping scroll")
                        break

                    if len(reviews_now) == prev_count:
                        print("No more reviews loaded, stopping scroll")
                        break

                    prev_count = len(reviews_now)

                if expected_count and len(reviews_now) < expected_count:
                    print(f"⚠️ Only loaded {len(reviews_now)} out of {expected_count} expected reviews, possible limitation")
                else:
                    print(f"✅ Loaded {len(reviews_now)} reviews successfully")
                # Print summary after scrolls
                print(f"Total expected: {expected_count} | Total loaded: {len(reviews_now)}")
                return True
            except Exception as e:
                print(f"Failed during review scroll: {e}")
                return False
        else:
            print("⚠️ Reviews tab not active after click")
            return False
    except Exception as e:
        print(f"Failed to click 'Reviews' tab: {e}")
        return False


def extract_reviews(driver):
    reviews_data = []
    if not safe_scroll_reviews(driver):
        print("Skipping review extraction, Reviews tab not accessible.")
        return []
    # After scrolling, scrape all available reviews
    review_elements = driver.find_elements(By.CSS_SELECTOR, 'div.jftiEf.fontBodyMedium')
    print(f"Found {len(review_elements)} reviews")

    failed_reviews = 0
    for rev in review_elements:
        try:
            reviewer = rev.find_element(By.CSS_SELECTOR, 'div.d4r55').text.strip()
            rating_el = rev.find_element(By.CSS_SELECTOR, 'span.kvMYJc')
            rating = float(rating_el.get_attribute("aria-label").split()[0]) if rating_el else None
            date = rev.find_element(By.CSS_SELECTOR, 'span.rsqaWe').text.strip()

            review_text_el = None
            if rev.find_elements(By.CSS_SELECTOR, 'div.MyEned span.wiI7pd'):
                review_text_el = rev.find_element(By.CSS_SELECTOR, 'div.MyEned span.wiI7pd')
            elif rev.find_elements(By.CSS_SELECTOR, 'div.MyEned'):
                review_text_el = rev.find_element(By.CSS_SELECTOR, 'div.MyEned')

            review_text = review_text_el.text.strip() if review_text_el else ""
            source_id = rev.get_attribute("data-review-id")

            reviews_data.append({
                "reviewer": reviewer,
                "rating": rating,
                "date": date,
                "review": review_text,
                "source_id": source_id,
            })
        except:
            failed_reviews += 1
            continue

    print(f"Total reviews extracted: {len(reviews_data)} | Failed to extract: {failed_reviews}")
    return reviews_data
# ─────────────────────────────────────────────
# 🔍 Main Google Maps Scraper
# ─────────────────────────────────────────────
def scrape_google_maps(queries, wait_time=3, limit=None):
    global READ_CONN, WRITE_CONN
    # ChromeOptions setup for fully headless and UI-suppressed scraping
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--remote-allow-origins=*")
    # options.add_argument("--single-window")  # Prevents new tabs or windows (commented out for headless mode)
    print("Initializing Chrome WebDriver...")
    init_start = time.time()

    driver_path = "/opt/homebrew/bin/chromedriver"  # Hardcoded for stability, avoids reinstall delays
    driver = webdriver.Chrome(service=Service(driver_path), options=options)

    print(f"Chrome WebDriver initialized in {time.time() - init_start:.2f} seconds")

    all_data = []

    if limit:
        queries = queries[:limit]

    from gdh_collect.utils.helpers import save_to_csv
    # Use env var or fallback to local folder
    OUTPUT_DIR = os.getenv("GOOGLE_OUTPUT_DIR", "./scraped-data")

    script_start = datetime.now()
    print(f"Running Google Maps Scraper for {len(queries)} queries (limit={limit})")

    print(f"Setup completed in {(datetime.now() - script_start).total_seconds():.2f} seconds, starting scrape...")
    for i, query in enumerate(queries, 1):
        start_time = datetime.now()
        print(f"\n--- Starting search {i}/{len(queries)}: {query} ---")
        try:
            driver.get("https://www.google.com/maps")
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )

            search_box = driver.find_element(By.ID, "searchboxinput")
            search_box.clear()
            search_box.send_keys(query)
            driver.find_element(By.ID, "searchbox-searchbutton").click()

            try:
                WebDriverWait(driver, 60).until(
                    EC.any_of(
                        EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]')),
                        EC.presence_of_element_located((By.XPATH, '//div[contains(text(),"could not find")]')),
                        EC.presence_of_element_located((By.XPATH, '//div[contains(text(),"No results")]'))
                    )
                )
            except Exception as e:
                print(f"Error loading results for '{query}': {type(e).__name__} - {e}. Retrying once...")
                # Retry logic after timeout
                try:
                    driver.refresh()
                    time.sleep(3)
                    search_box = driver.find_element(By.ID, "searchboxinput")
                    search_box.clear()
                    search_box.send_keys(query)
                    driver.find_element(By.ID, "searchbox-searchbutton").click()
                    WebDriverWait(driver, 60).until(
                        EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]'))
                    )
                except Exception as e:
                    print(f"Second attempt failed for '{query}': {type(e).__name__} - {e}")
                    continue

            if driver.find_elements(By.XPATH, '//div[contains(text(),"could not find")]') or \
               driver.find_elements(By.XPATH, '//div[contains(text(),"No results")]'):
                print(f"No results found for: {query}")
                continue
            else:
                time.sleep(2)
                if not driver.find_elements(By.XPATH, '//div[@role="feed"]'):
                    print(f"Retrying search for {query} after short delay...")
                    driver.refresh()
                    time.sleep(3)
                    search_box = driver.find_element(By.ID, "searchboxinput")
                    search_box.clear()
                    search_box.send_keys(query)
                    driver.find_element(By.ID, "searchbox-searchbutton").click()
                    WebDriverWait(driver, 60).until(
                        EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]'))
                    )

            scrollable = driver.find_element(By.XPATH, '//div[@role="feed"]')
            for _ in range(10):
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable)
                # Reduce sleep for faster scrolling but keep a minimal delay for rendering
                time.sleep(0.3)

            results = driver.find_elements(By.CSS_SELECTOR, 'div.Nv2PK.tH5CWc.THOPZb')
            listing_urls = [r.find_element(By.CSS_SELECTOR, 'a.hfpxzc').get_attribute("href") for r in results]
            print(f"Found {len(listing_urls)} listings. Extracting details...")

            query_results_start = datetime.now()

            for idx, href in enumerate(listing_urls):
                scrape_start = datetime.now()
                try:
                    close_place_details(driver)
                    dismiss_overlay(driver)

                    driver.get(href)
                    # Ensure only one tab remains open (prevents unintended new tabs)
                    while len(driver.window_handles) > 1:
                        driver.switch_to.window(driver.window_handles[-1])
                        driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.DUwDvf'))
                    )
                    time.sleep(0.3)

                    name_raw = driver.find_element(By.CSS_SELECTOR, 'h1.DUwDvf').text.strip()
                    name = clean_name(name_raw)

                    try:
                        addr_el = driver.find_element(By.CSS_SELECTOR, '[data-item-id="address"]')
                        addr = addr_el.get_attribute("aria-label").replace("Address: ", "")
                        zip_code = extract_zip(addr)
                        address = normalize_address(addr)
                    except Exception as e:
                        address = "No Address Available"
                        zip_code = ""

                    try:
                        phone_btn = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id^="phone:tel:"]')
                        phone_text = phone_btn.get_attribute("aria-label").replace("Phone: ", "").strip()
                        phone = clean_phone(phone_text)
                    except Exception as e:
                        phone = ""

                    try:
                        website = driver.find_element(By.CSS_SELECTOR, 'a[data-item-id="authority"]').get_attribute("href")
                    except Exception as e:
                        website = ""

                    # Skip records missing name or address
                    if not name or not address:
                        print(f"⚠️ Skipped listing {idx + 1} due to missing name or address.")
                        continue

                    detail_link = driver.current_url
                    try:
                        parts = re.findall(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)", detail_link)
                        lat, lng = map(float, parts[0]) if parts else (None, None)

                        place_id_match = re.search(r"!16s%2Fg%2F([^!&]+)", detail_link)
                        if place_id_match:
                            raw_place_id = place_id_match.group(1)
                            place_id = urllib.parse.unquote(raw_place_id).split("?")[0]
                        else:
                            place_id = None
                    except Exception as e:
                        lat, lng, place_id = None, None, None

                    short_link = get_share_link(driver)

                    try:
                        rating_text = driver.find_element(By.CSS_SELECTOR, 'div.fontDisplayLarge').text.strip()
                        rating = float(rating_text)
                    except:
                        rating = None

                    hours = extract_hours(driver)
                    excerpt = driver.find_element(By.CSS_SELECTOR, 'span.wiI7pd').text.strip() if driver.find_elements(By.CSS_SELECTOR, 'span.wiI7pd') else ""

                    review_count = extract_review_count(driver)
                    print(f"Scraping listing {idx + 1}/{len(listing_urls)}: {href}")
                    print("Starting review scroll attempts...")
                    reviews = extract_reviews(driver)
                    failed_reviews = 0  # Default, will be set by extract_reviews if available
                    if isinstance(reviews, list):
                        failed_reviews = 0  # extract_reviews() prints the count itself
                    scraped_data = {
                        "Search Query": query,
                        "Name Raw": name_raw,
                        "Name": name,
                        "Slug": slugify(name_raw),
                        "Address": address,
                        "Phone": phone,
                        "Rating": rating,
                        "Website": website,
                        "Website Domain": extract_domain(website),
                        "Google Place URL": short_link if short_link else detail_link,
                        "Latitude": lat,
                        "Longitude": lng,
                        "Place ID": place_id,
                        "Hours": hours,
                        "ids.external": place_id,
                        "review_excerpt": excerpt,
                        "review_count": len(reviews),
                        "reviews": reviews,
                        "ZIP": zip_code,
                        "Source": "google_maps",
                    }

                    # Ensure Phone and ZIP fields are explicitly stored as text before CSV export
                    scraped_data["Phone"] = str(scraped_data["Phone"]) if scraped_data["Phone"] else ""
                    scraped_data["ZIP"] = str(scraped_data["ZIP"]) if scraped_data["ZIP"] else ""

                    # Append every successfully scraped listing to all_data immediately after extraction
                    all_data.append(scraped_data)

                    # ---- SUMMARY PRINTS ----
                    print(f"[{idx + 1}/{len(listing_urls)}] Processing: {name}")

                    org_match = match_existing_org(name, phone, website, address, lat, lng)

                    # Print concise review extraction summary
                    print(f"Reviews scraped: {len(reviews)} | Reviews failed: {failed_reviews}")

                    db_action = None
                    org_id = None
                    if org_match:
                        with WRITE_CONN.cursor() as cur:
                            cur.execute("SELECT id FROM organizations WHERE id = %s", (org_match[0],))
                            exists_in_write = cur.fetchone()

                        if exists_in_write:
                            if not org_data_changed(org_match, scraped_data):
                                db_action = "no_update"
                            else:
                                # Update
                                with WRITE_CONN.cursor() as cur:
                                    cur.execute("""
                                        UPDATE organizations SET
                                            name = %s,
                                            name_raw = %s,
                                            slug = %s,
                                            phones = %s::jsonb,
                                            website = %s::jsonb,
                                            addresses = %s::jsonb,
                                            google_rating = %s::double precision,
                                            review_count = %s::integer,
                                            avg_rating = %s::double precision,
                                            source_ids = %s::jsonb,
                                            ids = %s::jsonb,
                                            hours = %s::jsonb,
                                            review_excerpt = %s,
                                            canonical_abbr = %s,
                                            geom_m = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                                            google_place_url = %s,
                                            website_domain = %s,
                                            source = %s
                                        WHERE id = %s
                                    """, (
                                        scraped_data["Name"],
                                        scraped_data["Name Raw"],
                                        scraped_data["Slug"],
                                        json.dumps([scraped_data["Phone"]]) if scraped_data["Phone"] else json.dumps([]),
                                        json.dumps([scraped_data["Website"]]) if scraped_data["Website"] else json.dumps([]),
                                        json.dumps({"address": scraped_data["Address"], "zip": scraped_data["ZIP"]}),
                                        float(scraped_data["Rating"]) if scraped_data.get("Rating") else None,
                                        int(scraped_data["review_count"]) if scraped_data.get("review_count") else None,
                                        float(scraped_data["Rating"]) if scraped_data.get("Rating") else None,
                                        json.dumps({"google_place_id": scraped_data["Place ID"]}),
                                        json.dumps({"external": scraped_data["Place ID"]}),
                                        json.dumps({"hours_text": scraped_data["Hours"]}) if scraped_data.get("Hours") else json.dumps({"hours_text": "Unavailable"}),
                                        scraped_data.get("review_excerpt"),
                                        scraped_data.get("canonical_abbr") or "",
                                        float(scraped_data["Longitude"]) if scraped_data.get("Longitude") else None,
                                        float(scraped_data["Latitude"]) if scraped_data.get("Latitude") else None,
                                        scraped_data.get("Google Place URL"),
                                        scraped_data.get("Website Domain"),
                                        scraped_data.get("Source"),
                                        org_match[0]
                                    ))
                                    WRITE_CONN.commit()
                                db_action = "updated"
                            org_id = org_match[0]
                        else:
                            # Insert into write DB
                            org_insert_payload = {
                                "Name": scraped_data.get("Name"),
                                "Name Raw": scraped_data.get("Name Raw"),
                                "Slug": scraped_data.get("Slug"),
                                "Phone": scraped_data.get("Phone"),
                                "Website": scraped_data.get("Website"),
                                "Address": scraped_data.get("Address"),
                                "Rating": scraped_data.get("Rating"),
                                "review_count": scraped_data.get("review_count"),
                                "Place ID": scraped_data.get("Place ID"),
                                "Hours": scraped_data.get("Hours"),
                                "Longitude": scraped_data.get("Longitude"),
                                "Latitude": scraped_data.get("Latitude"),
                                "Google Place URL": scraped_data.get("Google Place URL"),
                                "Website Domain": scraped_data.get("Website Domain"),
                                "review_excerpt": scraped_data.get("review_excerpt"),
                                "canonical_abbr": scraped_data.get("canonical_abbr") or "",
                            }
                            inserted_id = insert_organization(org_insert_payload)
                            if inserted_id:
                                org_id = inserted_id
                                db_action = "inserted"
                                EXISTING_ORGS.append((
                                    org_id, scraped_data["Name"], json.dumps([scraped_data["Phone"]]) if scraped_data["Phone"] else json.dumps([]),
                                    json.dumps([scraped_data["Website"]]) if scraped_data["Website"] else json.dumps([]),
                                    json.dumps({"address": scraped_data["Address"], "zip": scraped_data["ZIP"]})
                                ))
                            else:
                                print(f"DB insert failed for {name}")
                                continue
                    else:
                        # Insert new org
                        org_id = insert_organization(scraped_data)
                        if org_id:
                            db_action = "inserted"
                            EXISTING_ORGS.append((
                                org_id, scraped_data["Name"], json.dumps([scraped_data["Phone"]]) if scraped_data["Phone"] else json.dumps([]),
                                json.dumps([scraped_data["Website"]]) if scraped_data["Website"] else json.dumps([]),
                                json.dumps({"address": scraped_data["Address"], "zip": scraped_data["ZIP"]})
                            ))
                        else:
                            print(f"Skipping org due to DB insert failure: {name}")
                            continue

                    if org_id:
                        for rev in reviews:
                            insert_review(rev, org_id)

                    elapsed_time = f"{(datetime.now() - scrape_start).total_seconds():.2f}"
                    print(f"Finished {idx + 1}/{len(listing_urls)}: {name} in {elapsed_time} seconds\n")

                    for _ in range(5):
                        if dismiss_overlay(driver):
                            time.sleep(0.5)
                        else:
                            break

                    time.sleep(0.2)

                except Exception as e:
                    if "SSL connection has been closed" in str(e):
                        print("SSL error detected, restarting browser and reconnecting DB...")
                        driver.quit()
                        time.sleep(3)
                        driver_path = "/opt/homebrew/bin/chromedriver"
                        driver = webdriver.Chrome(service=Service(driver_path), options=options)
                        READ_CONN.close()
                        WRITE_CONN.close()
                        READ_CONN = psycopg2.connect(os.getenv("READ_DB_URL"))
                        WRITE_CONN = psycopg2.connect(os.getenv("WRITE_DB_URL"))
                    else:
                        print(f"Failed to scrape a result: {e}")
                    continue

            # Save all listings for this city/query (all_data contains all scraped so far)
            city_name = query.replace("garage door repair", "").strip().replace(" ", "_").replace(",", "")
            filename = f"GDH_{city_name}.csv"
            try:
                save_to_csv(all_data, OUTPUT_DIR, filename)
                total_saved = len(all_data)
                print(f"Saved {total_saved} rows to {OUTPUT_DIR}/{filename}")
            except Exception as e:
                print(f"CSV save failed: {e}")

            print(f"Finished query '{query}' in {(datetime.now() - start_time).total_seconds():.2f} seconds")
            time.sleep(random.uniform(1, 3))

            # Clear all_data for next city if you want per-city CSVs only, but if you want a global CSV at the end, comment this out
            all_data.clear()

        except Exception as e:
            print(f"Error on '{query}': {type(e).__name__} - {e}")
            continue

    driver.quit()
    return all_data
# ─────────────────────────────────────────────
# Script Runner
# ─────────────────────────────────────────────
if __name__ == "__main__":
    queries = fetch_city_queries()
    scrape_google_maps(queries)