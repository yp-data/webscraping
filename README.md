# Webscraping Portfolio

This repository showcases advanced data scraping projects built in Python. These scripts are used in production to extract structured business information from Google Maps and other business directories for market research, lead generation, and local SEO analysis.

The focus is on robustness, deduplication, and clean integration with PostgreSQL databases, making this more than a basic scraper — it's a modular pipeline designed for scalable, accurate data collection.

---

## Overview

### 1. Google Maps Scraper (`google_scraper/google_scraper.py`)
This scraper extracts business details from Google Maps based on dynamic search queries pulled from a PostgreSQL `cities` table.

**What it does:**
- Automates Chrome to search terms like "garage door repair Chicago IL"
- Extracts business name, phone, address, website, hours, reviews, rating, geolocation, and Google Place ID
- Deduplicates results by comparing to existing organization records (via fuzzy logic + phone/site/address matching)
- Stores new or updated entries into an `organizations` table
- Extracts and stores reviews per business into a `reviews` table

### 2. Alivizatos Scraper (`alivizatos_scraper/Alivizatos.py`)
A lightweight legal site scraper targeting structured articles from a law firm website. It extracts title, date, and body content from posts.

---

## What I Built and Why

I designed this scraper system to support a data product tracking garage door service companies across the U.S. The primary goals were:
- **Reliability**: it handles JS-rendered content using Selenium
- **Deduplication**: fuzzy match logic minimizes redundant records
- **Scalability**: database-first design enables analysis across hundreds of cities
- **Accuracy**: scraped data is validated, enriched, and geo-tagged

---

## Technologies Used

- Python 3
- Selenium (headless Chrome)
- PostgreSQL
- FuzzyWuzzy (deduplication)
- Google Maps DOM parsing
- JSONB handling for structured inserts
- Git + GitHub

---

## Setup & Run

### 1. Clone the repo
```bash
git clone https://github.com/yp-data/webscraping.git
cd webscraping
```

### 2. Setup virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure `.env` file
Rename `.env.example` to `.env` and add your database URLs and output folder:
```bash
cp .env.example .env
```

---

## Project Structure

```
webscraping/
├── alivizatos_scraper/
│   └── Alivizatos.py
├── google_scraper/
│   └── google_scraper.py
├── requirements.txt
├── .gitignore
├── .env.example
└── README.md
```

---

## Environment Variables

Defined in `.env`:
```env
READ_DB_URL=postgresql://your-read-username:your-password@host:port/dbname?sslmode=require
WRITE_DB_URL=postgresql://your-write-username:your-password@host:port/dbname?sslmode=require
GOOGLE_OUTPUT_DIR=./scraped-data/google
YELP_OUTPUT_DIR=./scraped-data/yelp
BBB_OUTPUT_DIR=./scraped-data/bbb
```

---

## Disclaimers

- This code is for educational and professional demonstration purposes.
- Web scraping should comply with the terms of service of the websites involved.
- This project uses Selenium to simulate browser behavior for publicly available data.

---

## Contact

Built by Yuri P. For questions, collaborations, or freelance scraping projects, reach me via [Email](mailto:yuri.punzalan@gmail.com)