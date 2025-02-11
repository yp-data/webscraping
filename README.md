🚗 Alivizatos Car Listings Web Scraper
This project is a web scraper designed to extract car listings from Alivizatos.gr, storing the data in a PostgreSQL database while tracking the status (active or inactive) of each listing.

📌 Features
✅ Scrapes car details including title, price, attributes, and features.
✅ Stores data in PostgreSQL, appending new listings while avoiding duplicates.
✅ Tracks car availability using adstatus (active or inactive).
✅ Saves the scraped data to a CSV file for easy access.
✅ Implements error handling for robust scraping.

🛠 Tech Stack
Python (Requests, BeautifulSoup, Pandas)
PostgreSQL (psycopg2)
Tabulate (for clean console output)
🚀 How It Works
The scraper fetches all car listing URLs.
It extracts relevant details from each listing.
Data is compared with the latest database records:
New listings → Inserted as active.
Missing listings → Marked as inactive.
Previously inactive listings → Not reinserted.
The final dataset is saved to PostgreSQL and exported to a CSV.
📂 Installation
1️⃣ Clone the Repository
bash
Copy
Edit
git clone https://github.com/yourusername/alivizatos-scraper.git
cd alivizatos-scraper
2️⃣ Install Dependencies
bash
Copy
Edit
pip install -r requirements.txt
3️⃣ Configure PostgreSQL
Update your database credentials in config.py or modify DB_PARAMS in AlivizatosNew.py:

python
Copy
Edit
DB_PARAMS = {
    "dbname": "vehicle_listing_db",
    "user": "postgres",
    "password": "yourpassword",
    "host": "localhost",
    "port": "5432"
}
4️⃣ Run the Scraper
bash
Copy
Edit
python Alivizatos.py
📊 Sample Output
sql
Copy
Edit
🚀 Starting Scraper...

✅ Table 'car_details' checked/created successfully.
Fetching page 1...
✅ Data for BMW X5 inserted into PostgreSQL with status 'active'.
✅ Data for Audi A3 inserted into PostgreSQL with status 'active'.
❗ Marked https://alivizatos.gr/listing/old-car-url/ as INACTIVE.
📊 Table View of First 5 Listings:
+----+--------+----------------------------------------+--------+------------+
| ID | Title  | Car URL                               | Price  | AdStatus   |
+----+--------+----------------------------------------+--------+------------+
| 1  | BMW X5 | https://alivizatos.gr/listing/bmw-x5 | 35,000€ | Active     |
| 2  | Audi A3 | https://alivizatos.gr/listing/audi-a3 | 18,500€ | Active     |
| 3  | Tesla  | https://alivizatos.gr/listing/tesla  | 50,000€ | Inactive   |
+----+--------+----------------------------------------+--------+------------+

✅ Data saved to 'scraped_car_listings.csv'
📌 To-Do / Future Enhancements
 Add support for proxy rotation to avoid bot detection.
 Implement multi-threading for faster scraping.
 Integrate with Power BI or Tableau for analytics.
👨‍💻 Author
Yuri P. – GitHub 

Let me know if you want any modifications! 🚀
