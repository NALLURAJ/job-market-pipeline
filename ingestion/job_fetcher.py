import requests
import json
import mysql.connector 
import boto3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import API_KEY, DB_CONFIG, s3_Bucket

API_URL = "https://jsearch.p.rapidapi.com/search"
USE_CACHE = True
s3_Client = boto3.client("s3") 
 # Set to False to fetch fresh data from the API instead of using cached files
headers = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
}

db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()

if db is not None:
    print("Connected to the database successfully!")
else:
    print("Failed to connect to the database.")

queries = [
    "software engineer in United States",
    "data engineer in United States",
    "data analyst in United States",
    "devops engineer in United States",
    "cloud engineer in United States"
]

total_inserted = 0

for query in queries:
    cache_file = f"data/raw/{query.replace(' ', '_')}.json"
    s3_key = f"raw/{query.replace(' ', '_')}.json"

    if USE_CACHE:
        with open(cache_file, "r") as f:
            data = json.load(f)
    else:
        params = {
            "query": query,
            "page": 1,
            "num_pages": 1,
            "country": "us",
            "date_posted": "today"
        }
        response = requests.get(API_URL, headers=headers, params=params)
        data = response.json()
        with open(cache_file, "w") as f:
            json.dump(data, f)
        s3_Client.upload_file(cache_file, s3_Bucket, s3_key)
        print(f"Uploaded {cache_file} to S3 bucket {s3_Bucket} with key {s3_key}")

    if data.get("status") != "OK":
        print(f"Failed for query: {query}")
        continue

    jobs = data.get("data", [])
    print(f"Query: {query} — Found {len(jobs)} jobs")
    for job in jobs:

        check_sql = "select company_id from companies where company_name = %s"
        cursor.execute(check_sql, (job.get("employer_name"),))
        result = cursor.fetchone()

        if result:
            company_id = result[0]
        else:
            insert_company_sql = """INSERT INTO companies (company_name, headquarters_location, created_at) 
            VALUES (%s, %s, NOW())"""
            company_values = job.get("employer_name"), job.get("job_location")
            cursor.execute(insert_company_sql, company_values)
            company_id = cursor.lastrowid
            

            # Convert work_model
        work_model = "remote" if job.get("job_is_remote") else "onsite"

        # Convert job_type: "Full-time" → "full_time", "Contractor" → "contract"
        job_type_raw = job.get("job_employment_type", "")
        job_type_map = {
            "Full-time": "full_time",
            "Part-time": "part_time",
            "Contractor": "contract",
            "Internship": "internship"
        }
        job_type = job_type_map.get(job_type_raw, "full_time")

            # Extract experience level from title or description
        title_lower = (job.get("job_title") or "").lower()
        desc_lower = (job.get("job_description") or "").lower()
        
        if "intern" in title_lower:
            experience_level = "internship"
        elif "senior" in title_lower or "sr." in title_lower or "sr " in title_lower:
            experience_level = "senior"
        elif "junior" in title_lower or "jr." in title_lower or "entry" in title_lower:
            experience_level = "junior"
        elif "lead" in title_lower or "principal" in title_lower or "staff" in title_lower:
            experience_level = "lead"
        else:
            experience_level = "mid"

        posting_sql = """
            INSERT INTO job_postings 
            (company_id, title, description, location, work_model, job_type, experience_level,
            salary_min, salary_max, salary_currency, source_platform, source_url,
            posted_date, scraped_date, is_active, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, NOW(), TRUE, NOW())
        """
            # Convert posted date: "2026-03-26T05:00:00.000Z" → "2026-03-26"
        posted_raw = job.get("job_posted_at_datetime_utc", "")
        posted_date = posted_raw[:10] if posted_raw else None

        salary_min = job.get("job_min_salary")
        salary_max = job.get("job_max_salary")
        salary_period = job.get("job_salary_period","")
        if salary_period == "HOUR" and salary_min:
            salary_min = round(salary_min * 2080, 2)
        if salary_period == "HOUR" and salary_max:
            salary_max = round(salary_max * 2080, 2)

        posting_values = (
            company_id,
            job.get("job_title"),
            job.get("job_description"),
            job.get("job_location"),
            work_model,
            job_type,
            experience_level,
            job.get("job_min_salary"),
            job.get("job_max_salary"),
            "USD",
            job.get("job_publisher"),
            job.get("job_apply_link"),
            posted_date
        )
        cursor.execute(posting_sql, posting_values)
        total_inserted += 1

db.commit()
cursor.close()
db.close()  
print("Data inserted successfully!")
print(f"Total job postings inserted: {total_inserted}")