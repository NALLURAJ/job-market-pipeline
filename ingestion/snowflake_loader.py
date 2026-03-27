import os 
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake.connector
import mysql.connector
from config.config import SNOWFLAKE_CONFIG,DB_CONFIG

mysql_db = mysql.connector.connect(**DB_CONFIG)
mysql_cursor = mysql_db.cursor()

snowflake_db = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
snowflake_cursor = snowflake_db.cursor()
print("Connected to both databases successfully!")

mysql_cursor.execute("SELECT company_name, industry, company_size, headquarters_location FROM companies")
companies = mysql_cursor.fetchall()

for company in companies:
    snowflake_cursor.execute("SELECT company_id FROM companies WHERE company_name = %s", (company[0],))
    if snowflake_cursor.fetchone():
        continue
    snowflake_cursor.execute("""
        INSERT INTO companies (company_name, industry, company_size, headquarters_location)
        VALUES (%s, %s, %s, %s)
    """, company)
print(f"Inserted {len(companies)} companies into Snowflake.")

mysql_cursor.execute("""
    SELECT c.company_name, jp.title, jp.description, jp.location, jp.work_model, 
           jp.job_type, jp.experience_level, jp.salary_min, jp.salary_max, 
           jp.salary_currency, jp.source_platform, jp.source_url, jp.posted_date, 
           jp.scraped_date, jp.is_active
    FROM job_postings jp
    JOIN companies c ON jp.company_id = c.company_id
""")
postings = mysql_cursor.fetchall()

for posting in postings:
    company_name = posting[0]
    
    # Get company_id from Snowflake
    snowflake_cursor.execute("SELECT company_id FROM companies WHERE company_name = %s", (company_name,))
    result = snowflake_cursor.fetchone()
    if not result:
        continue
    sf_company_id = result[0]
    snowflake_cursor.execute("SELECT job_id FROM job_postings WHERE source_url = %s", (posting[11],))
    if snowflake_cursor.fetchone():
        continue
    snowflake_cursor.execute("""
        INSERT INTO job_postings 
        (company_id, title, description, location, work_model, job_type, 
         experience_level, salary_min, salary_max, salary_currency, 
         source_platform, source_url, posted_date, scraped_date, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (sf_company_id,) + posting[1:])

print(f"Loaded {len(postings)} job postings into Snowflake")

snowflake_db.commit()
mysql_db.close()
mysql_cursor.close()
snowflake_cursor.close()
snowflake_db.close()