import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mysql.connector
from config.config import DB_CONFIG

db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()

# List of tech skills to look for
TECH_SKILLS = [
    "python", "java", "javascript", "typescript", "c#", "c++", "go", "rust", "ruby",
    "sql", "mysql", "postgresql", "mongodb", "redis", "elasticsearch",
    "aws", "azure", "gcp", "docker", "kubernetes", "terraform",
    "react", "angular", "node.js", "django", "flask", "spring boot",
    "spark", "hadoop", "kafka", "airflow", "dbt",
    "tableau", "power bi", "looker",
    "git", "jenkins", "ci/cd", "linux", "agile", "scrum",
    "machine learning", "deep learning", "nlp", "tensorflow", "pytorch",
    "snowflake", "redshift", "bigquery", "databricks",
    "rest api", "graphql", "microservices"
]

SOFT_SKILLS = [
    "communication", "leadership", "teamwork", "problem solving",
    "analytical", "collaboration", "mentoring","Critical thinking", "adaptability", "time management", 
    "creativity", "empathy", "resilience", "work ethic", "interpersonal skills", "conflict"
]

# Get all job postings with descriptions
cursor.execute("SELECT job_id, description FROM job_postings WHERE description IS NOT NULL")
jobs = cursor.fetchall()

total_skills = 0

for job_id, description in jobs:
    if not description:
        continue
    desc_lower = description.lower()

    # Check technical skills
    for skill in TECH_SKILLS:
        if skill in desc_lower:
            # Check if already exists
            cursor.execute(
                "SELECT skill_id FROM job_skills WHERE job_id = %s AND skill_name = %s",
                (job_id, skill)
            )
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO job_skills (job_id, skill_name, skill_type, created_at) VALUES (%s, %s, %s, NOW())",
                    (job_id, skill, "technical")
                )
                total_skills += 1

    # Check soft skills
    for skill in SOFT_SKILLS:
        if skill in desc_lower:
            cursor.execute(
                "SELECT skill_id FROM job_skills WHERE job_id = %s AND skill_name = %s",
                (job_id, skill)
            )
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO job_skills (job_id, skill_name, skill_type, created_at) VALUES (%s, %s, %s, NOW())",
                    (job_id, skill, "soft")
                )
                total_skills += 1

db.commit()
cursor.close()
db.close()
print(f"Extracted {total_skills} skills from {len(jobs)} job postings")
