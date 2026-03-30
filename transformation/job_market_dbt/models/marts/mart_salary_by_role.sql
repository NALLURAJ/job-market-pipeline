SELECT
    experience_level,
    COUNT(*) AS job_count,
    COUNT(salary_avg) AS jobs_with_salary,
    ROUND(AVG(salary_avg), 2) AS avg_salary,
    ROUND(MIN(salary_min), 2) AS min_salary,
    ROUND(MAX(salary_max), 2) AS max_salary
FROM {{ ref('stg_job_postings') }}
GROUP BY experience_level
ORDER BY avg_salary DESC
