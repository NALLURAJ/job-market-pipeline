SELECT
    location,
    COUNT(*) AS job_count,
    COUNT(salary_avg) AS jobs_with_salary,
    ROUND(AVG(salary_avg), 2) AS avg_salary,
    SUM(CASE WHEN work_model = 'remote' THEN 1 ELSE 0 END) AS remote_jobs
FROM {{ ref('stg_job_postings') }}
GROUP BY location
ORDER BY job_count DESC
