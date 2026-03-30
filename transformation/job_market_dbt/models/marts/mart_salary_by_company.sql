SELECT
    c.company_name,
    c.headquarters_location,
    COUNT(jp.job_id) AS total_openings,
    ROUND(AVG(jp.salary_avg), 2) AS avg_salary,
    COUNT(DISTINCT jp.job_type) AS job_types_offered
FROM {{ ref('stg_job_postings') }} jp
JOIN {{ ref('stg_companies') }} c ON jp.company_id = c.company_id
GROUP BY c.company_name, c.headquarters_location
ORDER BY total_openings DESC
