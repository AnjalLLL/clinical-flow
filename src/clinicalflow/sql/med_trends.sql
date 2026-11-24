-- Yearly prescriptions and cost for the 5 most prescribed medications, last 5 years of data
WITH top_meds AS (
    SELECT description FROM fact_medication GROUP BY description ORDER BY COUNT(*) DESC LIMIT 5
)
SELECT
    d.year,
    m.description,
    COUNT(*) AS prescriptions,
    ROUND(SUM(m.total_cost), 2) AS total_cost
FROM fact_medication m
JOIN dim_date d USING (date_key)
WHERE m.description IN (SELECT description FROM top_meds)
  AND d.year >= (SELECT MAX(year) - 4 FROM dim_date)
GROUP BY d.year, m.description
ORDER BY d.year, prescriptions DESC;
