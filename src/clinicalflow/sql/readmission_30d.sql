-- 30-day readmission rate for inpatient stays, grouped by the reason for the stay
WITH stays AS (
    SELECT
        patient_id,
        stop_ts,
        COALESCE(reason_description, 'Unspecified') AS reason,
        LEAD(start_ts) OVER (PARTITION BY patient_id ORDER BY start_ts) AS next_start
    FROM fact_encounter
    WHERE encounter_class = 'inpatient'
)
SELECT
    reason,
    COUNT(*) AS inpatient_stays,
    SUM(CASE WHEN next_start <= stop_ts + INTERVAL 30 DAY THEN 1 ELSE 0 END) AS readmissions,
    ROUND(100.0 * readmissions / inpatient_stays, 1) AS readmission_rate_pct
FROM stays
GROUP BY reason
ORDER BY inpatient_stays DESC
LIMIT 10;
