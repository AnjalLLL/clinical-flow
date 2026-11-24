-- Average cost and length of each encounter type, split into payer and patient share
SELECT
    encounter_class,
    COUNT(*) AS encounters,
    ROUND(AVG(total_claim_cost), 2) AS avg_claim_cost,
    ROUND(AVG(total_claim_cost - payer_coverage), 2) AS avg_patient_cost,
    ROUND(AVG(duration_min), 1) AS avg_duration_min
FROM fact_encounter
GROUP BY encounter_class
ORDER BY avg_claim_cost DESC;
