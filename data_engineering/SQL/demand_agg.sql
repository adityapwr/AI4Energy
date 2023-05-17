INSERT INTO aggregated_demand (agg_timestamp, state_name, average_freq, average_demand)
SELECT
  (DATE_TRUNC('hour', current_datetime) + INTERVAL '15 minutes')::timestamp AS agg_timestamp,
  state_name,
  AVG(frequency) AS average_freq,
  AVG(demand) AS average_demand
FROM
  demand
GROUP BY
  (DATE_TRUNC('hour', current_datetime) + INTERVAL '15 minutes')::timestamp,
  state_name
ORDER BY
  agg_timestamp;
