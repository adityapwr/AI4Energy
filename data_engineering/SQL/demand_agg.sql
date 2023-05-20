INSERT INTO aggregated_demand (agg_timestamp, state_name, average_freq, average_demand)
SELECT
  (DATE_TRUNC('hour', current_datetime) + INTERVAL '15 minutes' + ((FLOOR(EXTRACT(MINUTE FROM current_datetime) / 15) * 15) || ' minutes')::interval) AS agg_timestamp,
  state_name,
  AVG(frequency) AS average_freq,
  AVG(demand) AS average_demand
FROM
  demand
GROUP BY
  (DATE_TRUNC('hour', current_datetime) + INTERVAL '15 minutes' + ((FLOOR(EXTRACT(MINUTE FROM current_datetime) / 15) * 15) || ' minutes')::interval),
  state_name
ORDER BY
  agg_timestamp;
