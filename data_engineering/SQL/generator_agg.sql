INSERT INTO aggregated_generator (agg_timestamp, state_name, average_actual, average_schedule, average_dec_capacity)
SELECT
  (DATE_TRUNC('hour', last_update) + INTERVAL '15 minutes' + ((FLOOR(EXTRACT(MINUTE FROM last_update) / 15) * 15) || ' minutes')::interval) AS agg_timestamp,
  state_name,
  AVG(actual) AS average_actual,
  AVG(schedule) AS average_schedule,
  AVG(dec_capacity) AS average_dec_capacity
FROM
  generator
GROUP BY
  (DATE_TRUNC('hour', last_update) + INTERVAL '15 minutes' + ((FLOOR(EXTRACT(MINUTE FROM last_update) / 15) * 15) || ' minutes')::interval),
  state_name
ORDER BY
  agg_timestamp;
