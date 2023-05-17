INSERT INTO aggregated_interlink (agg_timestamp, region_name, average_export_ttc, average_import_ttc, average_long_term, average_short_term, average_px_import, average_px_export, average_total, average_current_loading)
SELECT
  (DATE_TRUNC('hour', last_update) + INTERVAL '15 minutes' + ((FLOOR(EXTRACT(MINUTE FROM last_update) / 15) * 15) || ' minutes')::interval) AS agg_timestamp,
  region_name,
  AVG(export_ttc) AS average_export_ttc,
  AVG(import_ttc) AS average_import_ttc,
  AVG(long_term) AS average_long_term,
  AVG(short_term) AS average_short_term,
  AVG(px_import) AS average_px_import,
  AVG(px_export) AS average_px_export,
  AVG(total) AS average_total,
  AVG(current_loading) AS average_current_loading
FROM
  interlink
GROUP BY
  (DATE_TRUNC('hour', last_update) + INTERVAL '15 minutes' + ((FLOOR(EXTRACT(MINUTE FROM last_update) / 15) * 15) || ' minutes')::interval),
  region_name
ORDER BY
  agg_timestamp;
