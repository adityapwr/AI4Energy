CREATE OR REPLACE FUNCTION update_aggregated_interlink()
  RETURNS TRIGGER AS
$BODY$
BEGIN
  INSERT INTO aggregated_interlink (
    agg_timestamp,
    region_name,
    average_export_ttc,
    average_import_ttc,
    average_long_term,
    average_short_term,
    average_px_import,
    average_px_export,
    average_total,
    average_current_loading
  )
  SELECT
    (DATE_TRUNC('hour', NEW.last_update) + INTERVAL '15 minutes')::TIMESTAMP AS agg_timestamp,
    NEW.region_name,
    AVG(export_ttc) AS average_export_ttc,
    AVG(import_ttc) AS average_import_ttc,
    AVG(long_term) AS average_long_term,
    AVG(short_term) AS average_short_term,
    AVG(px_import) AS average_px_import,
    AVG(px_export) AS average_px_export,
    AVG(total) AS average_total,
    AVG(current_loading) AS average_current_loading
  FROM interlink
  WHERE region_name = NEW.region_name
    AND last_update >= (DATE_TRUNC('hour', NEW.last_update) + INTERVAL '15 minutes')
    AND last_update < (DATE_TRUNC('hour', NEW.last_update) + INTERVAL '30 minutes')
  GROUP BY
    (DATE_TRUNC('hour', NEW.last_update) + INTERVAL '15 minutes'),
    NEW.region_name;
  
  RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER interlink_trigger
  AFTER INSERT ON interlink
  FOR EACH ROW
  EXECUTE FUNCTION update_aggregated_interlink();
