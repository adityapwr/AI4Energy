CREATE OR REPLACE FUNCTION aggregate_demand()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO aggregated_demand (agg_timestamp, state_name, average_freq, average_demand)
  SELECT
    (DATE_TRUNC('hour', NEW.current_datetime) + INTERVAL '15 minutes')::timestamp AS agg_timestamp,
    NEW.state_name,
    AVG(NEW.frequency) AS average_freq,
    AVG(NEW.demand) AS average_demand
  FROM
    (SELECT * FROM demand WHERE current_datetime >= NEW.current_datetime - INTERVAL '15 minutes') AS NEW
  GROUP BY
    (DATE_TRUNC('hour', NEW.current_datetime) + INTERVAL '15 minutes')::timestamp,
    NEW.state_name;
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER aggregate_demand_trigger
AFTER INSERT ON demand
FOR EACH ROW
EXECUTE FUNCTION aggregate_demand();
