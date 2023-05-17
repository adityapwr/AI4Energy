CREATE OR REPLACE FUNCTION aggregate_generator()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO aggregated_generator (agg_timestamp, state_name, average_actual, average_schedule, average_dec_capacity)
  SELECT
    (DATE_TRUNC('hour', NEW.last_update) + INTERVAL '15 minutes')::timestamp AS agg_timestamp,
    NEW.state_name,
    AVG(NEW.actual) AS average_actual,
    AVG(NEW.schedule) AS average_schedule,
    AVG(NEW.dec_capacity) AS average_dec_capacity
  FROM
    (SELECT * FROM generator WHERE last_update >= NEW.last_update - INTERVAL '15 minutes') AS NEW
  GROUP BY
    (DATE_TRUNC('hour', NEW.last_update) + INTERVAL '15 minutes')::timestamp,
    NEW.state_name;
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;


CREATE TRIGGER aggregate_generator_trigger
AFTER INSERT ON generator
FOR EACH ROW
EXECUTE FUNCTION aggregate_generator();

