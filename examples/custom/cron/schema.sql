DROP TABLE IF EXISTS power;
DROP TABLE IF EXISTS devices;

CREATE TABLE devices (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  ipv6 TEXT
);

CREATE TABLE power (
  id TEXT PRIMARY KEY,
  device_id TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  power float8 NOT NULL,
  FOREIGN KEY(device_id) REFERENCES devices(id)
);

-- To make types correct
ALTER TABLE power
ADD COLUMN t TIMESTAMP NULL;

UPDATE power
	SET t = timestamp::TIMESTAMP;

ALTER TABLE power
ALTER COLUMN timestamp TYPE TIMESTAMP USING t;

ALTER TABLE power
DROP COLUMN t;

-- Aggregation query
SELECT device_id, date_trunc('hour', timestamp), SUM(power) FROM power GROUP BY device_id, date_trunc('hour', timestamp);
