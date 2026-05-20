CREATE EXTENSION IF NOT EXISTS pg_yaap;
SET pg_yaap.enabled = on;
SET pg_yaap.trace_hooks = on;
SET pg_yaap.parallel = on;

CREATE TABLE yaap_scan_t (
    a int4,
    b text
);

INSERT INTO yaap_scan_t VALUES (1, 'x'), (2, 'y');

SELECT * FROM yaap_scan_t;

DROP TABLE yaap_scan_t;
DROP EXTENSION pg_yaap;
