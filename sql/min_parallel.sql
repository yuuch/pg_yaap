CREATE EXTENSION IF NOT EXISTS pg_yaap;
SET pg_yaap.enabled = on;
SET pg_yaap.parallel = on;
SET pg_yaap.parallel_max_workers = 4;
SET pg_yaap.parallel_min_relation_blocks = 0;
SET jit = off;

CREATE TABLE yaap_min_t (
    grp int4,
    v numeric(15,2)
);

INSERT INTO yaap_min_t
SELECT g % 4, ((g % 1000) + 100)::numeric(15,2)
FROM generate_series(1, 200000) AS g;

INSERT INTO yaap_min_t VALUES
    (0, 5.00),
    (1, 7.00),
    (2, 9.00),
    (3, 11.00);

SELECT grp, min(v)
FROM yaap_min_t
GROUP BY grp
ORDER BY grp;

DROP TABLE yaap_min_t;
DROP EXTENSION pg_yaap;
