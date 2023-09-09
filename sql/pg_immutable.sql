CREATE EXTENSION pg_immutable;
CREATE TABLE pg_immutable_tab (a int) USING pg_immutable;
SELECT * FROM pg_immutable_tab;
INSERT INTO pg_immutable_tab VALUES (1);
SELECT * FROM pg_immutable_tab;
UPDATE pg_immutable_tab SET a = 0 WHERE a = 1;
SELECT * FROM pg_immutable_tab;
DELETE FROM pg_immutable_tab WHERE a = 1;
SELECT * FROM pg_immutable_tab;

-- ALTER TABLE SET ACCESS METHOD
ALTER TABLE pg_immutable_tab SET ACCESS METHOD heap;
INSERT INTO pg_immutable_tab VALUES (1);
SELECT * FROM pg_immutable_tab;
ALTER TABLE pg_immutable_tab SET ACCESS METHOD pg_immutable;
SELECT * FROM pg_immutable_tab;

-- Clean up
DROP TABLE pg_immutable_tab;
