CREATE EXTENSION pg_immutable;
CREATE TABLE pg_immutable_tab (a int) USING pg_immutable;
SELECT * FROM pg_immutable_tab;
INSERT INTO pg_immutable_tab VALUES (1);
SELECT * FROM pg_immutable_tab;
UPDATE pg_immutable_tab SET a = 0 WHERE a = 1;
SELECT * FROM pg_immutable_tab;
DELETE FROM pg_immutable_tab WHERE a = 1;
SELECT * FROM pg_immutable_tab;
--
create table t(c int) using pg_immutable;
create index i on t(c);
insert into t select generate_series(1, 1000000);
delete from t;
update t set c=3 where c=999999;
select * from t where c = 555555;

-- ALTER TABLE SET ACCESS METHOD
ALTER TABLE pg_immutable_tab SET ACCESS METHOD heap;
SELECT * FROM pg_immutable_tab;
update pg_immutable_tab set a=1;

-- Clean up
DROP TABLE pg_immutable_tab;
