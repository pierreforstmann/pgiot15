CREATE EXTENSION pg_iot15;
CREATE TABLE pg_iot15_tab (a int) USING pg_iot15;
SELECT * FROM pg_iot15_tab;
INSERT INTO pg_iot15_tab VALUES (1);
SELECT * FROM pg_iot15_tab;
UPDATE pg_iot15_tab SET a = 0 WHERE a = 1;
SELECT * FROM pg_iot15_tab;
DELETE FROM pg_iot15_tab WHERE a = 1;
SELECT * FROM pg_iot15_tab;
--
create table t(c int) using pg_iot15;
create index i on t(c);
insert into t select generate_series(1, 1000000);
delete from t;
update t set c=3 where c=999999;
select * from t where c = 555555;

-- ALTER TABLE SET ACCESS METHOD
ALTER TABLE pg_iot15_tab SET ACCESS METHOD heap;
SELECT * FROM pg_iot15_tab;
update pg_iot15_tab set a=1;

-- Clean up
DROP TABLE pg_iot15_tab;
