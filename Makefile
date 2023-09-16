MODULES = pg_immutable pg_heapam
OBJS=pg_immutable.o pg_heapam.o

EXTENSION = pg_immutable 
DATA = pg_immutable--0.0.1.sql
PGFILEDESC = "pg_immutable - insert-only table access method"

REGRESS = pg_immutable 

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
