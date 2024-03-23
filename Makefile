MODULES = pg_iot15 pg_heapam
OBJS=pg_iot15.o pg_heapam.o

EXTENSION = pg_iot15 
DATA = pg_iot15--0.0.1.sql
PGFILEDESC = "pg_iot15 - insert-only table access method"

REGRESS_OPTS= --temp-instance=/tmp/5454 --port=5454 --temp-config pg.conf
REGRESS = pg_iot15 

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
