MODULES = pg_immutable 

EXTENSION = pg_immutable 
DATA = pg_immutable--0.0.1.sql
PGFILEDESC = "pg_immutable - insert-only table access method"

REGRESS = pg_immutable 

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
