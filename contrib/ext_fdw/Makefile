# contrib/ext_fdw/Makefile

MODULES = ext_fdw

EXTENSION = ext_fdw
DATA = ext_fdw--1.0.sql
PGFILEDESC = "ext_fdw - foreign data wrapper for external table"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/ext_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
