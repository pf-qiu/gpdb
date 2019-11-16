# contrib/gpss_fdw/Makefile

MODULES = gpss_fdw

EXTENSION = gpss_fdw
DATA = gpss_fdw--1.0.sql
PGFILEDESC = "gpss_fdw - foreign data wrapper for files"

REGRESS = gpss_fdw gp_gpss_fdw

EXTRA_CLEAN = sql/gpss_fdw.sql expected/gpss_fdw.out sql/gp_gpss_fdw.sql expected/gp_gpss_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gpss_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
