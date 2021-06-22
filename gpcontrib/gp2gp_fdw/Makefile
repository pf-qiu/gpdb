# gpcontrib/gp2gp_fdw/Makefile

MODULE_big = gp2gp_fdw
OBJS = gp2gp_fdw.o


EXTENSION = gp2gp_fdw
DATA = gp2gp_fdw--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/gp2gp_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)