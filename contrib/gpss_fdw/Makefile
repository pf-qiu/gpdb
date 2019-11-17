# contrib/gpss_fdw/Makefile

MODULE_big = gpss_fdw

OBJS = gpss_fdw.o gpss.pb.o gpss.grpc.pb.o gpss_rpc.o

EXTENSION = gpss_fdw
DATA = gpss_fdw--1.0.sql
PGFILEDESC = "gpss_fdw - foreign data wrapper for files"

REGRESS = gpss_fdw gp_gpss_fdw

EXTRA_CLEAN = sql/gpss_fdw.sql expected/gpss_fdw.out sql/gp_gpss_fdw.sql expected/gp_gpss_fdw.out

SHLIB_LINK += -lstdc++ $(shell pkg-config --libs grpc++)
PG_CPPFLAGS = -std=c++11 -fPIC $(shell pkg-config --cflags grpc++)


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

proto:
	protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` gpss.proto
