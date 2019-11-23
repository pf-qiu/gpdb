# contrib/gpss_fdw/Makefile

MODULE_big = gpss_fdw

GRPC_OBJS = gpss.grpc.pb.o gpss.pb.o
GRPC_LIBS = -lstdc++ $(shell pkg-config --libs grpc++) $(shell pkg-config --libs protobuf)

OBJS = gpss_fdw.o gpss_rpc.o $(GRPC_OBJS)

EXTENSION = gpss_fdw
DATA = gpss_fdw--1.0.sql
PGFILEDESC = "gpss_fdw - foreign data wrapper for files"

REGRESS = gpss_fdw gp_gpss_fdw

EXTRA_CLEAN = sql/gpss_fdw.sql expected/gpss_fdw.out sql/gp_gpss_fdw.sql expected/gp_gpss_fdw.out

SHLIB_LINK += $(GRPC_LIBS)
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

server: gpss_server.o $(GRPC_OBJS)
	$(CC) $(CFLAGS) gpss_server.o $(GRPC_OBJS) $(PG_LIBS_INTERNAL) $(LDFLAGS) $(LDFLAGS_EX) $(PG_LIBS) $(LIBS) $(GRPC_LIBS) -o $@$(X)