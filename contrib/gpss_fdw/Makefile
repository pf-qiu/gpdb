# contrib/gpss_fdw/Makefile

MODULE_big = gpss_fdw

GRPC_OBJS = gpss.grpc.pb.o gpss.pb.o
GRPC_LIBS = $(shell pkg-config --libs grpc++) $(shell pkg-config --libs protobuf)
KAFKA_LIBS = $(shell pkg-config --libs rdkafka)
OBJS = gpss_fdw.o gpss_rpc.o $(GRPC_OBJS)

EXTENSION = gpss_fdw
DATA = gpss_fdw--1.0.sql
PGFILEDESC = "gpss_fdw - foreign data wrapper for files"

REGRESS = gpss_fdw gp_gpss_fdw

EXTRA_CLEAN = sql/gpss_fdw.sql expected/gpss_fdw.out sql/gp_gpss_fdw.sql expected/gp_gpss_fdw.out

SHLIB_LINK += -lstdc++ $(GRPC_LIBS) $(KAFKA_LIBS)
PG_CPPFLAGS = -fPIC $(shell pkg-config --cflags grpc++) $(shell pkg-config --cflags rdkafka)

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

SERVER_OBJS = gpss_server.o gpss_server_utils.o

server: $(SERVER_OBJS) $(GRPC_OBJS)
	$(CC) $(SERVER_OBJS) $(GRPC_OBJS) $(GRPC_LIBS) $(KAFKA_LIBS) -lstdc++ -o $@$(X)