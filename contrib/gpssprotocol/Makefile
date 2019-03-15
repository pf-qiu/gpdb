MODULE_big = gpssprotocol
OBJS       = gpssprotocol.o stream.pb.o stream.grpc.pb.o

PG_CPPFLAGS = -std=c++11 -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)
SHLIB_LINK = $(shell pkg-config --libs protobuf grpc++ grpc) -lstdc++

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contriba/gpssprotocol/
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
