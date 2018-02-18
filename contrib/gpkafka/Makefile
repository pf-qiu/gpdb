MODULE_big = gpkafka
OBJS       = gpkafka.o resman.o meta.o

PG_CPPFLAGS = -I$(libpq_srcdir) $(shell pkg-config --cflags rdkafka)
PG_LIBS = $(libpq_pgport)
SHLIB_LINK += -Wl,-Bstatic $(shell pkg-config --libs-only-L rdkafka) -lrdkafka -Wl,-Bdynamic -lpthread
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gpkafka
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
