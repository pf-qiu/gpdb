#
# Makefile for isolation2 tests
#

subdir = src/test/isolation2
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

NAME = isolation2_regress
OBJS = isolation2_regress.o workfile_mgr_test.o
include $(top_srcdir)/src/Makefile.shlib

ifeq ($(PORTNAME), win32)
LDLIBS += -lws2_32
endif

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) -I$(srcdir)/../regress $(CPPFLAGS)
override LDLIBS := $(libpq_pgport) $(LDLIBS)

all: pg_isolation2_regress$(X) all-lib data extended_protocol_test

extended_protocol_test: extended_protocol_test.c
	$(CC) $(CPPFLAGS) -I$(top_builddir)/src/interfaces/libpq -L$(GPHOME)/lib -L$(top_builddir)/src/interfaces/libpq  -o $@ $< -lpq

test_parallel_retrieve_cursor_extended_query: test_parallel_retrieve_cursor_extended_query.c
	$(CC) $(CPPFLAGS) -I$(top_builddir)/src/interfaces/libpq -L$(GPHOME)/lib -L$(top_builddir)/src/interfaces/libpq  -o $@ $< -lpq

test_parallel_retrieve_cursor_extended_query_error: test_parallel_retrieve_cursor_extended_query_error.c
	$(CC) $(CPPFLAGS) -I$(top_builddir)/src/interfaces/libpq -L$(GPHOME)/lib -L$(top_builddir)/src/interfaces/libpq  -o $@ $< -lpq

pg_regress.o:
	$(MAKE) -C $(top_builddir)/src/test/regress pg_regress.o
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/pg_regress.o .

gpstringsubs.pl:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/gpstringsubs.pl

gpdiff.pl: atmsort.pm explain.pm GPTest.pm
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/gpdiff.pl

GPTest.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/GPTest.pm

atmsort.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/atmsort.pm

explain.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/explain.pm

data:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/data

pg_isolation2_regress$(X): isolation2_main.o pg_regress.o submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(filter %.o,$^) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

clean distclean:
	rm -f pg_isolation2_regress$(X) $(OBJS) isolation2_main.o
	rm -f pg_regress.o
	rm -f gpstringsubs.pl gpdiff.pl atmsort.pm explain.pm
	rm -f data
	rm -rf $(pg_regress_clean_files)

install: all gpdiff.pl gpstringsubs.pl

installcheck: install installcheck-parallel-retrieve-cursor
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_isolation2 --schedule=$(srcdir)/isolation2_schedule

installcheck-resgroup: install
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_resgroup --dbname=isolation2resgrouptest --schedule=$(srcdir)/isolation2_resgroup_schedule

installcheck-parallel-retrieve-cursor: install test_parallel_retrieve_cursor_extended_query test_parallel_retrieve_cursor_extended_query_error
	# Before running test, set statement_timeout to 1 minute so that even if some
	# error make 'gp_wait_parallel_retrieve_cursor()' hang, it will be canceled when timeout.
	#
	# And also add test for test_parallel_retrieve_cursor_extended_query, only need to check command rc==0
	default_coordinator_timeout=$$(gpconfig -s statement_timeout | grep "Coordinator" | cut -d':' -f2) && \
	default_segment_timeout=$$(gpconfig -s statement_timeout | grep "Segment" | cut -d':' -f2) && \
	if [ -z "$${default_coordinator_timeout}" ] || [ -z "$${default_segment_timeout}" ] ; then \
	  echo " --- default_coordinator_timeout is: $${default_coordinator_timeout} ---"; \
	  echo " --- default_segment_timeout is: $${default_segment_timeout} ---"; \
	  echo " Get gpconfig returns empty result " && exit -1; \
	fi; \
	gpconfig -c statement_timeout -v 60000 && \
	gpstop -u && \
	gpconfig -s statement_timeout && \
	if [[ "$$OSTYPE" == "darwin"* ]] && gpconfig -s gp_interconnect_type | grep udp -q ; then \
		echo  "**NOTE** Ignore parallel retrieve cursor fault injection test because of a known MacOS issue https://github.com/greenplum-db/gpdb/issues/8374" ; \
		./pg_isolation2_regress $(EXTRA_REGRESS_OPTS) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_parallel_retrieve_cursor --bindir='$(bindir)' --inputdir=$(srcdir) --dbname=isolation2parallelretrcursor --load-extension=gp_inject_fault --schedule=$(srcdir)/parallel_retrieve_cursor_schedule --exclude-test="parallel_retrieve_cursor/fault_inject_2" ; \
		ret1=$$? ; \
	else \
		./pg_isolation2_regress $(EXTRA_REGRESS_OPTS) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_parallel_retrieve_cursor --bindir='$(bindir)' --inputdir=$(srcdir) --dbname=isolation2parallelretrcursor --load-extension=gp_inject_fault --schedule=$(srcdir)/parallel_retrieve_cursor_schedule ; \
		ret1=$$? ; \
	fi; \
	./test_parallel_retrieve_cursor_extended_query $${USER} "postgres" > test_parallel_retrieve_cursor_extended_query.log 2>&1 ; \
	ret2=$$? ; \
	if [ "$${ret2}" == 0 ]; then \
		echo "[PASS]: test_parallel_retrieve_cursor_extended_query test pass. See test_parallel_retrieve_cursor_extended_query.log." ; echo "" ; \
	else \
		echo "[ERROR]: test_parallel_retrieve_cursor_extended_query test failed." ; \
		cat test_parallel_retrieve_cursor_extended_query.log ; echo "" ; \
	fi; \
	./test_parallel_retrieve_cursor_extended_query_error $${USER} "postgres" > test_parallel_retrieve_cursor_extended_query_error.log 2>&1 ; \
	ret3=$$? ; \
	if [ "$${ret3}" == 0 ]; then \
		echo "[PASS]: test_parallel_retrieve_cursor_extended_query_error test pass. See test_parallel_retrieve_cursor_extended_query_error.log." ; echo "" ; \
	else \
		echo "[ERROR]: test_parallel_retrieve_cursor_extended_query_error test failed." ; \
		cat test_parallel_retrieve_cursor_extended_query_error.log ; echo "" ; \
	fi; \
	gpconfig -c statement_timeout -v $${default_segment_timeout} -m $${default_coordinator_timeout} && \
	gpstop -u && \
	gpconfig -s statement_timeout && \
	exit "$$(( $${ret1} || $${ret2} || $${ret3} ))"
