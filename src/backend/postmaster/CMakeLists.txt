cmake_minimum_required(VERSION 3.12)

project(postmaster)

set(CHILD_FILES
    autovacuum.c bgwriter.c checkpointer.c fork_process.c pgarch.c pgstat.c
	postmaster.c syslogger.c startup.c
	perfmon.c backoff.c perfmon_segmentinfo.c
	sendalert.c alertseverity.c autostats.c walwriter.c
    PARENT_SCOPE
)