cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    analyzejoins.c createplan.c initsplan.c planagg.c planmain.c planner.c
    setrefs.c subselect.c
    plangroupext.c
    planshare.c
    planpartition.c
    transform.c
    PARENT_SCOPE)
