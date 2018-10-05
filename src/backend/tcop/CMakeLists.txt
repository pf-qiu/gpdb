cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    dest.c fastpath.c postgres.c pquery.c utility.c
    idle_resource_cleaner.c
    PARENT_SCOPE
)