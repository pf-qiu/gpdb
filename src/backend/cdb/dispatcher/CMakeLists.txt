cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    cdbconn.c cdbdisp.c cdbdisp_thread.c cdbdisp_async.c 
    cdbdispatchresult.c cdbdisp_dtx.c cdbdisp_query.c cdbgang.c 
	cdbgang_thread.c cdbgang_async.c cdbpq.c
    PARENT_SCOPE
)
