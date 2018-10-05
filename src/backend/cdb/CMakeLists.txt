cmake_minimum_required(VERSION 3.12)

set(SUBDIRS dispatcher motion)

foreach(SUBDIR ${SUBDIRS})
    add_subdirectory(${SUBDIR})
    list(TRANSFORM CHILD_FILES PREPEND "${SUBDIR}/")
    set(CDB_FILES ${CDB_FILES} ${CHILD_FILES})    
endforeach(SUBDIR)

set(CHILD_FILES
    cdbappendonlystorageformat.c
    cdbappendonlystorageread.c cdbappendonlystoragewrite.c
	cdbbufferedappend.c cdbbufferedread.c
	cdbcat.c cdbcopy.c
	cdbdistributedsnapshot.c
	cdbdistributedxid.c cdbdistributedxacts.c
	cdbdoublylinked.c
	cdbdtxcontextinfo.c
	cdbfts.c
	cdbgroup.c
	cdbhash.c cdbheap.c
	cdbllize.c cdblocaldistribxact.c
	cdbappendonlyxlog.c
	cdbmutate.c
	cdboidsync.c
	cdbpartindex.c
	cdbpartition.c
	cdbpath.c cdbpathlocus.c cdbpathtoplan.c
	cdbpgdatabase.c
	cdbplan.c cdbpullup.c
	cdbrelsize.c
	cdbsetop.c cdbsreh.c cdbsrlz.c cdbsubplan.c cdbsubselect.c
	cdbtargeteddispatch.c cdbthreadlog.c
	cdbtimer.c
	cdbtm.c cdbtmutils.c
	cdbutil.c
	cdbvars.c cdbvarblock.c
	partitionselection.c
	${CDB_FILES}
	PARENT_SCOPE
)