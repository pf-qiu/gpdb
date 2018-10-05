cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    aggregatecmds.c alter.c analyze.c analyzeutils.c async.c cluster.c comment.c
	collationcmds.c constraint.c conversioncmds.c copy.c createas.c
	dbcommands.c define.c discard.c dropcmds.c
	event_trigger.c explain.c extension.c foreigncmds.c functioncmds.c
	indexcmds.c lockcmds.c matview.c operatorcmds.c opclasscmds.c
	portalcmds.c prepare.c proclang.c
	schemacmds.c seclabel.c sequence.c tablecmds.c tablespace.c trigger.c
	tsearchcmds.c typecmds.c user.c vacuum.c vacuumlazy.c
	variable.c view.c
    analyzefuncs.c extprotocolcmds.c exttablecmds.c queue.c
    resgroupcmds.c
    PARENT_SCOPE
)