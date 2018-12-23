cmake_minimum_required(VERSION 3.12)

file(READ "${GPDB_SRC_DIR}/src/include/catalog/gp_version.in" GP_VERSION_IN)
string(REPLACE "$$Name:  $$" ${GP_VERSION} GP_VERSION_OUT "${GP_VERSION_IN}")
file(WRITE "${GPDB_SRC_DIR}/src/include/catalog/gp_version.h" "${GP_VERSION_OUT}")

set(POSTGRES_BKI_SRCS pg_type.h pg_attribute.h pg_class.h
	pg_attrdef.h pg_constraint.h pg_inherits.h pg_index.h pg_operator.h
	pg_opfamily.h pg_opclass.h pg_am.h pg_amop.h pg_amproc.h
	pg_language.h pg_largeobject_metadata.h pg_largeobject.h pg_aggregate.h
	pg_statistic.h pg_rewrite.h pg_trigger.h pg_event_trigger.h pg_description.h
	pg_cast.h pg_enum.h pg_namespace.h pg_conversion.h pg_depend.h
	pg_database.h pg_db_role_setting.h pg_tablespace.h pg_pltemplate.h
	pg_authid.h pg_auth_members.h pg_shdepend.h pg_shdescription.h
	pg_ts_config.h pg_ts_config_map.h pg_ts_dict.h
	pg_ts_parser.h pg_ts_template.h pg_extension.h
	pg_foreign_data_wrapper.h pg_foreign_server.h pg_user_mapping.h
	pg_foreign_table.h
	pg_default_acl.h pg_seclabel.h pg_shseclabel.h pg_collation.h pg_range.h
	pg_resqueue.h pg_resqueuecapability.h pg_resourcetype.h
	pg_resgroup.h pg_resgroupcapability.h
	gp_configuration_history.h gp_id.h gp_policy.h gp_version.h
	gp_segment_config.h
	pg_exttable.h pg_appendonly.h
	gp_fastsequence.h pg_extprotocol.h
	pg_partition.h pg_partition_rule.h
	pg_attribute_encoding.h
	pg_auth_time_constraint.h
	pg_compression.h
	pg_proc_callback.h
	pg_partition_encoding.h
	pg_type_encoding.h
	pg_stat_last_operation.h pg_stat_last_shoperation.h
	toasting.h indexing.h
    )

list(TRANSFORM POSTGRES_BKI_SRCS PREPEND ${GPDB_SRC_DIR}/src/include/catalog/)
list(APPEND POSTGRES_BKI_SRCS "pg_proc_combined.h")

execute_process(COMMAND ${PERL_EXECUTABLE} "-I" "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/genbki.pl"
    "-I" "${GPDB_SRC_DIR}/src/include/catalog"
    "--set-version=${PG_MAJORVERSION}"
    ${POSTGRES_BKI_SRCS}
    RESULT_VARIABLE ret
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
if (NOT ret EQUAL 0)
    message(FATAL_ERROR "genbki.pl failed")
endif()

set(CHILD_FILES
    catalog.c dependency.c heap.c index.c indexing.c namespace.c aclchk.c
    objectaccess.c objectaddress.c pg_aggregate.c pg_collation.c
    pg_constraint.c pg_conversion.c
    pg_depend.c pg_enum.c pg_inherits.c pg_largeobject.c pg_namespace.c
    pg_operator.c pg_proc.c pg_range.c pg_db_role_setting.c pg_shdepend.c
    pg_type.c storage.c toasting.c
    pg_exttable.c pg_extprotocol.c
    pg_proc_callback.c
    aoseg.c aoblkdir.c gp_fastsequence.c gp_segment_config.c
    pg_attribute_encoding.c pg_compression.c aovisimap.c
    pg_appendonly.c
    oid_dispatch.c aocatalog.c
	PARENT_SCOPE
)

install(FILES postgres.bki postgres.description postgres.shdescription
    system_views.sql information_schema.sql cdb_util.sql cdb_schema.sql
	sql_features.txt gp_toolkit.sql
    DESTINATION share/postgresql)