cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    analyze.c gram.c scan.c keywords.c kwlookup.c parser.c
    parse_agg.c parse_clause.c parse_coerce.c parse_collate.c parse_cte.c
    parse_expr.c parse_func.c parse_node.c parse_oper.c parse_param.c
    parse_relation.c parse_target.c parse_type.c parse_utilcmd.c scansup.c
    parse_partition.c
    PARENT_SCOPE
)