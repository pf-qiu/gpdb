/* gpcontrib/gp2gp_fdw/gp2gp_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp2gp_fdw" to load this file. \quit

CREATE FUNCTION gp2gp_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION gp2gp_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER gp2gp_fdw
    HANDLER gp2gp_fdw_handler
    VALIDATOR gp2gp_fdw_validator;
