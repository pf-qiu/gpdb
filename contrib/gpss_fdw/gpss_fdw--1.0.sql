/* contrib/gpss_fdw/gpss_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gpss_fdw" to load this file. \quit

CREATE FUNCTION gpss_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION gpss_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER gpss_fdw
  HANDLER gpss_fdw_handler
  VALIDATOR gpss_fdw_validator;
