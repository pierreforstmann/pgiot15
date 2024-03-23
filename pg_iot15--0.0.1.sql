/* pg_iot15-0.0.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_iot15" to load this file. \quit

CREATE FUNCTION pg_iot15_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD pg_iot15 TYPE TABLE HANDLER pg_iot15_handler;
COMMENT ON ACCESS METHOD pg_iot15 IS 'Table AM for insert-only table';
