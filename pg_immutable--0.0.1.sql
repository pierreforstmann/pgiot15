/* pg_immutable-0.0.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_immutable" to load this file. \quit

CREATE FUNCTION pg_immutable_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD pg_immutable TYPE TABLE HANDLER pg_immutable_handler;
COMMENT ON ACCESS METHOD pg_immutable IS 'Table AM for insert-only table';
