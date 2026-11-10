-- rollback migration: 0021
-- description:        drop distributed tracing tables

DROP TABLE IF EXISTS trace_links_registry CASCADE;
DROP TABLE IF EXISTS trace_spans_buffer CASCADE;
DROP TABLE IF EXISTS trace_sampling_rules CASCADE;
DROP TABLE IF EXISTS deep_trace_sessions CASCADE;

