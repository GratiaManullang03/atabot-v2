-- ATABOT 2.0 Database Initialization
-- Creates schema, tables, and functions for the universal adaptive business intelligence system

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For text search

-- Create ATABOT schema
CREATE SCHEMA IF NOT EXISTS atabot;

-- Set search path
SET search_path TO atabot, public;

-- =============================================================================
-- MANAGED SCHEMAS TABLE
-- Tracks all schemas being managed by ATABOT
-- =============================================================================
CREATE TABLE IF NOT EXISTS atabot.managed_schemas (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- Changed from uuid_generate_v4()
    schema_name TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    business_domain TEXT,
    is_active BOOLEAN DEFAULT false,
    metadata JSONB DEFAULT '{}',
    learned_patterns JSONB DEFAULT '{}',
    total_tables INTEGER DEFAULT 0,
    total_rows BIGINT DEFAULT 0,
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_synced_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_managed_schemas_active ON atabot.managed_schemas(is_active);
CREATE INDEX idx_managed_schemas_domain ON atabot.managed_schemas(business_domain);

-- =============================================================================
-- EMBEDDINGS TABLE
-- Stores vector embeddings for all synchronized data
-- =============================================================================
CREATE TABLE IF NOT EXISTS atabot.embeddings (
    id TEXT PRIMARY KEY,  -- Composite key: schema_table_recordid
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    content TEXT NOT NULL,  -- Searchable text representation
    embedding vector(1024) NOT NULL,  -- VoyageAI embedding dimension
    metadata JSONB DEFAULT '{}',  -- Original row data
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_embeddings_schema_table ON atabot.embeddings(schema_name, table_name);
CREATE INDEX idx_embeddings_created ON atabot.embeddings(created_at DESC);

-- Vector similarity search index (IVFFlat)
CREATE INDEX idx_embeddings_vector ON atabot.embeddings 
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);  -- Adjust lists parameter based on data size

-- Full text search index on content
CREATE INDEX idx_embeddings_content_gin ON atabot.embeddings 
USING gin(to_tsvector('english', content));

-- =============================================================================
-- SYNC STATUS TABLE
-- Tracks synchronization status for each table
-- =============================================================================
CREATE TABLE IF NOT EXISTS atabot.sync_status (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- Changed
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    sync_status TEXT DEFAULT 'pending', -- pending, running, completed, failed
    last_sync_started TIMESTAMP WITH TIME ZONE,
    last_sync_completed TIMESTAMP WITH TIME ZONE,
    rows_synced BIGINT DEFAULT 0,
    last_error TEXT,
    realtime_enabled BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(schema_name, table_name)
);

CREATE INDEX idx_sync_status_schema ON atabot.sync_status(schema_name);
CREATE INDEX idx_sync_status_status ON atabot.sync_status(sync_status);

-- =============================================================================
-- QUERY LOGS TABLE
-- Logs all queries for learning and analytics
-- =============================================================================
CREATE TABLE IF NOT EXISTS atabot.query_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- Changed
    session_id TEXT,
    user_id TEXT,
    query TEXT NOT NULL,
    query_type TEXT,  -- search, aggregation, comparison, etc.
    schema_name TEXT,
    tables_accessed TEXT[],
    response_time_ms FLOAT,
    result_count INTEGER,
    success BOOLEAN DEFAULT true,
    error_message TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_query_logs_session ON atabot.query_logs(session_id);
CREATE INDEX idx_query_logs_created ON atabot.query_logs(created_at DESC);
CREATE INDEX idx_query_logs_schema ON atabot.query_logs(schema_name);

-- =============================================================================
-- LEARNED PATTERNS TABLE
-- Stores patterns learned from data and queries
-- =============================================================================
CREATE TABLE IF NOT EXISTS atabot.learned_patterns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- Changed
    pattern_type TEXT NOT NULL,  -- entity, relationship, query, terminology
    schema_name TEXT,
    table_name TEXT,
    pattern_data JSONB NOT NULL,
    confidence FLOAT DEFAULT 0.5,
    usage_count INTEGER DEFAULT 0,
    last_used TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_learned_patterns_type ON atabot.learned_patterns(pattern_type);
CREATE INDEX idx_learned_patterns_schema ON atabot.learned_patterns(schema_name);
CREATE INDEX idx_learned_patterns_confidence ON atabot.learned_patterns(confidence DESC);

-- =============================================================================
-- UNUSED TABLES REMOVED
-- Query cache and conversation contexts removed as they're handled by MCP
-- =============================================================================
-- Tables removed:
-- - atabot.query_cache (unused - no query caching implemented)
-- - atabot.conversation_contexts (replaced by MCP context system)

-- =============================================================================
-- FUNCTIONS FOR REAL-TIME SYNC
-- =============================================================================

-- Function to notify on data changes
CREATE OR REPLACE FUNCTION atabot.notify_data_change() 
RETURNS TRIGGER AS $$
DECLARE
    payload JSON;
    table_name TEXT;
    schema_name TEXT;
BEGIN
    table_name := TG_TABLE_NAME;
    schema_name := TG_TABLE_SCHEMA;
    
    -- Build notification payload
    payload := json_build_object(
        'operation', TG_OP,
        'schema', schema_name,
        'table', table_name,
        'timestamp', NOW()
    );
    
    -- Include row data for INSERT and UPDATE
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        payload := json_build_object(
            'operation', TG_OP,
            'schema', schema_name,
            'table', table_name,
            'timestamp', NOW(),
            'new_data', row_to_json(NEW)
        );
    ELSIF TG_OP = 'DELETE' THEN
        payload := json_build_object(
            'operation', TG_OP,
            'schema', schema_name,
            'table', table_name,
            'timestamp', NOW(),
            'old_data', row_to_json(OLD)
        );
    END IF;
    
    -- Send notification
    PERFORM pg_notify('atabot_data_change', payload::text);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Function to calculate vector similarity (cosine)
CREATE OR REPLACE FUNCTION atabot.vector_similarity(
    vec1 vector,
    vec2 vector
) RETURNS FLOAT AS $$
BEGIN
    RETURN 1 - (vec1 <=> vec2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to search with filters
CREATE OR REPLACE FUNCTION atabot.hybrid_search(
    query_embedding vector,
    target_schema TEXT,
    target_table TEXT DEFAULT NULL,
    filters JSONB DEFAULT '{}',
    limit_count INTEGER DEFAULT 10
) RETURNS TABLE (
    id TEXT,
    content TEXT,
    metadata JSONB,
    similarity FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        e.id,
        e.content,
        e.metadata,
        1 - (e.embedding <=> query_embedding) as similarity
    FROM atabot.embeddings e
    WHERE e.schema_name = target_schema
        AND (target_table IS NULL OR e.table_name = target_table)
        AND (filters = '{}' OR e.metadata @> filters)
    ORDER BY e.embedding <=> query_embedding
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- UPDATE TRIGGERS
-- =============================================================================

-- Auto-update timestamps
CREATE OR REPLACE FUNCTION atabot.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for updated_at columns
CREATE TRIGGER update_managed_schemas_updated_at
    BEFORE UPDATE ON atabot.managed_schemas
    FOR EACH ROW EXECUTE FUNCTION atabot.update_updated_at();

CREATE TRIGGER update_embeddings_updated_at
    BEFORE UPDATE ON atabot.embeddings
    FOR EACH ROW EXECUTE FUNCTION atabot.update_updated_at();

CREATE TRIGGER update_sync_status_updated_at
    BEFORE UPDATE ON atabot.sync_status
    FOR EACH ROW EXECUTE FUNCTION atabot.update_updated_at();

CREATE TRIGGER update_learned_patterns_updated_at
    BEFORE UPDATE ON atabot.learned_patterns
    FOR EACH ROW EXECUTE FUNCTION atabot.update_updated_at();

-- =============================================================================
-- INITIAL DATA
-- =============================================================================

-- Insert default patterns
INSERT INTO atabot.learned_patterns (pattern_type, pattern_data, confidence)
VALUES 
    ('query', '{"type": "aggregation", "keywords": ["total", "sum", "count", "average"]}', 0.9),
    ('query', '{"type": "comparison", "keywords": ["compare", "versus", "difference"]}', 0.9),
    ('query', '{"type": "search", "keywords": ["find", "search", "show", "list"]}', 0.9)
ON CONFLICT DO NOTHING;

-- =============================================================================
-- PERMISSIONS (Adjust based on your user setup)
-- =============================================================================

-- Grant usage on schema
GRANT USAGE ON SCHEMA atabot TO PUBLIC;

-- Grant appropriate permissions on tables
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA atabot TO PUBLIC;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA atabot TO PUBLIC;

-- =============================================================================
-- MAINTENANCE NOTES
-- =============================================================================

-- Regularly run VACUUM ANALYZE on embeddings table for optimal performance
-- VACUUM ANALYZE atabot.embeddings;

-- Monitor index usage and adjust as needed
-- SELECT * FROM pg_stat_user_indexes WHERE schemaname = 'atabot';

-- Clean up old query logs periodically
-- DELETE FROM atabot.query_logs WHERE created_at < NOW() - INTERVAL '30 days';

-- Optimize vector index based on data size
-- For < 1M vectors: lists = 100
-- For 1M-10M vectors: lists = 1000  
-- For > 10M vectors: lists = sqrt(total_vectors)

COMMENT ON SCHEMA atabot IS 'ATABOT 2.0 - Universal Adaptive Business Intelligence System';
COMMENT ON TABLE atabot.embeddings IS 'Vector embeddings for all synchronized data';
COMMENT ON TABLE atabot.managed_schemas IS 'Schemas managed by ATABOT with learned patterns';
COMMENT ON TABLE atabot.query_logs IS 'Query history for learning and optimization';
COMMENT ON FUNCTION atabot.hybrid_search IS 'Hybrid search combining vector similarity and SQL filters';