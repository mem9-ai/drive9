-- Add image embedding support while keeping text embedding compatibility.
--
-- TiDB / MySQL path (tidb_zero, tidb_cloud_starter):
-- Keep existing `embedding` text column/index unchanged.
ALTER TABLE files
  ADD COLUMN IF NOT EXISTS embedding_image VECTOR(1024)
    GENERATED ALWAYS AS (
      EMBED_IMAGE('tidbcloud_free/amazon/titan-embed-image-v1', content_blob, '{"dimensions": 1024}')
    ) STORED;

ALTER TABLE files
  ADD VECTOR INDEX idx_files_cosine_image((VEC_COSINE_DISTANCE(embedding_image)))
  ADD_COLUMNAR_REPLICA_ON_DEMAND;

-- db9 / PostgreSQL path:
-- Keep existing `embedding` text column/index unchanged.
ALTER TABLE files
  ADD COLUMN IF NOT EXISTS embedding_image vector(1024)
    GENERATED ALWAYS AS (
      EMBED_IMAGE('tidbcloud_free/amazon/titan-embed-image-v1', content_blob, '{"dimensions": 1024}'::jsonb)
    ) STORED;

CREATE INDEX IF NOT EXISTS idx_files_cosine_image
  ON files USING hnsw (embedding_image vector_cosine_ops);
