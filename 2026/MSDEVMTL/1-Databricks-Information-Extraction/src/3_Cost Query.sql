-- Databricks notebook source
-- Cost query
--   Agent Bricks Information Extraction: billing_origin_product: MODEL_SERVING, endpoint_name matching Agent Bricks' serving endpoint.
--   ai_parse_document: billing_origin_product: AI_FUNCTIONS, endpoint_name = databricks-ai-parse-document

SELECT 
  usage_metadata.endpoint_name,
  usage_metadata.endpoint_id,
  sku_name,
  billing_origin_product,
  COUNT(*) as record_count,
  SUM(usage_quantity) as total_dbu_usage,
  usage_date
FROM system.billing.usage
WHERE usage_metadata.endpoint_name IS NOT NULL
  AND usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY 
  usage_metadata.endpoint_name,
  usage_metadata.endpoint_id,
  sku_name,
  billing_origin_product,
  usage_date
ORDER BY usage_date DESC, usage_metadata.endpoint_name;
