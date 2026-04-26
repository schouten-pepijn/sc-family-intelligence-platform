-- Spatial health-check for staging.stg_bag_pand_spatial
-- Exits with non-zero if any check fails

DO $$
DECLARE
  null_count bigint;
  srids integer[];
  gist_exists boolean;
BEGIN
  -- ensure PostGIS is installed
  PERFORM 1 FROM pg_extension WHERE extname = 'postgis';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'postgis extension not installed';
  END IF;

  -- check for null geom
  EXECUTE 'SELECT count(*) FROM staging.stg_bag_pand_spatial WHERE geom IS NULL' INTO null_count;
  IF null_count > 0 THEN
    RAISE EXCEPTION 'Found % rows with null geom', null_count;
  END IF;

  -- check SRID
  EXECUTE 'SELECT array_agg(DISTINCT st_srid(geom)) FROM staging.stg_bag_pand_spatial WHERE geom IS NOT NULL' INTO srids;
  IF srids IS NULL THEN
    RAISE EXCEPTION 'No non-null geoms found in staging.stg_bag_pand_spatial';
  END IF;
  IF array_length(srids,1) <> 1 OR srids[1] <> 4326 THEN
    RAISE EXCEPTION 'Unexpected SRIDs found: %', srids;
  END IF;

  -- check for GiST index
  SELECT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'staging'
      AND tablename = 'stg_bag_pand_spatial'
      AND indexdef ILIKE '%gist%'
  ) INTO gist_exists;
  IF NOT gist_exists THEN
    RAISE EXCEPTION 'No GiST index found for staging.stg_bag_pand_spatial';
  END IF;

  RAISE NOTICE 'Spatial health check passed';
END
$$;
