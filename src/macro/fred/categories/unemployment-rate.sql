


SELECT s.series_id
FROM category_paths cp
JOIN category_analysis ca ON ca.category_id = cp.category_id AND ca.is_branch = false
JOIN series s ON s.category_id = cp.category_id
WHERE cp.full_path LIKE '%Unemployment%' and cp.full_path not like '%U.S. Regional Data%' and cp.full_path not like '%DISCONTINUED%'
  AND s.observation_end >= '2025-01-01'
ORDER BY s.series_id;
