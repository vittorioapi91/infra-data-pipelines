-- Create the view that builds full category paths from PostgreSQL categories table
CREATE OR REPLACE VIEW category_paths AS
WITH RECURSIVE cats AS (
    -- Base: only the root node(s), so each category appears once with one path
    SELECT
        category_id,
        name,
        parent_id,
        CAST(name AS VARCHAR) AS full_path
    FROM categories
    WHERE (parent_id IS NULL OR parent_id = 0) AND (parent_id <> 0 OR category_id = 0)

    UNION ALL

    -- Recursive: attach children to their parent path (exclude self-loop: root 0 has parent_id=0)
    SELECT
        c.category_id,
        c.name,
        c.parent_id,
        CONCAT(p.full_path, ' / ', c.name) AS full_path
    FROM categories c
    JOIN cats p
      ON c.parent_id = p.category_id
      AND c.category_id <> p.category_id
)
SELECT
    category_id,
    name,
    parent_id,
    full_path
FROM cats;