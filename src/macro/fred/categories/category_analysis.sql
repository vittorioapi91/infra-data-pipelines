-- Analyze categories to identify leaves vs branches
-- A branch category has children (other categories with this category_id as parent_id)
-- A leaf category has no children

-- Option 1: Add is_branch, child_count, and parent_count columns to a view
CREATE OR REPLACE VIEW category_analysis AS
WITH RECURSIVE category_paths AS (
    -- Base case: only the root node(s), so each category appears once in the tree
    SELECT 
        category_id,
        name,
        parent_id,
        ARRAY[category_id] AS path,
        0 AS depth
    FROM categories
    WHERE (parent_id IS NULL OR parent_id = 0) AND (parent_id <> 0 OR category_id = 0)

    UNION ALL
    
    -- Recursive case: children with their parent paths (exclude self-loop: root 0 has parent_id=0)
    SELECT 
        c.category_id,
        c.name,
        c.parent_id,
        cp.path || c.category_id,
        cp.depth + 1
    FROM categories c
    JOIN category_paths cp ON c.parent_id = cp.category_id
      AND c.category_id <> cp.category_id
)
SELECT 
    c.category_id,
    c.name,
    c.parent_id,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM categories child 
            WHERE child.parent_id = c.category_id
        ) THEN true 
        ELSE false 
    END AS is_branch,
    (
        SELECT COUNT(*) 
        FROM categories child 
        WHERE child.parent_id = c.category_id
    ) AS child_count,
    COALESCE(cp.depth, 0) AS parent_count
FROM categories c
LEFT JOIN category_paths cp ON c.category_id = cp.category_id;

