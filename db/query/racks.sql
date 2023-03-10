-- name: GetMainRack :one
SELECT * FROM racks
WHERE product_id = $1 AND is_main = true
LIMIT 1;

-- name: GetSecondaryRacks :many
SELECT * FROM racks
WHERE product_id = $1 AND is_main = false;
