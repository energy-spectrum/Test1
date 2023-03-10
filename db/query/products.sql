-- name: GetProduct :one
SELECT * FROM products
WHERE id = $1
LIMIT 1;
