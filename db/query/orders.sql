-- name: GetOrder :many
SELECT * FROM orders
WHERE id = $1;
