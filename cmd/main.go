package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"app/bootstrap"
)

func main() {
	logrus.SetFormatter(new(logrus.JSONFormatter))

	env := bootstrap.NewEnv()

	db := connectToDB(env)

	DBName := env.DBDriver
	runDBMigration(db, DBName, env.MigrationURL)

	solve(db)
}

func connectToDB(env *bootstrap.Env) *sql.DB {
	logrus.Print(env.DBSource)
	db, err := sql.Open(env.DBDriver, env.DBSource)
	if err != nil {
		logrus.Fatalf("failed to connect to Postgresql: %v", err)
	}
	err = db.Ping()
	if err != nil {
		logrus.Fatalf("failed to connect to Postgresql: %v", err)
	}
	logrus.Infof("connected to Postgresql")

	return db
}

func runDBMigration(db *sql.DB, DBname, migrationURL string) {
	driver, _ := postgres.WithInstance(db, &postgres.Config{})

	migration, err := migrate.NewWithDatabaseInstance(
		migrationURL,
		DBname, // "postgres"
		driver)
	if err != nil {
		logrus.Fatalf("cannot create new migrate instance: %v", err)
	}

	if err = migration.Up(); err != nil && err != migrate.ErrNoChange {
		logrus.Fatalf("failed to run migrate up: %v", err)
	}

	logrus.Printf("db migrated successfully")
}

type PartOrderList []PartOrder

type PartOrder struct {
	OrderID   int
	ProductID int
	Quantity  int
}

var productsNames map[int]string
var racksNames map[int]string
var mainRacks map[int]int
var secondaryRacks map[int][]int

func solve(db *sql.DB) {
	ctx := context.Background()

	productsNames = make(map[int]string)
	racksNames = make(map[int]string)
	mainRacks = make(map[int]int)
	secondaryRacks = make(map[int][]int)

	orderIDs := parseOrderIDs(os.Args[1])

	orders, err := getOrders(db, ctx, orderIDs)
	if err != nil {
		logrus.Fatalf("failed to get orders: %v", err)
	}

	productIDs := make([]int, 0, len(productsNames))
	for productID := range productsNames {
		productIDs = append(productIDs, productID)
	}

	err = initProductsNames(db, ctx, productIDs)
	if err != nil {
		logrus.Fatalf("failed to init products names: %v", err)
	}

	err = initRacks(db, ctx, productIDs)
	if err != nil {
		logrus.Fatalf("failed to init racks: %v", err)
	}

	racks, err := groupOrdersByRack(db, ctx, orders)
	if err != nil {
		logrus.Fatalf("failed to group orders by rack: %v", err)
	}

	printRacksAndOrders(racks)
}

func parseOrderIDs(orderIDsStr string) []int {
	orderIDs := make([]int, 0)
	for _, orderIDStr := range strings.Split(orderIDsStr, ",") {
		orderID, _ := strconv.Atoi(orderIDStr)
		orderIDs = append(orderIDs, orderID)
	}
	return orderIDs
}

func getOrders(db *sql.DB, ctx context.Context, orderIDs []int) (map[int]PartOrderList, error) {
	const getOrdersQuery = `
SELECT o.order_id, o.product_id, o.quantity
FROM order_product o
JOIN product p ON p.product_id = o.product_id
WHERE o.order_id = ANY($1)
`
	rows, err := db.QueryContext(ctx, getOrdersQuery, pq.Array(orderIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query orders: %v", err)
	}
	defer rows.Close()

	orders := make(map[int]PartOrderList)
	for rows.Next() {
		var partOrder PartOrder

		err := rows.Scan(&partOrder.OrderID, &partOrder.ProductID, &partOrder.Quantity)
		if err != nil {
			return nil, fmt.Errorf("failed to scan order row: %v", err)
		}

		orders[partOrder.OrderID] = append(orders[partOrder.OrderID], partOrder)
		productsNames[partOrder.ProductID] = ""
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate order rows: %v", err)
	}

	return orders, nil
}

func initProductsNames(db *sql.DB, ctx context.Context, productIDs []int) error {
	const getProductsQuery = `
SELECT product_id, product_name
FROM product
WHERE product_id = ANY($1)
`

	rows, err := db.QueryContext(ctx, getProductsQuery, pq.Array(productIDs))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var productID int
		var productName string
		if err := rows.Scan(&productID, &productName); err != nil {
			return err
		}

		productsNames[productID] = productName
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func initRacks(db *sql.DB, ctx context.Context, productIDs []int) error {
	const getProductRacksQuery = `
SELECT product_id, rack_id, is_main
FROM product_rack
WHERE product_id = ANY($1)
`

	rows, err := db.QueryContext(ctx, getProductRacksQuery, pq.Array(productIDs))
	if err != nil {
		return err
	}
	defer rows.Close()

	rackIDs := make([]int, 0)
	for rows.Next() {
		var productID int
		var rackID int
		var isMain bool

		if err := rows.Scan(&productID, &rackID, &isMain); err != nil {
			return err
		}

		if isMain {
			mainRacks[productID] = rackID
		} else {
			secondaryRacks[productID] = append(secondaryRacks[productID], rackID)
		}

		rackIDs = append(rackIDs, rackID)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	initRacksNames(db, ctx, rackIDs)

	return nil
}

func initRacksNames(db *sql.DB, ctx context.Context, rackIDs []int) error {
	const getRacksQuery = `
SELECT rack_id, rack_name
FROM rack
WHERE rack_id = ANY($1)
`

	rows, err := db.QueryContext(ctx, getRacksQuery, pq.Array(rackIDs))
	if err != nil {
		return err
	}
	defer rows.Close()

	racksNames = make(map[int]string)
	for rows.Next() {
		var rackID int
		var rackName string
		if err := rows.Scan(&rackID, &rackName); err != nil {
			return err
		}

		racksNames[rackID] = rackName
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func groupOrdersByRack(db *sql.DB, ctx context.Context, orders map[int]PartOrderList) (map[string]PartOrderList, error) {
	racks := make(map[string]PartOrderList)

	for _, partOrderList := range orders {
		for _, partOrder := range partOrderList {
			rackID := mainRacks[partOrder.ProductID]
			rackName := racksNames[rackID]
			racks[rackName] = append(racks[rackName], partOrder)
		}
	}

	return racks, nil
}

func printRacksAndOrders(racks map[string]PartOrderList) {
	fmt.Println("=+=+=+=")

	sortedRacksNames := getSortedRacksNames(racks)
	for _, rackName := range sortedRacksNames {
		fmt.Printf("===Стеллаж %s\n", rackName)

		partOrderList := racks[rackName]
		for _, partOrder := range partOrderList {
			productID := partOrder.ProductID
			productName := productsNames[productID]
			fmt.Printf("%s (id=%d)\n", productName, productID)
			fmt.Printf("заказ %d, %d шт\n", partOrder.OrderID, partOrder.Quantity)

			if len(secondaryRacks[productID]) > 0 {
				fmt.Printf("доп стеллаж: ")
				listAdditionalRacksStr := ""
				for _, secondaryRackID := range secondaryRacks[productID] {
					listAdditionalRacksStr += racksNames[secondaryRackID] + ","
				}
				fmt.Println(listAdditionalRacksStr[:len(listAdditionalRacksStr)-1])
			}

			fmt.Println()
		}
	}
}

func getSortedRacksNames(racks map[string]PartOrderList) []string {
	racksNames := make([]string, 0, len(racks))
	for rackName := range racks {
		racksNames = append(racksNames, rackName)
	}
	sort.Strings(racksNames)

	return racksNames
}
