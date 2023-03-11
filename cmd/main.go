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

type PartOrder struct {
	OrderID  int64
	Product  *Product
	Quantity int
}

type Product struct {
	ID   int64
	Name string
}

var mainRacks map[int64]string
var secondaryRacks map[int64][]string

func solve(db *sql.DB) {
	ctx := context.Background()

	orderIDs := parseOrderIDs(os.Args[1])

	mainRacks = make(map[int64]string)
	secondaryRacks = make(map[int64][]string)

	orders, err := getOrders(db, ctx, orderIDs)
	if err != nil {
		logrus.Fatalf(err.Error())
	}

	err = initRacks(db, ctx)
	if err != nil {
		logrus.Fatalf(err.Error())
	}

	racks, err := groupOrdersByRack(db, ctx, orders)
	if err != nil {
		logrus.Fatalf(err.Error())
	}

	printRacksAndOrders(racks)
}

func parseOrderIDs(orderIDsStr string) []int64 {
	orderIDs := make([]int64, 0)
	for _, orderIDStr := range strings.Split(orderIDsStr, ",") {
		orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
		orderIDs = append(orderIDs, orderID)
	}
	return orderIDs
}

func getOrders(db *sql.DB, ctx context.Context, orderIDs []int64) (map[int64][]*PartOrder, error) {
	const getOrdersQuery = `
SELECT o.id, o.product_id, o.quantity, p.name
FROM orders o
JOIN products p ON p.id = o.product_id
WHERE o.id = ANY($1)
`
	rows, err := db.QueryContext(ctx, getOrdersQuery, pq.Array(orderIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query orders: %v", err)
	}
	defer rows.Close()

	orders := make(map[int64][]*PartOrder)
	for rows.Next() {
		var partOrder PartOrder = PartOrder{
			Product: &Product{},
		}

		err := rows.Scan(&partOrder.OrderID, &partOrder.Product.ID, &partOrder.Quantity, &partOrder.Product.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to scan order row: %v", err)
		}

		orders[partOrder.OrderID] = append(orders[partOrder.OrderID], &partOrder)
		mainRacks[partOrder.Product.ID] = ""
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate order rows: %v", err)
	}

	return orders, nil
}

func initRacks(db *sql.DB, ctx context.Context) error {
	productIDs := make([]int64, 0, len(mainRacks))
	for productID := range mainRacks {
		productIDs = append(productIDs, productID)
	}

	const getRacksQuery = `
SELECT product_id, name, is_main
FROM racks
WHERE product_id = ANY($1)
`

	rows, err := db.QueryContext(ctx, getRacksQuery, pq.Array(productIDs))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var productID int64
		var rackName string
		var isMain bool
		if err := rows.Scan(&productID, &rackName, &isMain); err != nil {
			return err
		}

		if isMain {
			mainRacks[productID] = rackName
		} else {
			secondaryRacks[productID] = append(secondaryRacks[productID], rackName)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func groupOrdersByRack(db *sql.DB, ctx context.Context, orders map[int64][]*PartOrder) (map[string][]*PartOrder, error) {
	racks := make(map[string][]*PartOrder)

	for _, listPartsOrder := range orders {
		for _, partOrder := range listPartsOrder {
			rackName := mainRacks[partOrder.Product.ID]
			racks[rackName] = append(racks[rackName], partOrder)
		}
	}

	return racks, nil
}

func printRacksAndOrders(racks map[string][]*PartOrder) {
	fmt.Println("=+=+=+=")

	racksNames := getSortedRacksNames(racks)

	for _, rackName := range racksNames {
		fmt.Printf("===Стеллаж %s\n", rackName)

		listPartsOrder := racks[rackName]
		for _, partOrder := range listPartsOrder {
			orderID := partOrder.OrderID
			product := partOrder.Product

			fmt.Printf("%s (id=%d)\n", product.Name, product.ID)
			fmt.Printf("заказ %d, %d шт\n", orderID, partOrder.Quantity)

			if len(secondaryRacks[product.ID]) > 0 {
				fmt.Printf("доп стеллаж: ")
				listAdditionalRacksStr := ""
				for _, secondaryRack := range secondaryRacks[product.ID] {
					listAdditionalRacksStr += secondaryRack + ","
				}
				fmt.Println(listAdditionalRacksStr[:len(listAdditionalRacksStr)-1])
			}

			fmt.Println()
		}
	}
}

func getSortedRacksNames(racks map[string][]*PartOrder) []string{
	racksNames := make([]string, 0, len(racks))
	for rackName := range racks{
		racksNames = append(racksNames, rackName)
	}
	sort.Strings(racksNames)

	return racksNames
}