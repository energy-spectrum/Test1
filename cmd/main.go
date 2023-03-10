package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"app/bootstrap"
	"app/db"
	sqlc_db "app/db/sqlc"
)

func main() {
	logrus.SetFormatter(new(logrus.JSONFormatter))

	env := bootstrap.NewEnv()

	db, store := connectToDB(env)

	DBName := env.DBDriver
	runDBMigration(db, DBName, env.MigrationURL)

	solve(store)
}

func connectToDB(env *bootstrap.Env) (*sql.DB, sqlc_db.Store) {
	logrus.Print(env.DBSource)
	db, err := db.Connect(env.DBDriver, env.DBSource)
	if err != nil {
		logrus.Fatalf("failed to connect to Postgresql: %v", err)
	}
	logrus.Infof("connected to Postgresql")

	store := sqlc_db.NewStore(db)
	return db, store
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

type Product struct {
	ID             int64
	Name           string
	Quantity       int
	SecondaryRacks []string
}

type PartOrder struct {
	ID      int64
	Product *Product
}

func solve(store sqlc_db.Store) {
	ctx := context.Background()

	orderIDs := parseOrderIDs(os.Args[1])

	orders, err := getOrders(ctx, store, orderIDs)
	if err != nil {
		logrus.Fatalf(err.Error())
	}

	racks, err := groupOrdersByRack(ctx, store, orders)
	if err != nil {
		logrus.Fatalf(err.Error())
	}

	printRacksAndOrders(racks, orders)
}

func parseOrderIDs(orderIDsStr string) []int64 {
	orderIDs := []int64{}
	for _, orderIDStr := range strings.Split(orderIDsStr, ",") {
		orderID, _ := strconv.ParseInt(orderIDStr, 10, 64)
		orderIDs = append(orderIDs, orderID)
	}
	return orderIDs
}

func getOrders(ctx context.Context, store sqlc_db.Store, orderIDs []int64) (map[int64][]*Product, error) {
	orders := make(map[int64][]*Product)
	for _, orderID := range orderIDs {
		ordersSQLC, err := store.GetOrder(ctx, orderID)
		if err != nil {
			return nil, fmt.Errorf("failed to get order with ID %d: %v", orderID, err)
		}

		products := make([]*Product, 0, len(ordersSQLC))
		for _, sqlcOrder := range ordersSQLC {
			productName, err := getProductName(ctx, store, sqlcOrder.ProductID)
			if err != nil {
				return nil, err
			}

			products = append(products, &Product{
				ID:       sqlcOrder.ProductID,
				Name:     productName,
				Quantity: int(sqlcOrder.Quantity),
			})
		}
		orders[orderID] = products
	}
	return orders, nil
}

var productsName map[int64]string = make(map[int64]string, 0)

func getProductName(ctx context.Context, store sqlc_db.Store, id int64) (string, error) {
	if name, ok := productsName[id]; ok {
		return name, nil
	} else {
		productSQLC, err := store.GetProduct(ctx, id)
		if err != nil {
			return "", fmt.Errorf("failed to get product %d: %v", id, err)
		}

		productsName[id] = productSQLC.Name
		return productsName[id], nil
	}
}

func groupOrdersByRack(ctx context.Context, store sqlc_db.Store, orders map[int64][]*Product) (map[string][]PartOrder, error) {
	racks := make(map[string][]PartOrder)

	for orderID, products := range orders {
		for _, product := range products {
			rack, err := store.GetMainRack(ctx, product.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to get main rack for product %d: %v", product.ID, err)
			}

			racks[rack.Name] = append(racks[rack.Name], PartOrder{
				ID:      orderID,
				Product: product,
			})

			secondaryRacks, err := store.GetSecondaryRacks(ctx, product.ID)
			if err != nil {
				return nil, fmt.Errorf("failed to get secondary racks for product %d: %v", product.ID, err)
			}

			for _, secondaryRack := range secondaryRacks {
				product.SecondaryRacks = append(product.SecondaryRacks, secondaryRack.Name)
			}
		}
	}

	return racks, nil
}

func printRacksAndOrders(racks map[string][]PartOrder, orders map[int64][]*Product) {
	fmt.Println("=+=+=+=")

	for rackName, listPartOrder := range racks {
		fmt.Printf("===Стеллаж %s\n", rackName)
		for _, partOrder := range listPartOrder {
			orderID := partOrder.ID
			product := partOrder.Product

			fmt.Printf("%s (id=%d)\n", product.Name, product.ID)
			fmt.Printf("заказ %d, %d шт\n", orderID, product.Quantity)

			if len(product.SecondaryRacks) > 0 {
				fmt.Printf("доп стеллаж: ")
				listAdditionalRacksStr := ""
				for _, secondaryRack := range product.SecondaryRacks {
					listAdditionalRacksStr += secondaryRack + ","
				}
				fmt.Println(listAdditionalRacksStr[:len(listAdditionalRacksStr)-1])
			}

			fmt.Println()
		}
	}
}
