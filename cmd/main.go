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

	var orderIDs []int64 = make([]int64, 0)
	for _, orderIDstr := range strings.Split(os.Args[1], ",") {
		orderID, _ := strconv.ParseInt(orderIDstr, 10, 64)
		orderIDs = append(orderIDs, orderID)
	}

	var orders map[int64][]*Product = make(map[int64][]*Product)

	for _, orderID := range orderIDs {
		ordersSQLC, err := store.GetOrder(ctx, orderID)
		if err != nil {
			logrus.Fatalf(err.Error())
		}

		var products []*Product = make([]*Product, 0)
		for _, sqlcOrder := range ordersSQLC {
			products = append(products, &Product{
				ID:       sqlcOrder.ProductID,
				Quantity: int(sqlcOrder.Quantity),
			})
		}

		orders[orderID] = products
	}

	racks := make(map[string][]PartOrder)

	for _, orderID := range orderIDs {
		products := orders[orderID]

		for i := range products {
			rack, err := store.GetMainRack(ctx, products[i].ID)
			if err != nil {
				logrus.Fatalf(err.Error())
			}

			racks[rack.Name] = append(racks[rack.Name], PartOrder{
				ID:      orderID,
				Product: products[i],
			})

			secondaryRacks, err := store.GetSecondaryRacks(ctx, products[i].ID)
			if err != nil {
				logrus.Fatalf(err.Error())
			}
			for _, secondaryRack := range secondaryRacks {
				products[i].SecondaryRacks = append(products[i].SecondaryRacks, secondaryRack.Name)
			}
		}
	}

	fmt.Println("=+=+=+=")
	fmt.Printf("Страница сборки заказов %s\n", os.Args[1])
	fmt.Println()

	for rackName, listPartOrder := range racks {
		fmt.Printf("===Стеллаж %s\n", rackName)
		for _, partOrder := range listPartOrder {
			InfoProduct, err := store.GetProduct(ctx, partOrder.Product.ID)
			if err != nil {
				logrus.Fatalf(err.Error())
			}

			fmt.Printf("%s (id=%d)\n", InfoProduct.Name, InfoProduct.ID)
			fmt.Printf("заказ %d, %d шт\n", partOrder.ID, partOrder.Product.Quantity)

			if len(partOrder.Product.SecondaryRacks) > 0 {
				fmt.Printf("доп стеллаж: ")
				for _, secondaryRack := range partOrder.Product.SecondaryRacks {
					fmt.Print(secondaryRack + " ")
				}
				fmt.Println()
			}

			fmt.Println()
		}
	}
}
