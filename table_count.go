package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

func checkIDColumn(db *sql.DB, tableName string) bool {
	var columnName, dataType, columnKey string
	query := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = 'id'", tableName)
	err := db.QueryRow(query).Scan(&columnName, &dataType, &columnKey)
	if err != nil {
		log.Fatalf("Failed to get column information for 'id' in table %s: %v", tableName, err)
	}
	if columnKey != "PRI" || (dataType != "int" && dataType != "bigint") {
		return false
	}
	return true
}

func getMaxID(db *sql.DB, tableName string) int {
	var maxID int
	query := fmt.Sprintf("SELECT id FROM %s ORDER BY id DESC LIMIT 1", tableName)
	err := db.QueryRow(query).Scan(&maxID)
	if err != nil {
		log.Fatalf("Failed to get max ID from %s: %v", tableName, err)
	}
	return maxID
}

func getTotalCount(db *sql.DB, tableName string, maxID, step int) (int, error) {
	totalCount := 0
	for i := 1; i <= maxID; i += step {
		var count int
		end := min(i+step-1, maxID)
		query := fmt.Sprintf("SELECT COUNT(id) FROM %s WHERE id BETWEEN %d AND %d", tableName, i, end)
		log.Println("Executing query:", query)
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("error counting %s in range %d - %d: %v", tableName, i, end, err)
		}
		totalCount += count
	}
	return totalCount, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: table_count <table_name>")
	}

	tableName := os.Args[1]

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	stepSizeEnv := os.Getenv("STEP_SIZE")
	step := 100000 // default step size
	if stepSizeEnv != "" {
		var err error
		step, err = strconv.Atoi(stepSizeEnv)
		if err != nil {
			log.Fatalf("Invalid STEP_SIZE: %v", err)
		}
	}

	db, err := sql.Open("mysql", databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if !checkIDColumn(db, tableName) {
		log.Fatalf("The 'id' column is not an integer primary key in the %s table.", tableName)
	}

	maxID := getMaxID(db, tableName)

	totalCount, err := getTotalCount(db, tableName, maxID, step)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total number of records in the %s table: %d\n", tableName, totalCount)
}
