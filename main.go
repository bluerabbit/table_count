package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/semaphore"
)

func checkIDColumn(db *sql.DB, tableName string) bool {
	var dataType, columnKey string
	query := fmt.Sprintf("SELECT DATA_TYPE, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = 'id'", tableName)
	err := db.QueryRow(query).Scan(&dataType, &columnKey)
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

func getTotalCount(db *sql.DB, tableName string, start, end int) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(id) FROM %s WHERE id BETWEEN %d AND %d", tableName, start, end)
	//log.Println("Executing query:", query)
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error counting %s in range %d - %d: %v", tableName, start, end, err)
	}
	return count, nil
}
func getTotalCountParallel(db *sql.DB, tableName string, maxID, step int, concurrency int) (int, error) {
	var wg sync.WaitGroup
	results := make(chan int)

	sem := semaphore.NewWeighted(int64(concurrency))

	for i := 1; i <= maxID; i += step {
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			if err := sem.Acquire(context.Background(), 1); err != nil {
				log.Printf("Failed to acquire semaphore: %v", err)
				return
			}
			defer sem.Release(1)

			count, err := getTotalCount(db, tableName, start, end)
			if err != nil {
				log.Printf("Error counting %s in range %d - %d: %v", tableName, start, end, err)
				return
			}
			results <- count
		}(i, min(i+step-1, maxID))
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	totalCount := 0
	for result := range results {
		totalCount += result
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
	startTime := time.Now()
	if len(os.Args) < 2 {
		log.Fatal("Usage: table_count <table_name>")
	}

	tableName := os.Args[1]

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	concurrencyEnv := os.Getenv("CONCURRENCY")
	concurrency := 3 // default concurrency
	if concurrencyEnv != "" {
		var err error
		concurrency, err = strconv.Atoi(concurrencyEnv)
		if err != nil {
			log.Fatalf("Invalid CONCURRENCY: %v", err)
		}
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

	totalCount, err := getTotalCountParallel(db, tableName, maxID, step, concurrency)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total number of records in the %s table: %d\n", tableName, totalCount)
	log.Printf("Total running time: %s\n", time.Since(startTime))
}
