// main_test.go
package main

import (
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestCheckIDColumn(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Could not connect to the database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50));")
	if err != nil {
		t.Fatalf("Could not create test table: %v", err)
	}

	if !checkIDColumn(db, "users") {
		t.Errorf("checkIDColumn should return true for test_table's id column")
	}

	_, err = db.Exec("DROP TABLE users;")
	if err != nil {
		t.Fatalf("Could not drop test table: %v", err)
	}
}

func TestCheckIDColumnInvalid(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Could not connect to the database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test_table_invalid (id VARCHAR(50) PRIMARY KEY, name VARCHAR(50));")
	if err != nil {
		t.Fatalf("Could not create test table with invalid id column: %v", err)
	}

	if checkIDColumn(db, "test_table_invalid") {
		t.Errorf("checkIDColumn should return false for test_table_invalid's id column")
	}

	_, err = db.Exec("DROP TABLE test_table_invalid;")
	if err != nil {
		t.Fatalf("Could not drop test table with invalid id column: %v", err)
	}
}

func TestGetMaxID(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Could not connect to the database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50));")
	if err != nil {
		t.Fatalf("Could not create test table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie');")
	if err != nil {
		t.Fatalf("Could not insert test data: %v", err)
	}

	expectedMaxID := 3
	maxID := getMaxID(db, "users")
	if maxID != expectedMaxID {
		t.Errorf("Expected max ID of %d, but got %d", expectedMaxID, maxID)
	}

	_, err = db.Exec("DROP TABLE users;")
	if err != nil {
		t.Fatalf("Could not drop test table: %v", err)
	}
}

func TestGetTotalCountParallel(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Could not connect to the database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50));")
	if err != nil {
		t.Fatalf("Could not create test table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie');")
	if err != nil {
		t.Fatalf("Could not insert test data: %v", err)
	}

	step := 2
	expectedTotalCount := 3
	totalCount, err := getTotalCountParallel(db, "users", 3, step, "", 2)
	if err != nil {
		t.Fatalf("getTotalCount failed: %v", err)
	}
	if totalCount != expectedTotalCount {
		t.Errorf("Expected total count of %d, but got %d", expectedTotalCount, totalCount)
	}

	_, err = db.Exec("DROP TABLE users;")
	if err != nil {
		t.Fatalf("Could not drop test table: %v", err)
	}
}
