package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/EconomistDigitalSolutions/gref/models"
	// Silent import for driver.
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB

var sources []string

func init() {
	sources = append(sources, "salesforce", "ezpublish", "zuora", "gluu")
}

// SQLBackend implements a backend in MySQL.
type SQLBackend struct {
	db *sqlx.DB
}

// Boot does any setup required by the back end.
func (m *SQLBackend) Boot() error {
	dbHost := os.Getenv("DB_HOST")
	dbName := os.Getenv("DB_DATABASE")
	dbUsername := os.Getenv("DB_USERNAME")
	dbPass := os.Getenv("DB_PASSWORD")
	dbPort := os.Getenv("DB_PORT")

	conn := dbUsername + ":" + dbPass + "@tcp(" + dbHost + ":" + dbPort + ")/" + dbName
	var err error
	// Create a database object - this does NOT return
	// a database connection, these are managed in an
	// internal connection pool and returned lazily when
	// we need to interact with the database.
	db, err = sqlx.Connect("mysql", conn)
	if err != nil {
		return err
	}
	// Test the connection is available.
	err = db.Ping()
	if err != nil {
		return err
	}
	return nil
}

// Ping returns the status of the back end.
func (m *SQLBackend) Ping() (string, error) {
	return m.Name(), nil
}

// Name returns the name of the back end.
func (m *SQLBackend) Name() string {
	return "mysql"
}

// Execute executes a collection of queries.
func (m *SQLBackend) Execute(queries []string) {}

// MapCreate creates a source object mapping to retrieve a canonical.
func (m *SQLBackend) MapCreate(mapping *models.Mapping) (code int, err error) {
	var canonical string
	mapCheck := fmt.Sprintf("SELECT id FROM %s WHERE %s = '%s'", mapping.ObjectType, mapping.Source, mapping.SourceID)
	err = db.QueryRow(mapCheck).Scan(&canonical)
	switch {
	case err == sql.ErrNoRows:
	case err != nil:
		log.Printf("%v", err)
		return http.StatusInternalServerError, err
	}
	if canonical != "" {
		return http.StatusConflict, errors.New("The mapping already exists")
	}
	mapInsert := fmt.Sprintf("INSERT INTO %s (id, %s) VALUES (:canonical, :source_id)", mapping.ObjectType, mapping.Source)
	_, err2 := db.NamedExec(mapInsert, mapping)
	if err2 != nil {
		log.Printf("%v", err2)
		return http.StatusInternalServerError, err2
	}
	return http.StatusCreated, nil
}

// MapUpdate adds a source identifier to an existing canonical mapping.
func (m *SQLBackend) MapUpdate(mapping *models.Mapping) (string, int, error) {
	mapUpdate := fmt.Sprintf("UPDATE %s SET %s = :source_id WHERE id = '%s'", mapping.ObjectType, mapping.Source, mapping.Canonical)
	_, err := db.NamedExec(mapUpdate, mapping)
	if err != nil {
		log.Printf("%v", err)
		return "error updating mapping", http.StatusInternalServerError, err
	}
	return "mapping successfully updated", http.StatusOK, nil
}

// SourceInfo retrieves the source object including the canonical identifier.
func (m *SQLBackend) SourceInfo(mapping *models.Mapping) (string, int, error) {
	var canonical string
	sourceInfo := fmt.Sprintf("SELECT id FROM %s WHERE %s = '%s'", mapping.ObjectType, mapping.Source, mapping.SourceID)
	_, err := db.Query(sourceInfo)
	err = db.QueryRow(sourceInfo).Scan(&canonical)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("%v", err)
		return "error retrieving canonical identifier", http.StatusNotFound, err
	case err != nil:
		log.Printf("%v", err)
		return "error retrieving canonical identifier", http.StatusInternalServerError, err
	}
	return canonical, http.StatusOK, nil
}

// CanonicalInfo retrieves a collection of mappings to this canonical resource.
func (m *SQLBackend) CanonicalInfo(mapping *models.Mapping, baseURL string) (models.Mappings, int, error) {
	var mappings models.Mappings
	for _, source := range sources {
		var sourceID string
		canonicalInfo := fmt.Sprintf("SELECT %s FROM %s WHERE id = '%s'", source, mapping.ObjectType, mapping.Canonical)
		row := db.QueryRow(canonicalInfo)
		row.Scan(&sourceID)
		if sourceID != "" {
			mappings = append(mappings, models.NewMapping(mapping.Canonical, source, sourceID, mapping.Object, mapping.ObjectType, baseURL))
		}
	}
	return mappings, http.StatusOK, nil
}
