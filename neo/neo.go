package neo

import (
	"errors"
	"fmt"
	"net/http"
	"os"

	"database/sql"
	"log"

	"github.com/EconomistDigitalSolutions/gref/models"

	// cq package imported as driver.
	_ "github.com/go-cq/cq"
)

var (
	db     *sql.DB
	dbHost string
	dbPort string
	dbUser string
	dbPass string
)

// If the neo database user and password are not specified, we can still connect if
// dbms.security.auth_enabled=false is set in neo4j-server.properties
func init() {
	dbHost = os.Getenv("GREF_NEO_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}
	dbPort = os.Getenv("GREF_NEO_PORT")
	if dbPort == "" {
		dbPort = "7474"
	}
	dbUser = os.Getenv("GREF_NEO_USER")
	if dbUser == "" {
		dbUser = "neo4j"
	}
	dbPass = os.Getenv("GREF_NEO_PASS")
	if dbPass == "" {
		dbPass = "neo4j"
	}
}

// NeoBackend implements a backend in MySQL.
type NeoBackend struct{}

// Boot does any setup required by the back end.
func (n *NeoBackend) Boot() (err error) {
	db, err = sql.Open("neo4j-cypher", fmt.Sprintf("http://%s:%s@%s:%s", dbUser, dbPass, dbHost, dbPort))
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

// Ping returns the status of the back end.
func (n *NeoBackend) Ping() (string, error) {
	return n.Name(), nil
}

// Name returns the name of the back end.
func (n *NeoBackend) Name() string {
	return "neo4j"
}

// Execute executes a collection of queries.
func (n *NeoBackend) Execute(queries []string) {
	graphExec(queries)
}

// MapCreate creates a source object mapping to retrieve a canonical.
func (n *NeoBackend) MapCreate(mapping *models.Mapping) (code int, err error) {

	// Create the source.
	q := fmt.Sprintf(`merge (o:%s)-[r:KNOWS {id : '%s'}]->(s:source {name:'%s'}) on create set o.canonical='%s' return o.canonical`, mapping.ObjectType, mapping.SourceID, mapping.Source, mapping.Canonical)

	vals, err := graphExec([]string{q})
	if err != nil {
		return http.StatusInternalServerError, err
	}
	v := vals[0]["o.canonical"]
	if v != mapping.Canonical {
		log.Println("POST with ", mapping.Canonical, " but resource already exists with canonical:", v)
		mapping.Canonical = v
		return http.StatusConflict, nil
	}
	return http.StatusCreated, nil
}

// MapUpdate adds a source identifier to an existing canonical mapping.
func (n *NeoBackend) MapUpdate(mapping *models.Mapping) (string, int, error) {

	q := fmt.Sprintf(`merge (o:%s {canonical:'%s'}) merge (s:source {name:'%s'}) merge (o)-[r:KNOWS {id : '%s'}]->(s)`, mapping.ObjectType, mapping.Canonical, mapping.Source, mapping.SourceID)

	_, err := graphExec([]string{q})
	if err != nil {
		return "", http.StatusInternalServerError, err
	}
	return "", http.StatusNoContent, nil
}

// SourceInfo retrieves the source object including the canonical identifier.
func (n *NeoBackend) SourceInfo(mapping *models.Mapping) (string, int, error) {

	q := fmt.Sprintf(`match (o:%s)-[r:KNOWS {id:'%s'}]->(s:source {name:'%s'}) return o.canonical`, mapping.ObjectType, mapping.SourceID, mapping.Source)
	vals, err := graphExec([]string{q})
	if err != nil {
		return "", http.StatusInternalServerError, err
	}
	if len(vals) == 0 {
		return "resource not found", http.StatusNotFound, errors.New("resource not found")
	}
	v := vals[0]["o.canonical"]
	mapping.Canonical = v
	mapping.Object += "/" + v

	return mapping.Canonical, http.StatusOK, nil
}

// CanonicalInfo retrieves a collection of mappings to this canonical resource.
func (n *NeoBackend) CanonicalInfo(mapping *models.Mapping, baseURL string) (models.Mappings, int, error) {

	q := fmt.Sprintf(`match (c:%s {canonical:'%s'})-[i:KNOWS]->(s:source) return c.canonical, s.name, i.id`, mapping.ObjectType, mapping.Canonical)
	var mappings models.Mappings

	vals, err := graphExec([]string{q})
	if err != nil {
		return mappings, http.StatusInternalServerError, err
	}

	mappings = make(models.Mappings, (len(vals)))
	for i := range vals {
		mappings[i].SourceID = vals[i]["i.id"]
		mappings[i].Canonical = vals[i]["c.canonical"]
		mappings[i].Source = vals[i]["s.name"]
		mappings[i].ObjectType = mapping.ObjectType
		mappings[i].Object = mappings[i].ObjectURL(baseURL)
	}

	return mappings, http.StatusOK, nil
}

func graphExec(queries []string) ([]map[string]string, error) {
	var vals []map[string]string

	for _, query := range queries {
		smt, err := db.Prepare(query)
		if err != nil {
			log.Printf("%v", err)
			return nil, err
		}
		defer smt.Close()

		rows, err := smt.Query()
		if err != nil {
			log.Printf("%v", err)
			return nil, err
		}
		defer rows.Close()

		vals = packageData(rows)
	}
	return vals, nil
}

// Make a sql rows thing into a slice of maps to strings so we can do something
// sensible without knowing the number and types of the columns returned
func packageData(rows *sql.Rows) []map[string]string {
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println("Unable to read from database", err)
		log.Println(err)
		return nil
	}
	if len(columns) == 0 {
		return nil
	}
	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var data []map[string]string

	// Fetch rows
	for rows.Next() {
		newRow := make(map[string]string)
		// get RawBytes from data
		err := rows.Scan(scanArgs...)
		if err != nil {
			fmt.Println("Unable to read from database", err)
			log.Println(err)
			return nil
		}
		var value string
		for i, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			newRow[columns[i]] = value
		}
		data = append(data, newRow)
	}
	return data
}
