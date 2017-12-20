// Package spiffy providers a basic abstraction layer above normal database/sql that makes it easier to
// interact with the database and organize database related code. It is not intended to replace actual sql
// (you write queries yourself).
package spiffy

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/blendlabs/go-exception"
	logger "github.com/blendlabs/go-logger"

	// PQ is the postgres driver
	_ "github.com/lib/pq"
)

const (
	//DBNilError is a common error
	DBNilError = "connection is nil"
)

const (
	runeComma   = rune(',')
	runeNewline = rune('\n')
	runeTab     = rune('\t')
	runeSpace   = rune(' ')
)

// --------------------------------------------------------------------------------
// Connection
// --------------------------------------------------------------------------------

// New returns a new Connection.
func New() *Connection {
	return &Connection{
		bufferPool:         NewBufferPool(1024),
		useStatementCache:  false, //doesnt actually help perf, maybe someday.
		statementCacheLock: &sync.Mutex{},
		connectionLock:     &sync.Mutex{},
	}
}

// NewWithHost creates a new Connection using current user peer authentication.
func NewWithHost(host, dbName string) *Connection {
	dbc := New()
	dbc.Host = host
	dbc.Database = dbName
	dbc.SSLMode = "disable"
	return dbc
}

// NewWithPassword creates a new connection with SSLMode set to "disable"
func NewWithPassword(host, dbName, username, password string) *Connection {
	dbc := New()
	dbc.Host = host
	dbc.Database = dbName
	dbc.Username = username
	dbc.Password = password
	dbc.SSLMode = "disable"
	return dbc
}

// NewWithSSLMode creates a new connection with all available options (including SSLMode)
func NewWithSSLMode(host, dbName, username, password, sslMode string) *Connection {
	dbc := New()
	dbc.Host = host
	dbc.Database = dbName
	dbc.Username = username
	dbc.Password = password
	dbc.SSLMode = sslMode
	return dbc
}

// NewFromDSN creates a new connection with SSLMode set to "disable"
func NewFromDSN(dsn string) *Connection {
	dbc := New()
	dbc.DSN = dsn
	return dbc
}

func envVarWithDefault(varName, defaultValue string) string {
	envVarValue := os.Getenv(varName)
	if len(envVarValue) > 0 {
		return envVarValue
	}
	return defaultValue
}

// NewFromEnv creates a new db connection from environment variables.
//
// The environment variable mappings are as follows:
//	-	DATABSE_URL 	= DSN 	//note that this trumps other vars (!!)
// 	-	DB_HOST 		= Host
//	-	DB_PORT 		= Port
//	- 	DB_NAME 		= Database
//	-	DB_SCHEMA		= Schema
//	-	DB_USER 		= Username
//	-	DB_PASSWORD 	= Password
//	-	DB_SSLMODE 		= SSLMode
func NewFromEnv() *Connection {
	if len(os.Getenv("DATABASE_URL")) > 0 {
		return NewFromDSN(os.Getenv("DATABASE_URL"))
	}

	dbc := New()
	dbc.Host = envVarWithDefault("DB_HOST", "localhost")
	dbc.Database = envVarWithDefault("DB_NAME", "postgres")
	dbc.Schema = os.Getenv("DB_SCHEMA")
	dbc.Username = os.Getenv("DB_USER")
	dbc.Password = os.Getenv("DB_PASSWORD")
	dbc.SSLMode = envVarWithDefault("DB_SSLMODE", "disable")
	return dbc
}

// Connection is the basic wrapper for connection parameters and saves a reference to the created sql.Connection.
type Connection struct {
	// DSN is a fully formed DSN (this skips DSN formation from other variables).
	DSN string

	// Host is the server to connect to.
	Host string
	// Port is the port to connect to.
	Port string
	// DBName is the database name
	Database string
	// Schema is the application schema within the database, defaults to `public`.
	Schema string
	// Username is the username for the connection via password auth.
	Username string
	// Password is the password for the connection via password auth.
	Password string
	// SSLMode is the sslmode for the connection.
	SSLMode string

	// Connection is the underlying sql driver connection for the Connection.
	Connection *sql.DB

	connectionLock     *sync.Mutex
	statementCacheLock *sync.Mutex

	bufferPool *BufferPool
	log        *logger.Logger

	useStatementCache bool
	statementCache    *StatementCache
}

// Close implements a closer.
func (dbc *Connection) Close() error {
	var err error
	if dbc.statementCache != nil {
		err = dbc.statementCache.Close()
	}
	if err != nil {
		return err
	}
	return dbc.Connection.Close()
}

// WithLogger sets the connection's diagnostic agent.
func (dbc *Connection) WithLogger(log *logger.Logger) {
	dbc.log = log
}

// Logger returns the diagnostics agent.
func (dbc *Connection) Logger() *logger.Logger {
	return dbc.log
}

func (dbc *Connection) fireEvent(flag logger.Flag, query string, elapsed time.Duration, err error, optionalQueryLabel ...string) {
	if dbc.log != nil {
		var queryLabel string
		if len(optionalQueryLabel) > 0 {
			queryLabel = optionalQueryLabel[0]
		}

		dbc.log.Trigger(NewEvent(flag, queryLabel, query, elapsed, err))
		dbc.log.Trigger(NewStatementEvent(flag, queryLabel, query, elapsed, err))
	}
}

// EnableStatementCache opts to cache statements for the connection.
func (dbc *Connection) EnableStatementCache() {
	dbc.useStatementCache = true
}

// DisableStatementCache opts to not use the statement cache.
func (dbc *Connection) DisableStatementCache() {
	dbc.useStatementCache = false
}

// StatementCache returns the statement cache.
func (dbc *Connection) StatementCache() *StatementCache {
	return dbc.statementCache
}

// CreatePostgresConnectionString returns a sql connection string from a given set of Connection parameters.
func (dbc *Connection) CreatePostgresConnectionString() (string, error) {
	if len(dbc.DSN) != 0 {
		return dbc.DSN, nil
	}

	if len(dbc.Database) == 0 {
		return "", exception.New("`DB_NAME` is required to open a new connection")
	}

	sslMode := "?sslmode=disable"
	if len(dbc.SSLMode) > 0 {
		sslMode = fmt.Sprintf("?sslmode=%s", url.QueryEscape(dbc.SSLMode))
	}

	var portSegment string
	if len(dbc.Port) > 0 {
		portSegment = fmt.Sprintf(":%s", dbc.Port)
	}

	if dbc.Username != "" {
		if dbc.Password != "" {
			return fmt.Sprintf("postgres://%s:%s@%s%s/%s%s", url.QueryEscape(dbc.Username), url.QueryEscape(dbc.Password), dbc.Host, portSegment, dbc.Database, sslMode), nil
		}
		return fmt.Sprintf("postgres://%s@%s%s/%s%s", url.QueryEscape(dbc.Username), dbc.Host, portSegment, dbc.Database, sslMode), nil
	}
	return fmt.Sprintf("postgres://%s%s/%s%s", dbc.Host, portSegment, dbc.Database, sslMode), nil
}

// openNewSQLConnection returns a new connection object.
func (dbc *Connection) openNewSQLConnection() (*sql.DB, error) {
	connStr, err := dbc.CreatePostgresConnectionString()
	if err != nil {
		return nil, err
	}

	dbConn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, exception.Wrap(err)
	}

	if len(dbc.Schema) > 0 {
		_, err = dbConn.Exec(fmt.Sprintf("SET search_path TO %s,public;", dbc.Schema))
		if err != nil {
			return nil, exception.Wrap(err)
		}
	}

	_, err = dbConn.Exec("select 'ok!'")
	if err != nil {
		return nil, exception.Wrap(err)
	}

	return dbConn, nil
}

// Open returns a connection object, either a cached connection object or creating a new one in the process.
func (dbc *Connection) Open() (*Connection, error) {
	if dbc.Connection == nil {
		dbc.connectionLock.Lock()
		defer dbc.connectionLock.Unlock()

		if dbc.Connection == nil {
			newConn, err := dbc.openNewSQLConnection()
			if err != nil {
				return nil, exception.Wrap(err)
			}
			dbc.Connection = newConn
		}
	}
	return dbc, nil
}

// Begin starts a new transaction.
func (dbc *Connection) Begin() (*sql.Tx, error) {
	if dbc.Connection != nil {
		tx, txErr := dbc.Connection.Begin()
		return tx, exception.Wrap(txErr)
	}

	connection, err := dbc.Open()
	if err != nil {
		return nil, exception.Wrap(err)
	}
	tx, err := connection.Begin()
	return tx, exception.Wrap(err)
}

// Prepare prepares a new statement for the connection.
func (dbc *Connection) Prepare(statement string, tx *sql.Tx) (*sql.Stmt, error) {
	if tx != nil {
		stmt, err := tx.Prepare(statement)
		if err != nil {
			return nil, exception.Wrap(err)
		}
		return stmt, nil
	}

	// open shared connection
	dbConn, err := dbc.Open()
	if err != nil {
		return nil, exception.Wrap(err)
	}

	stmt, err := dbConn.Connection.Prepare(statement)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	return stmt, nil
}

func (dbc *Connection) ensureStatementCache() error {
	if dbc.statementCache == nil {
		dbc.statementCacheLock.Lock()
		defer dbc.statementCacheLock.Unlock()
		if dbc.statementCache == nil {
			db, err := dbc.Open()
			if err != nil {
				return exception.Wrap(err)
			}
			dbc.statementCache = newStatementCache(db.Connection)
		}
	}
	return nil
}

// PrepareCached prepares a potentially cached statement.
func (dbc *Connection) PrepareCached(id, statement string, tx *sql.Tx) (*sql.Stmt, error) {
	if tx != nil {
		stmt, err := tx.Prepare(statement)
		if err != nil {
			return nil, exception.Wrap(err)
		}
		return stmt, nil
	}

	if dbc.useStatementCache {
		dbc.ensureStatementCache()
		return dbc.statementCache.Prepare(id, statement)
	}
	return dbc.Prepare(statement, tx)
}

// --------------------------------------------------------------------------------
// Invocation context
// --------------------------------------------------------------------------------

// DB returns a new db context.
func (dbc *Connection) DB(txs ...*sql.Tx) *DB {
	return &DB{
		conn:       dbc,
		tx:         OptionalTx(txs...),
		fireEvents: dbc.log != nil,
	}
}

// Invoke returns a new invocation.
func (dbc *Connection) Invoke(txs ...*sql.Tx) *Invocation {
	return &Invocation{
		conn:       dbc,
		tx:         OptionalTx(txs...),
		fireEvents: dbc.log != nil,
	}
}

// InTx is an alias to Invoke.
func (dbc *Connection) InTx(txs ...*sql.Tx) *Invocation {
	return dbc.Invoke(txs...)
}

// --------------------------------------------------------------------------------
// Invocation Context Stubs
// --------------------------------------------------------------------------------

// Exec runs the statement without creating a QueryResult.
func (dbc *Connection) Exec(statement string, args ...interface{}) error {
	return dbc.ExecInTx(statement, nil, args...)
}

// ExecWithCacheLabel runs the statement without creating a QueryResult.
func (dbc *Connection) ExecWithCacheLabel(statement, cacheLabel string, args ...interface{}) error {
	return dbc.ExecInTxWithCacheLabel(statement, cacheLabel, nil, args...)
}

// ExecInTx runs a statement within a transaction.
func (dbc *Connection) ExecInTx(statement string, tx *sql.Tx, args ...interface{}) (err error) {
	return dbc.ExecInTxWithCacheLabel(statement, statement, tx, args...)
}

// ExecInTxWithCacheLabel runs a statement within a transaction.
func (dbc *Connection) ExecInTxWithCacheLabel(statement, cacheLabel string, tx *sql.Tx, args ...interface{}) (err error) {
	return dbc.Invoke(tx).WithLabel(cacheLabel).Exec(statement, args...)
}

// Query runs the selected statement and returns a Query.
func (dbc *Connection) Query(statement string, args ...interface{}) *Query {
	return dbc.QueryInTx(statement, nil, args...)
}

// QueryInTx runs the selected statement in a transaction and returns a Query.
func (dbc *Connection) QueryInTx(statement string, tx *sql.Tx, args ...interface{}) (result *Query) {
	return dbc.Invoke(tx).Query(statement, args...)
}

// Get returns a given object based on a group of primary key ids.
func (dbc *Connection) Get(object DatabaseMapped, ids ...interface{}) error {
	return dbc.GetInTx(object, nil, ids...)
}

// GetInTx returns a given object based on a group of primary key ids within a transaction.
func (dbc *Connection) GetInTx(object DatabaseMapped, tx *sql.Tx, args ...interface{}) error {
	return dbc.Invoke(tx).Get(object, args...)
}

// GetAll returns all rows of an object mapped table.
func (dbc *Connection) GetAll(collection interface{}) error {
	return dbc.GetAllInTx(collection, nil)
}

// GetAllInTx returns all rows of an object mapped table wrapped in a transaction.
func (dbc *Connection) GetAllInTx(collection interface{}, tx *sql.Tx) error {
	return dbc.Invoke(tx).GetAll(collection)
}

// Create writes an object to the database.
func (dbc *Connection) Create(object DatabaseMapped) error {
	return dbc.CreateInTx(object, nil)
}

// CreateInTx writes an object to the database within a transaction.
func (dbc *Connection) CreateInTx(object DatabaseMapped, tx *sql.Tx) (err error) {
	return dbc.Invoke(tx).Create(object)
}

// CreateIfNotExists writes an object to the database if it does not already exist.
func (dbc *Connection) CreateIfNotExists(object DatabaseMapped) error {
	return dbc.CreateIfNotExistsInTx(object, nil)
}

// CreateIfNotExistsInTx writes an object to the database if it does not already exist within a transaction.
func (dbc *Connection) CreateIfNotExistsInTx(object DatabaseMapped, tx *sql.Tx) (err error) {
	return dbc.Invoke(tx).CreateIfNotExists(object)
}

// CreateMany writes many an objects to the database.
func (dbc *Connection) CreateMany(objects interface{}) error {
	return dbc.CreateManyInTx(objects, nil)
}

// CreateManyInTx writes many an objects to the database within a transaction.
func (dbc *Connection) CreateManyInTx(objects interface{}, tx *sql.Tx) (err error) {
	return dbc.Invoke(tx).CreateMany(objects)
}

// Update updates an object.
func (dbc *Connection) Update(object DatabaseMapped) error {
	return dbc.UpdateInTx(object, nil)
}

// UpdateInTx updates an object wrapped in a transaction.
func (dbc *Connection) UpdateInTx(object DatabaseMapped, tx *sql.Tx) (err error) {
	return dbc.Invoke(tx).Update(object)
}

// Exists returns a bool if a given object exists (utilizing the primary key columns if they exist).
func (dbc *Connection) Exists(object DatabaseMapped) (bool, error) {
	return dbc.ExistsInTx(object, nil)
}

// ExistsInTx returns a bool if a given object exists (utilizing the primary key columns if they exist) wrapped in a transaction.
func (dbc *Connection) ExistsInTx(object DatabaseMapped, tx *sql.Tx) (exists bool, err error) {
	return dbc.Invoke(tx).Exists(object)
}

// Delete deletes an object from the database.
func (dbc *Connection) Delete(object DatabaseMapped) error {
	return dbc.DeleteInTx(object, nil)
}

// DeleteInTx deletes an object from the database wrapped in a transaction.
func (dbc *Connection) DeleteInTx(object DatabaseMapped, tx *sql.Tx) (err error) {
	return dbc.Invoke(tx).Delete(object)
}

// Upsert inserts the object if it doesn't exist already (as defined by its primary keys) or updates it.
func (dbc *Connection) Upsert(object DatabaseMapped) error {
	return dbc.UpsertInTx(object, nil)
}

// UpsertInTx inserts the object if it doesn't exist already (as defined by its primary keys) or updates it wrapped in a transaction.
func (dbc *Connection) UpsertInTx(object DatabaseMapped, tx *sql.Tx) (err error) {
	return dbc.Invoke(tx).Upsert(object)
}

// Truncate fully removes an tables rows in a single opertation.
func (dbc *Connection) Truncate(object DatabaseMapped) error {
	return dbc.TruncateInTx(object, nil)
}

// TruncateInTx applies a truncation in a transaction.
func (dbc *Connection) TruncateInTx(object DatabaseMapped, tx *sql.Tx) error {
	return dbc.Invoke(tx).Truncate(object)
}
