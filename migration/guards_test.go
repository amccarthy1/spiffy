package migration

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/blendlabs/go-assert"
	"github.com/blendlabs/spiffy"
)

var (
	// lowerLetters is a runset of lowercase letters.
	lowerLetters = []rune("abcdefghijklmnopqrstuvwxyz")
)

func randomName() string {
	runes := make([]rune, 12)
	letterCount := len(lowerLetters)
	for index := range runes {
		runes[index] = lowerLetters[rand.Intn(letterCount)]
	}
	return string(runes)
}

func createTestTable(tableName string, tx *sql.Tx) error {
	body := fmt.Sprintf("CREATE TABLE %s (id int, name varchar(32));", tableName)
	step := Step(TableNotExists(tableName), BodyStatements(body))
	return step.Apply(spiffy.Default(), tx)
}

func insertTestValue(tableName string, id int, name string, tx *sql.Tx) error {
	body := fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1, $2);", tableName)
	return spiffy.Default().ExecInTx(body, tx, id, name)
}

func createTestColumn(tableName, columnName string, tx *sql.Tx) error {
	body := fmt.Sprintf("ALTER TABLE %s ADD %s varchar(32);", tableName, columnName)
	step := Step(ColumnNotExists(tableName, columnName), BodyStatements(body))
	return step.Apply(spiffy.Default(), tx)
}

func createTestConstraint(tableName, constraintName string, tx *sql.Tx) error {
	body := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (name);", tableName, constraintName)
	step := Step(ColumnNotExists(tableName, constraintName), BodyStatements(body))
	return step.Apply(spiffy.Default(), tx)
}

func createTestIndex(tableName, indexName string, tx *sql.Tx) error {
	body := fmt.Sprintf("CREATE INDEX %s ON %s (name);", indexName, tableName)
	step := Step(IndexNotExists(tableName, indexName), BodyStatements(body))
	return step.Apply(spiffy.Default(), tx)
}

func createTestRole(roleName string, tx *sql.Tx) error {
	body := fmt.Sprintf("CREATE ROLE %s;", roleName)
	step := Step(RoleNotExists(roleName), BodyStatements(body))
	return step.Apply(spiffy.Default(), tx)
}

func TestCreateTable(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.Default().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := randomName()
	err = createTestTable(tableName, nil)
	assert.Nil(err)

	exists, err := tableExists(spiffy.Default(), nil, tableName)
	assert.Nil(err)
	assert.True(exists, "table does not exist")
}

func TestCreateColumn(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.Default().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := randomName()
	err = createTestTable(tableName, tx)
	assert.Nil(err)

	columnName := randomName()
	err = createTestColumn(tableName, columnName, tx)
	assert.Nil(err)

	exists, err := columnExists(spiffy.Default(), tx, tableName, columnName)
	assert.Nil(err)
	assert.True(exists, "column does not exist on table")
}

func TestCreateConstraint(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.Default().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := randomName()
	err = createTestTable(tableName, tx)
	assert.Nil(err)

	constraintName := fmt.Sprintf("uk_%s_%s", tableName, randomName())
	err = createTestConstraint(tableName, constraintName, tx)
	assert.Nil(err)

	exists, err := constraintExists(spiffy.Default(), tx, constraintName)
	assert.Nil(err)
	assert.True(exists, "constraint does not exist")
}

func TestCreateIndex(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.Default().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := randomName()
	err = createTestTable(tableName, tx)
	assert.Nil(err)

	indexName := fmt.Sprintf("ix_%s_%s", tableName, randomName())
	err = createTestIndex(tableName, indexName, tx)
	assert.Nil(err)

	exists, err := indexExists(spiffy.Default(), tx, tableName, indexName)
	assert.Nil(err)
	assert.True(exists, "constraint does not exist")
}

func TestCreateRole(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.Default().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	roleName := randomName()
	err = createTestRole(roleName, tx)
	assert.Nil(err)

	exists, err := roleExists(spiffy.Default(), tx, roleName)
	assert.Nil(err)
	assert.True(exists, "role does not exist")
}

func TestNotExists(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.Default().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := randomName()
	err = createTestTable(tableName, tx)
	assert.Nil(err)

	err = insertTestValue(tableName, 4, "test", tx)
	assert.Nil(err)

	ne, err := notExists(spiffy.Default(), tx, fmt.Sprintf(`select * from %s where id = %d`, tableName, 4))
	assert.Nil(err)
	assert.False(ne)

	ne, err = notExists(spiffy.Default(), tx, fmt.Sprintf(`select * from %s where id = %d`, tableName, 101))
	assert.Nil(err)
	assert.True(ne)
}

func TestGuard(t *testing.T) {
	assert := assert.New(t)
	tx, err := spiffy.Default().Begin()
	assert.Nil(err)
	defer tx.Rollback()

	tableName := randomName()
	err = createTestTable(tableName, tx)
	assert.Nil(err)

	err = insertTestValue(tableName, 4, "test", tx)
	assert.Nil(err)

	var didRun bool
	action := Body(func(c *spiffy.Connection, itx *sql.Tx) error {
		didRun = true
		return nil
	})

	err = Guard("test", func(c *spiffy.Connection, itx *sql.Tx) (bool, error) {
		return c.QueryInTx(fmt.Sprintf("select * from %s", tableName), itx).Any()
	})(
		&Operation{body: action},
		spiffy.Default(),
		tx,
	)
	assert.Nil(err)
	assert.True(didRun)
}
