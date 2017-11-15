package main

import (
	"log"

	"database/sql"

	"github.com/blendlabs/spiffy"
	"github.com/blendlabs/spiffy/migration"
)

func main() {
	db, err := spiffy.NewFromEnv().Open()
	if err != nil {
		log.Fatal(err)
	}

	err = migration.NewGroup(
		migration.NewStep(
			migration.TableExists("test_vocab"),
			migration.Statements(
				"DROP TABLE test_vocab",
			),
		),
		migration.NewStep(
			migration.TableNotExists("test_vocab"),
			migration.Statements(
				"CREATE TABLE test_vocab (id serial not null, word varchar(32) not null);",
				"ALTER TABLE test_vocab ADD CONSTRAINT pk_test_vocab_id PRIMARY KEY(id);",
			),
		),
		migration.ReadDataFile("data.sql"),
		migration.NewStep(
			migration.DynamicGuard("test custom step", func(c *spiffy.Connection, tx *sql.Tx) (bool, error) {
				return c.QueryInTx("select 1 from test_vocab where word = $1", tx, "foo").None()
			}),
			migration.Body(func(c *spiffy.Connection, tx *sql.Tx) error {
				return c.ExecInTx("insert into test_vocab (word) values ($1)", tx, "foo")
			}),
		),
		migration.NewStep(
			migration.TableExists("test_vocab"),
			migration.Statements(
				"DROP TABLE test_vocab",
			),
		),
	).WithShouldAbortOnError(true).WithLogger(migration.NewLoggerFromEnv()).Apply(db)
	if err != nil {
		log.Fatal(err)
	}
}
