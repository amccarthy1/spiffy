package main

import (
	"log"

	"database/sql"

	"github.com/blendlabs/spiffy"
	"github.com/blendlabs/spiffy/migration"
)

func main() {
	err := spiffy.OpenDefault(spiffy.NewFromEnv())
	if err != nil {
		log.Fatal(err)
	}

	m := migration.New(
		"create & fill `test_vocab`",
		migration.Step(
			migration.TableExists("test_vocab"),
			migration.BodyStatements(
				"DROP TABLE test_vocab",
			),
		),
		migration.Step(
			migration.TableNotExists("test_vocab"),
			migration.BodyStatements(
				"CREATE TABLE test_vocab (id serial not null, word varchar(32) not null);",
				"ALTER TABLE test_vocab ADD CONSTRAINT pk_test_vocab_id PRIMARY KEY(id);",
			),
		),
		migration.New(
			"fill `test_vocab`",
			migration.ReadDataFile("data.sql"),
		),
		migration.Step(
			migration.Guard("test custom step", func(c *spiffy.Connection, tx *sql.Tx) (bool, error) {
				return c.QueryInTx("select 1 from test_vocab where word = $1", tx, "foo").None()
			}),
			migration.Body(func(c *spiffy.Connection, tx *sql.Tx) error {
				return c.ExecInTx("insert into test_vocab (word) values ($1)", tx, "foo")
			}),
		),
		migration.Step(
			migration.TableExists("test_vocab"),
			migration.BodyStatements(
				"DROP TABLE test_vocab",
			),
		),
	)
	m.SetLogger(migration.NewLogger())
	m.SetShouldAbortOnError(true)
	err = m.Apply(spiffy.Default())
	if err != nil {
		log.Fatal(err)
	}
}
