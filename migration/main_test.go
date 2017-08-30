package migration

import (
	"log"
	"os"
	"testing"

	"github.com/blendlabs/spiffy"
)

// TestMain is the testing entrypoint.
func TestMain(m *testing.M) {
	connection := spiffy.NewFromEnv()

	err := spiffy.OpenDefault(connection)
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(m.Run())
}
