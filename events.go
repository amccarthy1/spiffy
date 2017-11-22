package spiffy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	logger "github.com/blendlabs/go-logger"
)

const (
	// FlagExecute is a logger.EventFlag
	FlagExecute logger.Flag = "db.execute"

	// FlagQuery is a logger.EventFlag
	FlagQuery logger.Flag = "db.query"
)

// NewEvent creates a new logger event.
func NewEvent(flag logger.Flag, label, query string, elapsed time.Duration, err error) Event {
	return Event{
		flag:       flag,
		ts:         time.Now().UTC(),
		queryLabel: label,
		queryBody:  query,
		elapsed:    elapsed,
		err:        err,
	}
}

// NewEventListener returns a new listener for spiffy events.
func NewEventListener(listener func(wr logger.Writer, e Event)) logger.Listener {
	return func(wr logger.Writer, e logger.Event) {
		if typed, isTyped := e.(Event); isTyped {
			listener(wr, typed)
		}
	}
}

// Event is the event we trigger the logger with.
type Event struct {
	flag       logger.Flag
	ts         time.Time
	queryLabel string
	queryBody  string
	elapsed    time.Duration
	err        error
}

// Flag returns the event flag.
func (e Event) Flag() logger.Flag {
	return e.flag
}

// Timestamp returns the event timestamp.
func (e Event) Timestamp() time.Time {
	return e.ts
}

// WriteText writes the event text to the output.
func (e Event) WriteText(tf logger.TextFormatter, buf *bytes.Buffer) error {
	buf.WriteString(fmt.Sprintf("(%v) ", e.elapsed))
	if len(e.queryLabel) > 0 {
		buf.WriteString(e.queryLabel)
	}
	buf.WriteRune(logger.RuneNewline)
	buf.WriteString(e.queryBody)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"flag":       e.flag,
		"ts":         e.ts,
		"queryLabel": e.queryLabel,
		"queryBody":  e.queryBody,
		"elapsed":    logger.Milliseconds(e.elapsed),
	})
}
