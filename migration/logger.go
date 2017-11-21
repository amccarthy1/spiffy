package migration

import (
	"fmt"
	"strings"

	logger "github.com/blendlabs/go-logger"
)

const (
	// Event is a logger event flag.
	Event logger.Event = "db.migration"
)

// NewLogger returns a new logger instance.
func NewLogger(log *logger.Agent) *Logger {
	log.EnableEvent(Event)
	return &Logger{
		Output: log,
	}
}

// NewLoggerFromEnv returns a new logger instance.
func NewLoggerFromEnv() *Logger {
	log := logger.NewFromEnv()
	log.EnableEvent(Event)
	return &Logger{
		Output: log,
	}
}

// Logger is a logger for migration steps.
type Logger struct {
	Output *logger.Agent
	Phase  string // `test` or `apply`
	Result string // `apply` or `skipped` or `failed`

	applied int
	skipped int
	failed  int
}

// Applyf active actions to the log.
func (l *Logger) Applyf(m Migration, body string, args ...interface{}) error {
	if l == nil {
		return nil
	}

	l.applied = l.applied + 1
	l.Result = "applied"
	l.write(m, logger.ColorLightGreen, fmt.Sprintf(body, args...))
	return nil
}

// Skipf passive actions to the log.
func (l *Logger) Skipf(m Migration, body string, args ...interface{}) error {
	if l == nil {
		return nil
	}
	l.skipped = l.skipped + 1
	l.Result = "skipped"
	l.write(m, logger.ColorGreen, fmt.Sprintf(body, args...))
	return nil
}

// Errorf writes errors to the log.
func (l *Logger) Error(m Migration, err error) error {
	if l == nil {
		return err
	}
	l.failed = l.failed + 1
	l.Result = "failed"
	l.write(m, logger.ColorRed, fmt.Sprintf("%v", err.Error()))
	return err
}

// WriteStats writes final stats to output
func (l *Logger) WriteStats() {
	l.Output.Sync().WriteEventf(
		Event,
		logger.ColorWhite,
		"%s applied %s skipped %s failed",
		l.colorize(fmt.Sprintf("%d", l.applied), logger.ColorGreen),
		l.colorize(fmt.Sprintf("%d", l.skipped), logger.ColorLightGreen),
		l.colorize(fmt.Sprintf("%d", l.failed), logger.ColorRed),
	)
}

func (l *Logger) colorize(text string, color logger.AnsiColorCode) string {
	return l.Output.Writer().Colorize(text, color)
}

func (l *Logger) colorizeFixedWidthLeftAligned(text string, color logger.AnsiColorCode, width int) string {
	fixedToken := fmt.Sprintf("%%-%ds", width)
	fixedMessage := fmt.Sprintf(fixedToken, text)
	return l.Output.Writer().Colorize(fixedMessage, color)
}

func (l *Logger) write(m Migration, color logger.AnsiColorCode, body string) {
	if l.Output == nil {
		return
	}

	resultColor := logger.ColorBlue
	switch l.Result {
	case "skipped":
		resultColor = logger.ColorYellow
	case "failed":
		resultColor = logger.ColorRed
	}

	buffer := l.Output.Writer().GetBuffer()
	defer l.Output.Writer().PutBuffer(buffer)

	buffer.WriteString(l.colorizeFixedWidthLeftAligned(l.Phase, logger.ColorBlue, 5))
	buffer.WriteRune(logger.RuneSpace)
	buffer.WriteString(l.colorize("--", logger.ColorLightBlack))
	buffer.WriteRune(logger.RuneSpace)
	buffer.WriteString(l.colorizeFixedWidthLeftAligned(l.Result, resultColor, 5))

	if stack := l.renderStack(m, color); len(stack) > 0 {
		buffer.WriteRune(logger.RuneSpace)
		buffer.WriteString(stack)
	}
	if len(body) > 0 {
		buffer.WriteRune(logger.RuneSpace)
		buffer.WriteString(l.colorize("--", logger.ColorLightBlack))
		buffer.WriteRune(logger.RuneSpace)
		buffer.WriteString(body)
	}

	l.Output.Sync().WriteEventf(
		Event,
		logger.ColorWhite,
		buffer.String(),
	)
}

func (l *Logger) renderStack(m Migration, color logger.AnsiColorCode) string {
	stackSeparator := fmt.Sprintf(" %s ", l.colorize(">", logger.ColorLightBlack))
	var renderedStack string
	cursor := m.Parent()
	for cursor != nil {
		if len(cursor.Label()) > 0 {
			renderedStack = stackSeparator + cursor.Label() + renderedStack
		}
		cursor = cursor.Parent()
	}
	return strings.TrimPrefix(renderedStack, " ")
}