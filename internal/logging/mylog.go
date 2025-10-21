package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

type LoggerConfig struct {
	LogLevel string `help:"Log level" default:"info"`
}

type Logger struct {
	zerolog.Logger
}

// NewLogger creates a new Logger with the given configuration
func NewLogger(cfg LoggerConfig) (*Logger, error) {
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		return nil, err
	}

	// Configure the output to use ConsoleWriter for human-readable logs
	output := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
	}

	zl := zerolog.New(output).
		Level(level).
		With().
		Timestamp().
		Logger()

	return &Logger{Logger: zl}, nil
}

// ModuleLogger creates a sub-logger with the given module name
func (l *Logger) ModuleLogger(module string) *Logger {
	subLogger := l.With().
		Str("module", module).
		Logger()
	return &Logger{Logger: subLogger}
}
