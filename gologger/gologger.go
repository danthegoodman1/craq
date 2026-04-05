package gologger

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

var configureGlobalsOnce sync.Once

func init() {
	l := NewLogger()
	zerolog.DefaultContextLogger = &l
}

// Makes context.Canceled errors a warn (for when people abandon requests)
func LvlForErr(err error) zerolog.Level {
	if errors.Is(err, context.Canceled) {
		return zerolog.WarnLevel
	}
	return zerolog.ErrorLevel
}

func NewLogger() zerolog.Logger {
	configureLoggerGlobals()

	output := io.Writer(os.Stdout)
	if logsDisabledForTests() {
		output = io.Discard
	}

	logger := zerolog.New(output).With().Timestamp().Logger()

	logger = logger.Hook(CallerHook{})

	if os.Getenv("LOG_JSON") == "" && !logsDisabledForTests() {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	if os.Getenv("TRACE") == "1" {
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	} else if os.Getenv("DEBUG") == "0" {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	return logger
}

func configureLoggerGlobals() {
	configureGlobalsOnce.Do(func() {
		zerolog.TimeFieldFormat = time.RFC3339Nano
		if lvlKey := os.Getenv("LOG_LEVEL_KEY"); lvlKey != "" {
			zerolog.LevelFieldName = lvlKey
		} else {
			zerolog.LevelFieldName = "level"
		}
		zerolog.TimestampFieldName = "time"
		zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
			function := ""
			fun := runtime.FuncForPC(pc)
			if fun != nil {
				funName := fun.Name()
				slash := strings.LastIndex(funName, "/")
				if slash > 0 {
					funName = funName[slash+1:]
				}
				function = " " + funName + "()"
			}
			return file + ":" + strconv.Itoa(line) + function
		}
		if os.Getenv("TRACE") == "1" {
			zerolog.SetGlobalLevel(zerolog.TraceLevel)
		} else if os.Getenv("DEBUG") == "0" {
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		} else {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		}
	})
}

func logsDisabledForTests() bool {
	if os.Getenv("CRAQ_ENABLE_TEST_LOGS") == "1" {
		return false
	}
	return strings.HasSuffix(os.Args[0], ".test")
}

type CallerHook struct{}

func (h CallerHook) Run(e *zerolog.Event, _ zerolog.Level, _ string) {
	e.Caller(3)
}
