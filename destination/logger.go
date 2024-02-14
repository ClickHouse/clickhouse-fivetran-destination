package main

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitLogger(isDevelopment bool) error {
	switch *logLevel {
	case "notice":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warning":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "severe":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		return fmt.Errorf("invalid log level: %s, allowed values: notice, info, warning, severe", *logLevel)
	}
	if isDevelopment {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	return nil
}

func LogNotice(msg string) {
	if zerolog.GlobalLevel() == zerolog.TraceLevel {
		log.Log().
			Str("message-origin", "sdk_destination").
			Str("level", "NOTICE").
			Msg(msg)
	}
}

func LogInfo(msg string) {
	if zerolog.GlobalLevel() <= zerolog.InfoLevel {
		log.Log().
			Str("message-origin", "sdk_destination").
			Str("level", "INFO").
			Msg(msg)
	}
}

func LogWarn(msg string) {
	if zerolog.GlobalLevel() <= zerolog.WarnLevel {
		log.Log().
			Str("message-origin", "sdk_destination").
			Str("level", "WARNING").
			Msg(msg)
	}
}

func LogError(err error) {
	if zerolog.GlobalLevel() <= zerolog.ErrorLevel {
		log.Log().
			Str("message-origin", "sdk_destination").
			Str("level", "SEVERE").
			Str("message", fmt.Sprintf("%+v", err)).
			Send()
	}
}
