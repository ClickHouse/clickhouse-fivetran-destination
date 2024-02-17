package log

import (
	"fmt"
	"os"

	"fivetran.com/fivetran_sdk/destination/common/flags"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Init() error {
	switch *flags.LogLevel {
	case "notice":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warning":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "severe":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		return fmt.Errorf("invalid log level: %s, allowed values: notice, info, warning, severe", *flags.LogLevel)
	}
	if *flags.LogPretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	return nil
}

func Notice(msg string) {
	if zerolog.GlobalLevel() == zerolog.TraceLevel {
		log.Log().
			Str("message-origin", "sdk_destination").
			Str("level", "NOTICE").
			Msg(msg)
	}
}

func Info(msg string) {
	if zerolog.GlobalLevel() <= zerolog.InfoLevel {
		log.Log().
			Str("message-origin", "sdk_destination").
			Str("level", "INFO").
			Msg(msg)
	}
}

func Warn(msg string) {
	if zerolog.GlobalLevel() <= zerolog.WarnLevel {
		log.Log().
			Str("message-origin", "sdk_destination").
			Str("level", "WARNING").
			Msg(msg)
	}
}

func Error(err error) {
	if zerolog.GlobalLevel() <= zerolog.ErrorLevel {
		log.Log().
			Str("message-origin", "sdk_destination").
			Str("level", "SEVERE").
			Str("message", fmt.Sprintf("%+v", err)).
			Send()
	}
}
