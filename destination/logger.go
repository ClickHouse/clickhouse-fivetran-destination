package main

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitLogger(isDevelopment bool) {
	if isDevelopment {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

func LogInfo(msg string) {
	log.Log().
		Str("message-origin", "sdk_destination").
		Str("level", "INFO").
		Msg(msg)
}

func LogWarn(msg string) {
	log.Log().
		Str("message-origin", "sdk_destination").
		Str("level", "WARNING").
		Msg(msg)
}

func LogError(err error) {
	log.Log().
		Str("message-origin", "sdk_destination").
		Str("level", "SEVERE").
		Str("message", fmt.Sprintf("%+v", err)).Send()
}
