package cmd

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"fivetran.com/fivetran_sdk/destination/common/flags"
	"fivetran.com/fivetran_sdk/destination/common/log"
	"fivetran.com/fivetran_sdk/destination/service"
	pb "fivetran.com/fivetran_sdk/proto"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

var ExitChan = make(chan os.Signal, 1)

func StartServer() {
	flag.Parse()
	err := log.Init()
	if err != nil {
		log.Error(fmt.Errorf("failed to initialize logger: %w", err))
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *flags.Port))
	if err != nil {
		log.Error(fmt.Errorf("failed to listen: %w", err))
		os.Exit(1)
	}
	s := grpc.NewServer()
	pb.RegisterDestinationConnectorServer(s, &service.Server{})

	errChan := make(chan error)
	signal.Notify(ExitChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		var sb strings.Builder
		flag.VisitAll(func(f *flag.Flag) {
			sb.WriteString(fmt.Sprintf("%s: %s, ", f.Name, f.Value))
		})
		flagsValues := sb.String()
		log.Info(fmt.Sprintf("Server is ready. Flags: %s", flagsValues[:len(flagsValues)-2]))
		err := s.Serve(listener)
		if err != nil {
			errChan <- err
		}
	}()

	select {
	case <-ExitChan:
		log.Info("Shutting down the server...")
		s.GracefulStop()
	case err = <-errChan:
		log.Error(fmt.Errorf("failed to serve: %w", err))
		os.Exit(1)
	}
}
