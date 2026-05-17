package main

import (
	"fmt"
	"os"

	"baidudisklink/internal/app"
	"baidudisklink/internal/config"
)

func main() {
	cfg := config.Load()
	application, err := app.New(app.Config(cfg))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := application.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
