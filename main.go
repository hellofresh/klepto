package main

import (
	"log"

	"github.com/hellofresh/klepto/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
