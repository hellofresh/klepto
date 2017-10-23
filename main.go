package main

import (
	"fmt"
	"os"

	"github.com/hellofresh/klepto/cmd"
)

func main() {
	if err := cmd.Klepto.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
