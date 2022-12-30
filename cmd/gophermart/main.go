package main

import (
	"flag"
	"log"
	"os"

	"github.com/inkpics/gophermart/internal/app"
)

func main() {
	var runAddr string
	var databaseAddr string
	var accrualAddr string

	flag.StringVar(&runAddr, "a", os.Getenv("RUN_ADDRESS"), "service address")
	flag.StringVar(&databaseAddr, "d", os.Getenv("DATABASE_URI"), "database address")
	flag.StringVar(&accrualAddr, "r", os.Getenv("ACCRUAL_SYSTEM_ADDRESS"), "accrual address")
	flag.Parse()

	if runAddr == "" {
		runAddr = "localhost:8080"
	}

	if databaseAddr == "" {
		log.Print("no database connection provided!")
		return
	}

	log.Printf("database connection string is %s", databaseAddr)

	err := app.Start(runAddr, databaseAddr, accrualAddr)
	if err != nil {
		log.Fatal(err)
	}
}
