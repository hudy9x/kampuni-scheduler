package main

import (
	"fmt"
	report "kampuni/scheduler/internal/cronjob"
	"kampuni/scheduler/pkg/db"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}

	if err := db.Run(); err != nil {
		panic(err)
	}

	report.ReportDaily()
}
