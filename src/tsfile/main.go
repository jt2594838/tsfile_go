// tsfile project main.go
package main

import (
	"log"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Error: ", err)
		}
	}()

	log.Println("Program exit.")
}
