package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	pb "MapReduce/mapreduce"
	"google.golang.org/grpc"
)

func main() {

	// open json file maintaining data
	file, err := os.Open("../data.json")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// insert data into a slice
	var numbers []int32
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&numbers)
	if err != nil {
		fmt.Println("Error decoding json file:", err)
		return
	}

	var slices [][]int32
	slices = append(slices, numbers[:10], numbers[10:])

	// open json file maintaining mapper addresses
	file, err = os.Open("../config.json")
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// read data and decode them into a map
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Errore durante la lettura del file: %v", err)
	}

	var data map[string]string
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		log.Fatalf("Errore durante la decodifica del JSON: %v", err)
	}

	//launch two go routines (implement concurrency) in which master connects to mappers
	//and send them the data to sort
	//In order to avoid an early termination of the process master use wait groups
	var wg sync.WaitGroup

	wg.Add(2) //add # of mappers that will be launched

	for i := 0; i < 2; i++ {

		localI := i

		go func(index int) {
			defer wg.Done()
			key := "addrMapper" + strconv.Itoa(index)
			address, found := data[key]
			if found {
				fmt.Printf("'%s': %v\n", key, address)
			} else {
				fmt.Printf("'%s' not found in config file\n", key)
				return
			}

			// Set up a connection to the server.
			// Unsecured connection between client and server
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Connection failed: %v", err)
			}
			defer conn.Close() // Close the connection when everything is done.

			// Pass the connection and create a client stub instance.
			// It contains all the remote methods to invoke the server.
			c := pb.NewMapReduceClient(conn)

			// Create a Context to pass with the remote call.
			// Context object contains metadata (identity of end user, authorization
			// tokens and request deadline) and will exist during the lifetime
			// of the request.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// Call addProduct method with product details.
			// addProduct returns a product ID if the action completed successfully.
			// Otherwise it returns an error.
			_, err = c.SortData(ctx, &pb.DataSet{Values: slices[index]})
			if err != nil {
				log.Fatalf("Could not sort data: %v", err)
			}
			log.Printf("Data sorted successfully")
		}(localI)
	}

	wg.Wait()

}
