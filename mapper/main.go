package main

import (
	pb "MapReduce/mapreduce"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

type server struct {
	pb.UnimplementedMapReduceServer
}

var (
	result []int32
)

func (s *server) SortData(_ context.Context, in *pb.DataSet) (*pb.DataSet, error) {

	fmt.Println("Serving master")
	numbers := in.GetValues()
	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})

	result = numbers
	fmt.Println("Mapper sorted")

	//mando risultato ai reducer
	file, err := os.Open("../config.json")
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Errore durante la lettura del file: %v", err)
	}

	var data map[string]string
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		log.Fatalf("Errore durante la decodifica del JSON: %v", err)
	}

	//divido il risultato per i reducer
	// Calcola il range massimo e suddivisione
	input := make([][]int32, 2)
	input[0] = []int32{}
	input[1] = []int32{}

	fmt.Println("Dividing result for reducers")

	for _, v := range result {
		if v < 50 {
			input[0] = append(input[0], v)
		} else {
			input[1] = append(input[1], v)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		key := "addrReducer0"
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
		_, err = c.SortData(ctx, &pb.DataSet{Values: input[0]})
		if err != nil {
			log.Fatalf("Could not sort data: %v", err)
		}
		log.Printf("Data sorted successfully")
	}()

	go func() {
		defer wg.Done()
		key := "addrReducer1"
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
		_, err = c.SortData(ctx, &pb.DataSet{Values: input[1]})
		if err != nil {
			log.Fatalf("Could not sort data: %v", err)
		}
		log.Printf("Data sorted successfully")
	}()

	wg.Wait()

	set := pb.DataSet{Values: numbers}
	return &set, nil
}

func main() {
	//check mapper port and number of reducers
	port := flag.String("port", "50051", "Mapper port.")
	flag.Parse()

	//ottengo i dati dal master
	lis, err := net.Listen("tcp", "localhost:"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterMapReduceServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
