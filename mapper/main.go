package main

import (
	pb "MapReduce/mapreduce"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
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

	//sorting data
	fmt.Println("Serving master")
	numbers := in.GetValues()
	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})

	result = numbers
	fmt.Println("Mapper done sorting")

	//getting reducers addresses
	file, err := os.Open("../config.json")
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Error reading file json: %v", err)
	}

	var data map[string]string
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		log.Fatalf("Error decoding file json: %v", err)
	}

	//splitting data to send to reducers
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

	//launching reducers
	var wg sync.WaitGroup
	wg.Add(2)

	for i := 0; i < 2; i++ {
		localI := i

		go func(index int) {
			defer wg.Done()
			key := "addrReducer" + strconv.Itoa(index)
			address, found := data[key]
			if found {
				fmt.Printf("'%s': %v\n", key, address)
			} else {
				fmt.Printf("'%s' not found in config file\n", key)
				return
			}

			// Set up a connection to the server
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("Connection failed: %v", err)
			}
			defer conn.Close() // Close the connection when everything is done.

			// Pass the connection and create a client stub instance
			c := pb.NewMapReduceClient(conn)

			// Create a Context to pass with the remote call
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// Call SortData method
			_, err = c.SortData(ctx, &pb.DataSet{Values: input[index]})
			if err != nil {
				log.Fatalf("Could not sort data: %v", err)
			}
			log.Printf("Data sorted successfully")
		}(localI)
	}

	wg.Wait()

	set := pb.DataSet{Values: numbers}
	return &set, nil
}

func main() {
	//check mapper port
	port := flag.String("port", "50051", "Mapper port.")
	flag.Parse()

	//get data from master
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
