package main

import (
	pb "MapReduce/mapreduce"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"sort"
)

type server struct {
	pb.UnimplementedMapReduceServer
}

var (
	portInfo string
	numbers  []int32
	count    = 0
)

func (s *server) SortData(_ context.Context, in *pb.DataSet) (*pb.DataSet, error) {

	//if all mappers have sent their data proceed with sorting, otherwise just append mapper's data
	count++

	for _, v := range in.GetValues() {
		numbers = append(numbers, v)
	}

	if count == 2 {
		sort.Slice(numbers, func(i, j int) bool {
			return numbers[i] < numbers[j]
		})

		fmt.Println("Data sorted.")

		printOutFile(numbers)

		set := pb.DataSet{Values: numbers}
		return &set, nil
	}

	return nil, nil
}

func printOutFile(numbers []int32) {
	//print service result into a file json
	filename := "reducer" + portInfo + ".json"

	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer file.Close()

	jsonData, err := json.Marshal(numbers)
	if err != nil {
		fmt.Printf("Error marshaling json data: %v\n", err)
		return
	}

	_, err = file.Write(jsonData)
	if err != nil {
		fmt.Printf("Error writing json file: %v\n", err)
		return
	}

	fmt.Printf("File json '%s' created successfully\n", filename)
}

func main() {
	//check reducer port
	port := flag.String("port", "50051", "Reducer port.")
	flag.Parse()
	portInfo = *port

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
