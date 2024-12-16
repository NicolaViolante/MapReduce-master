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
	filename := portInfo + ".json"

	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Errore nella creazione del file: %v\n", err)
		return
	}
	defer file.Close()

	jsonData, err := json.Marshal(numbers)
	if err != nil {
		fmt.Printf("Errore nella conversione in JSON: %v\n", err)
		return
	}

	_, err = file.Write(jsonData)
	if err != nil {
		fmt.Printf("Errore nella scrittura del file JSON: %v\n", err)
		return
	}

	fmt.Printf("File JSON '%s' creato con successo.\n", filename)
}

func main() {
	//check mapper port and number of reducers
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
