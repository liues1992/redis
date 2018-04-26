package main

import "github.com/go-redis/redis"
import (
	"fmt"
	"github.com/go-redis/redis/internal/proto"
	"net"
)

func main() {
    ExampleNewClient();
}

func ExampleNewClient() {
	fmt.Println("Launching redis proxy server...")

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// listen on all interfaces
	ln, _ := net.Listen("tcp", ":6666")

	// accept connection on port
	for {
		conn, _ := ln.Accept()

		go handleConn(conn, client)
	}




	// Output: PONG <nil>
}

func handleConn(c net.Conn, cli *redis.Client) {
	r := proto.NewReader(c)
	for {
		result, err := r.ReadReq()

		if err == nil {
			for _, item := range result {
				fmt.Printf("input %s\n", item)
			}
		} else {
			fmt.Println("proxy: read req err", err)
			return
		}

		cmd := redis.NewCmd(result...)
		cli.Process(cmd)

		_, err = cmd.Result()
		if err != nil {
			fmt.Println("proxy error: ", err)
			var errReply = []byte("-")
			errReply = append(errReply, []byte(err.Error() + "\r\n")...)
			c.Write(errReply)
		} else {
			fmt.Println("now write", len(cmd.GetChainBuf()))
			for _, bufLine := range cmd.GetChainBuf() {
				fmt.Println("write line ", bufLine, fmt.Sprintf("%s", bufLine))
				c.Write(bufLine)
			}
		}
	}

}