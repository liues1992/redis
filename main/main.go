package main

import "github.com/go-redis/redis"
import (
	"fmt"
	"github.com/go-redis/redis/internal/proto"
	"net"
	"io"
	"bufio"
	"runtime"
)

func main() {
    ExampleNewClient()
}

func ExampleNewClient() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("Launching redis proxy server...")

	//client := redis.NewClient(&redis.Options{
	//	Addr:     "localhost:6379",
	//	Password: "", // no password set
	//	DB:       0,  // use default DB
	//})

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:     []string {
			"localhost:30001",
			"localhost:30002",
			"localhost:30003",
			"localhost:30004",
			"localhost:30005",
			"localhost:30006",
		},
		Password: "", // no password set
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

func handleConn(c net.Conn, cli *redis.ClusterClient) {
	r := proto.NewReader(c)
	w := bufio.NewWriter(c)

	for {
		var allReq [][]string
		w.Reset(c)
		for i := 0; i == 0 || r.HasMore(); i++ {
			var query, err = r.ReadReq()

			if err == nil {
				//fmt.Printf("input :%s\n", strings.Join(query, " "))
				allReq = append(allReq, query)
			} else {
				c.Close()
				if err == io.EOF {
					fmt.Print("X ")
				} else {
					fmt.Println("proxy: read query err", err)
				}
				return
			}
		}

		//fmt.Printf("req count %d\n", len(allReq))

		if len(allReq) == 1 {
			var query= allReq[0]
			tmpSlice := make([]interface{}, len(query))
			for i, v := range query {
				tmpSlice[i] = v
			}
			cmd := redis.NewCmd(tmpSlice...)
			err := cli.Process(cmd)
			if err != nil {
				replyError(err, c)
			} else {
				processCmd(cmd, c, w)
			}
		} else {
			pipe := cli.Pipeline()
			var cmds []*redis.Cmd
			for _, query := range allReq {
				// presume it is pipeline
				tmpSlice := make([]interface{}, len(query))
				for i, v := range query {
					tmpSlice[i] = v
				}
				cmd := redis.NewCmd(tmpSlice...)
				cmds = append(cmds, cmd)
				//cli.Process(cmd)
				pipe.Process(cmd)

			}
			_, err := pipe.Exec()
			if err != nil {
				fmt.Println("pipeline exec error", err)
			} else {
				for _, cmd := range cmds {
					processCmd(cmd, c, w)
				}
			}
			pipe.Close()
		}
		w.Flush()
	}
}

func processCmd(cmd *redis.Cmd, c net.Conn, w *bufio.Writer) {
	var err error
	_, err = cmd.Result()
	if err != nil {
		replyError(err, c)
	} else {
		//fmt.Printf("now write %d line\n", len(cmd.GetChainBuf()))
		for _, bufLine := range cmd.GetChainBuf() {
			//fmt.Println("write line ", fmt.Sprintf("%s", bufLine))
			// todo handle write error
			w.Write(bufLine)
		}
	}
}


func replyError(err error, c net.Conn) {
	if err == redis.Nil {
		c.Write([]byte{'$', '-', '1', '\r', '\n'})
		return
	}
	fmt.Println("proxy error: ", err)
	var errReply = []byte("-")
	errReply = append(errReply, []byte(err.Error() + "\r\n")...)
	c.Write(errReply)
}