package main

import "github.com/liues1992/redis-proxy"
import (
	"fmt"
	"github.com/liues1992/redis-proxy/internal/proto"
	"net"
	"io"
	"bufio"
	"runtime"
	"flag"
	"strings"
	"os"
)

func main() {
    newProxyServer()
}

// ./redis-proxy -listen 0.0.0.0:6379 -cluster -addr
func newProxyServer() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	//client := redis.NewClient(&redis.Options{
	//	Addr:     "127.0.0.1:6379",
	//	Password: "", // no password set
	//	DB:       0,  // use default DB
	//})
	var cli redis.ClientInterface
	listen := flag.String("l", ":6380", "addr to listen for")
	cluster := flag.Bool("c", false, "enable cluster or not")
	addr := flag.String("a", "127.0.0.1:6379", "addrs, separated by comma")
	flag.Parse()

	fmt.Println("Launching redis proxy server...")

	fmt.Printf("Using cluster: %t, listen on: %s, proxy for: %s\n", *cluster, *listen, *addr)
	if *cluster {
		cli = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     strings.Split(*addr, ","),
			Password: "", // no password set
		})
	} else {
		cli = redis.NewClient(&redis.Options{
			Addr:     *addr,
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	}


	// listen on all interfaces
	listenAddr := *listen
	var ln net.Listener
	var err error
	if strings.HasPrefix(listenAddr, "unix://") {
		os.Remove(listenAddr[7:])
		ln, err = net.Listen("unix", listenAddr[7:])
	} else if strings.HasPrefix(listenAddr, "tcp://") {
		ln, err = net.Listen("tcp", listenAddr[6:])
	} else {
		ln, err = net.Listen("tcp", listenAddr)
	}
	if err != nil {
		panic("cannot listen:" + err.Error())
	}

	// accept connection on port
	for {
		conn, _ := ln.Accept()

		go handleConn(conn, cli)
	}




	// Output: PONG <nil>
}

func handleConn(c net.Conn, cli redis.ClientInterface) {
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
			if err != nil && err != redis.Nil {
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
	if err == io.EOF {
		err = proto.RedisError("Remote Server Closed")
	}
	var errReply = []byte("-")
	errReply = append(errReply, []byte(err.Error() + "\r\n")...)
	c.Write(errReply)
}