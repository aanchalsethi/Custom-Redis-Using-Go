package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Struct to store value and expiry time (in milliseconds)
type RedisEntry struct {
	value   string
	expires int64 // expiry timestamp in milliseconds
}

var redisMap = map[string]RedisEntry{}
var dir string
var dbfilename string

func main() {
	// Command-line flags for dir and dbfilename
	flag.StringVar(&dir, "dir", "", "Directory for Redis data")
	flag.StringVar(&dbfilename, "dbfilename", "", "RDB file name")
	flag.Parse()

	// Listening on TCP port 1234
	l, err := net.Listen("tcp", "0.0.0.0:1234")
	if err != nil {
		fmt.Println("Failed to bind to port 1234")
		os.Exit(1)
	}
	defer l.Close() // Ensure the listener is closed when the program exits
	fmt.Println("Server is listening on port 1234")

	// Main server loop to accept connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		// Handle each connection in a new goroutine for concurrency
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close() // Ensure connection is closed after handling the client

	reader := bufio.NewReader(conn) // Buffer for reading client input

	for {
		// Read a line of input from the client
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			return
		}

		resp := handleResponse(strings.ToLower(message))

		conn.Write([]byte(resp))
	}
}

// handleResponse processes each command and returns the response
func handleResponse(message string) string {
	cmds := strings.Fields(strings.TrimSpace(message))

	var resp string
	switch cmds[0] {
	case "ping":
		resp = "+PONG\r\n"
	case "echo":
		if len(cmds) < 2 {
			resp = "$-1\r\n" // No message to echo
		} else {
			resp = strings.Join(cmds[1:], " ") + "\r\n"
		}
	case "set":
		if len(cmds) >= 3 {
			key := cmds[1]
			value := cmds[2]
			expiryTime := int64(0) // No expiry by default

			// Handle optional PX argument for expiry
			if len(cmds) >= 5 && cmds[3] == "PX" {
				expiryMs, err := strconv.ParseInt(cmds[4], 10, 64)
				if err == nil {
					expiryTime = time.Now().UnixMilli() + expiryMs
				}
			}

			redisMap[key] = RedisEntry{value: value, expires: expiryTime}
			resp = "+OK\r\n"
		} else {
			resp = "-ERR wrong number of arguments for 'set' command\r\n"
		}
	case "get":
		if len(cmds) >= 2 {
			key := cmds[1]
			entry, exists := redisMap[key]

			// If the key doesn't exist or has expired
			if !exists || (entry.expires > 0 && time.Now().UnixMilli() > entry.expires) {
				resp = "$-1\r\n"      // Null bulk string
				delete(redisMap, key) // Remove the expired key
			} else {
				resp = "$" + strconv.Itoa(len(entry.value)) + "\r\n" + entry.value + "\r\n"
			}
		} else {
			resp = "-ERR wrong number of arguments for 'get' command\r\n"
		}
	case "config":
		if len(cmds) != 3 {
			resp = "-ERR wrong number of arguments for 'config' command\r\n"
		} else if cmds[1] == "get" {
			if cmds[2] == "dir" {
				resp = "*2\r\n$3\r\ndir\r\n$" + strconv.Itoa(len(dir)) + "\r\n" + dir + "\r\n"
			} else if cmds[2] == "dbfilename" {
				resp = "*2\r\n$10\r\ndbfilename\r\n$" + strconv.Itoa(len(dbfilename)) + "\r\n" + dbfilename + "\r\n"
			} else {
				resp = "$-1\r\n"
			}
		} else {
			resp = "-ERR unknown command\r\n"
		}
	default:
		resp = "-ERR unknown command\r\n"
	}

	return resp
}
