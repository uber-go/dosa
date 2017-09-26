package testutil

import "strconv"
import "net"

// IsRunningOnPort checks to see if anything is already running on a particular port
func IsRunningOnPort(port int) bool {
	socket, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		return true
	}
	_ = socket.Close()
	return false
}
