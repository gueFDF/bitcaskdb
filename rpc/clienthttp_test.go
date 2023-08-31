package rpc

import (
	"log"
	"net"
	"os"
	"runtime"
	"testing"
)

func TestDial(t *testing.T) {
	if runtime.GOOS == "linux" {
		ch := make(chan struct{})
		addr := "/tmp/rpc.sock"
		//充当服务器，让协程跑
		go func() {
			//_ = os.Remove(addr)
			l, err := net.Listen("unix", addr)
			if err != nil {
				log.Fatal("failed to listen unix socket")
			}
			ch <- struct{}{}
			Accept(l)
		}()
		<-ch
		_, err := XDial("unix@" + addr)
		_assert(err == nil, "failed to connect unix socket")
		_ = os.Remove(addr)
	}
}
