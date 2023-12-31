package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// 测试客户端的连接超时情况
func TestClient_dialTimout(t *testing.T) {
	t.Parallel()
	l, _ := net.Listen("tcp", "localhost:9999")

	//模拟NewClient
	f := func(conn net.Conn, opt *Option) (client *Client, err error) {
		_ = conn.Close()
		time.Sleep(time.Second * 2)
		return nil, nil
	}
	//测试连接超时的情况
	t.Run("timeout", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectTimeout: time.Second})
		_assert(err != nil && strings.Contains(err.Error(), "connect timeout"), "expect a timeout error")

	})

	//测试无时间限制的连接
	t.Run("0", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectTimeout: 0})
		_assert(err == nil, "0 means no limit")
	})
}

// 测试处理超时
type Bar int

func (b Bar) Timeout(argv int, reply *int) error {
	time.Sleep(time.Second * 2)
	return nil
}

func startServer(addr chan string) {
	var b Bar
	_ = Register(&b)
	l, _ := net.Listen("tcp", "localhost:9999")
	addr <- l.Addr().String()
	Accept(l)
}

func TestClient_Call(t *testing.T) {
	t.Parallel()
	addrCh := make(chan string)
	go startServer(addrCh)
	addr := <-addrCh

	time.Sleep(time.Second)
	t.Run("client timeout", func(t *testing.T) {
		client, _ := Dial("tcp", addr)

		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		var reply int
		err := client.Call(ctx, "Bar.Timeout", 1, &reply)

		_assert(err != nil && strings.Contains(err.Error(), ctx.Err().Error()), "expect a timeout error")

	})

	t.Run("server handle timeout", func(t *testing.T) {
		client, _ := Dial("tcp", addr, &Option{
			HandleTimeout: time.Second,
		})
		var reply int

		err := client.Call(context.Background(), "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), "handle timeout"), "expect a timeout error")

	})

}

func hello(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "颜然大傻瓜\n")
	io.WriteString(w, "颜然大傻瓜\n")
	io.WriteString(w, "颜然大笨蛋\n")
	io.WriteString(w, "颜然大笨蛋\n")

}

// func main() {
// 	http.HandleFunc("/hello", hello)
// 	http.ListenAndServe(":8080", nil)
// }

func TestHttp(t *testing.T) {
	http.HandleFunc("/hello", hello)
	err := http.ListenAndServe("15.235.163.186:1010", nil)
	if err != nil {
		fmt.Println("err:", err)
	}
}
