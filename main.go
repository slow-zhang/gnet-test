package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/anevsky/cachego/memory"
	"github.com/panjf2000/gnet"
	"github.com/panjf2000/gnet/pkg/pool/goroutine"
	"github.com/yongman/go/goredis"
)

var (
	OK         = []byte("++OK\r\n")
	errInput   = []byte("+-Error input\r\n")
	config     = []byte("+*2\r\n$6\r\nconfig\r\n$0\r\n\r\n")
	commonErr  = "+-Error %s\r\n"
	blank      = []byte("\r\n")
	dirGnet    = "tcp://:6383"
	dir2       = ":6382"
	cache      = memory.Alloc()
	workerPool = goroutine.Default()
)

func NewErr(str string) []byte {
	return []byte(fmt.Sprintf(commonErr, str))
}

type echoServer struct {
	*gnet.EventServer
}

func handle(input [][]byte) []byte {
	switch string(input[0]) {
	case "set":
		if len(input) >= 3 {
			err := cache.SetString(string(input[1]), string(input[2]))
			if err == nil {
				return OK
			}
			return NewErr(err.Error())
		}
		return NewErr("input < 3")
	case "get":
		data, err := cache.Get(string(input[1]))
		if err != nil {
			return NewErr(err.Error())
		}
		str, ok := data.(string)
		if !ok {
			return NewErr("not string for get")
		}
		return []byte(str)
	case "cmd":
		return config
	}
	return NewErr("not supported cmd")
}

func makeResp(resp []byte) []byte {
	if resp[0] == '+' || resp[0] == '-' {
		return resp[1:]
	}
	b := new(bytes.Buffer)
	w := goredis.NewRespWriter(bufio.NewWriter(b))
	w.WriteBulk(resp)
	w.Flush()
	return b.Bytes()
}

func (es *echoServer) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	if c.Context() != nil {
		// bad thing happened
		out = errInput[1:]
		action = gnet.Close
		return
	}
	// handle the request
	out = frame
	return
}

func echo() {
	echo := new(echoServer)
	log.Fatal(gnet.Serve(echo, dirGnet, gnet.WithMulticore(true), gnet.WithReadBufferCap(8*1024*1024),
		gnet.WithCodec(new(RedisCodec))))
}

func main() {
	go echo()
	listener, _ := net.Listen("tcp", dir2)
	for {
		conn, _ := listener.Accept()
		go func() {
			br := bufio.NewReader(conn)
			r := goredis.NewRespReader(br)
			bw := bufio.NewWriter(conn)
			for {
				input, err := r.ParseRequest()
				// check ParseRequest errors
				switch {
				case err != nil && strings.Contains(err.Error(), "short resp line"):
					continue
				case err != nil && !errors.Is(err, io.EOF):
					return
				case err != nil:
					return
				}
				resp := handle(input)
				resp = makeResp(resp)
				bw.Write(resp)
				bw.Flush()
			}
		}()
	}
}

type RedisCodec struct {
}

type Resp struct {
	cmds     [][]byte
	n        int
	complete bool
}

func (r *RedisCodec) Encode(c gnet.Conn, buf []byte) (out []byte, err error) {
	if c.Context() == nil {
		return buf, nil
	}
	return errInput[1:], nil
}

func (r *RedisCodec) Decode(c gnet.Conn) (out []byte, err error) {
	buf := c.Read()
	if len(buf) == 0 {
		c.ResetBuffer()
		return
	}
pipeline:
	if len(buf) == 0 {
		return
	}
	resp := new(Resp)
	err = resp.parserRESP(buf)
	// bad thing happened
	if err != nil {
		c.SetContext(err)
		return nil, err
	} else if !resp.complete {
		// request not ready, yet
		log.Println("fail to handling short req: ", string(buf))
		return
	}
	c.ShiftN(resp.n)
	buf = buf[resp.n:]
	out = append(out, makeResp(handle(resp.cmds))...)
	goto pipeline
}

func (r *Resp) parserRESP(buf []byte) error {
	line := r.readLine(buf)
	if len(line) == 0 {
		return nil
	}
	if line[0] == '*' {
		n, err := strconv.Atoi(string(line[1:]))
		if n < 0 || err != nil {
			return err
		}
		r.cmds = make([][]byte, n)
		for i := range r.cmds {
			r.cmds[i], err = r.readBulk(buf)
			if len(r.cmds[i]) == 0 && err != nil {
				return err
			}
		}
		r.complete = true
	}
	return nil
}

func (r *Resp) readLine(buf []byte) []byte {
	if i := bytes.IndexByte(buf[r.n:], '\n'); i >= 0 {
		defer func() {
			r.n += i + 1
		}()
		return buf[r.n : r.n+i-1]
		// todo: check line and if line nil
	} else {
		return nil
	}
}

func (r *Resp) readBulk(buf []byte) ([]byte, error) {
	line := r.readLine(buf)
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] == '$' {
		n, err := strconv.Atoi(string(line[1:]))
		if n < 0 || err != nil {
			return nil, err
		}
		if len(buf) < n+r.n {
			return nil, nil
		}
		defer func() {
			r.n += n + 2
		}()
		return buf[r.n : r.n+n], nil
	}
	return nil, errors.New(string(NewErr("not supported cmd")))
}
