package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadline(t *testing.T) {
	resp := new(Resp)
	buf := []byte("*1\r\n$7\r\nCommand\r\n")
	line := resp.readLine(buf)
	assert.Equal(t, buf[:2], line)
	line = resp.readLine(buf)
	assert.Equal(t, buf[4:6], line)
	line = resp.readLine(buf)
	assert.Equal(t, buf[8:15], line)
}

func TestReadBulk(t *testing.T) {
	resp := new(Resp)
	buf := []byte("$7\r\nCommand\r\n$1\r\na\r\n")
	line, _ := resp.readBulk(buf)
	assert.Equal(t, buf[4:11], line)
	line, _ = resp.readBulk(buf)
	assert.Equal(t, []byte{'a'}, line)
	assert.Equal(t, len(buf), resp.n)
}

func TestDecode(t *testing.T) {
	resp := new(Resp)
	buf := []byte("*1\r\n$7\r\nCommand\r\n")
	err := resp.parserRESP(buf)
	assert.NoError(t, err)

	resp = new(Resp)
	buf = []byte("")
	err = resp.parserRESP(buf)
	assert.NoError(t, err)
}
