package main

import (
	"net"
	"os"
	"time"
)

type Addr struct{}

func (a Addr) Network() string {
	return "stdio"
}

func (a Addr) String() string {
	return "stdio"
}

type StdioConn struct {
	stdin  *os.File
	stdout *os.File
}

func (s *StdioConn) Read(p []byte) (int, error) {
	return s.stdin.Read(p)
}

func (s *StdioConn) Write(p []byte) (int, error) {
	return s.stdout.Write(p)
}

func (s *StdioConn) Close() error {
	err1 := s.stdin.Close()
	err2 := s.stdout.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *StdioConn) LocalAddr() net.Addr {
	return Addr{}
}

func (s *StdioConn) RemoteAddr() net.Addr {
	return Addr{}
}

func (s *StdioConn) SetDeadline(t time.Time) error {
	err1 := s.stdin.SetReadDeadline(t)
	err2 := s.stdout.SetWriteDeadline(t)
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *StdioConn) SetReadDeadline(t time.Time) error {
	return s.stdin.SetReadDeadline(t)
}

func (s *StdioConn) SetWriteDeadline(t time.Time) error {
	return s.stdout.SetWriteDeadline(t)
}

type stdioListener struct {
	conn chan net.Conn
}

func (l *stdioListener) Accept() (net.Conn, error) {
	if conn, ok := <-l.conn; ok {
		return conn, nil
	}
	return nil, net.ErrClosed
}

func (l *stdioListener) Close() error {
	close(l.conn)
	return nil
}

func (l *stdioListener) Addr() net.Addr {
	return Addr{}
}
