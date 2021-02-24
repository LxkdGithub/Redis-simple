package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)


type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close()
}


func ListenAndServe(address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("listen err: %v", err)
	}

	var closing atomic.AtomicBool
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 因为要阻塞所以只能开启协程
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			logger.Info("shuting down...")

			closing.Set(true)

			// listener关闭
			_ = listener.Close()
			// 逐个关闭连接
			_ = handler.Close()

		}
	}()

	log.Println(fmt.Sprintf("bind: %s, start listening...", address))

	defer func() {
		// 在保证出现错误后者panic后正常关闭
		// 但有问题，就是正常关闭后悔再次关闭
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx, _ := context.WithCancel(context.Background())

	// waitGroup当前存在连接数
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if closing.Get() {
				// 此时是因为外部中断而停止运行，wait等待其他处理协程结束
				logger.Info("waiting disconnect...")
				waitDone.Wait()
				return
			}
			log.Fatal(fmt.Sprintf("accept err:%v", err))
			continue
		}

		go func() {
			waitDone.Add(1)
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
}

func Handle(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("connection close")
			} else {
				log.Println(err)
			}
			return
		}
		b := []byte(msg)
		conn.Write(b)
	}
}


func main() {
	ListenAndServe(":8080")
}
