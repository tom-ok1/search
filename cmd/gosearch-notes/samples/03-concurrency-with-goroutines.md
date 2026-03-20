---
title: Concurrency with Goroutines and Channels
tags: go, concurrency, goroutines, channels
category: tutorial
priority: 8
---
Goroutines are lightweight threads managed by the Go runtime. Launch one with the go keyword:

    go func() {
        fmt.Println("running concurrently")
    }()

Channels provide a way for goroutines to communicate and synchronize:

    ch := make(chan string)
    go func() {
        ch <- "hello from goroutine"
    }()
    msg := <-ch

Buffered channels allow sending without an immediate receiver:

    ch := make(chan int, 10)

Use the select statement to wait on multiple channel operations simultaneously. This is the foundation of many concurrent patterns in Go, including fan-in, fan-out, and pipeline architectures.

Always ensure goroutines can exit. Leaking goroutines is a common source of memory issues in long-running Go applications.
