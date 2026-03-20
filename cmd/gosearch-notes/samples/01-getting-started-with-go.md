---
title: Getting Started with Go
tags: go, basics, beginner
category: tutorial
priority: 9
---
Go is a statically typed, compiled language designed for simplicity and efficiency.

To install Go, download the latest release from the official website and follow the installation instructions for your platform. Verify the installation by running `go version` in your terminal.

A basic Go program starts with a package declaration, imports, and a main function:

    package main

    import "fmt"

    func main() {
        fmt.Println("Hello, World!")
    }

Key features of Go include fast compilation, garbage collection, built-in concurrency with goroutines, and a rich standard library.
