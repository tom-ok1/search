---
title: Writing Tests in Go
tags: go, testing, basics
category: best-practices
priority: 7
---
Go has a built-in testing framework in the testing package. Test files end with _test.go and test functions start with Test:

    func TestAdd(t *testing.T) {
        got := Add(2, 3)
        if got != 5 {
            t.Errorf("Add(2, 3) = %d, want 5", got)
        }
    }

Run tests with `go test ./...` to test all packages recursively.

Table-driven tests are idiomatic in Go. Define test cases as a slice of structs and loop over them:

    tests := []struct {
        name string
        a, b int
        want int
    }{
        {"positive", 2, 3, 5},
        {"zero", 0, 0, 0},
        {"negative", -1, 1, 0},
    }

Use t.Run for subtests to get clear output on failures. Benchmarks use testing.B and are run with `go test -bench=.`
