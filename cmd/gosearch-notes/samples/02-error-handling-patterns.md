---
title: Error Handling Patterns in Go
tags: go, errors, patterns
category: best-practices
priority: 8
---
Go treats errors as values rather than exceptions. Functions return an error as the last return value, and callers are expected to check it explicitly.

The simplest pattern is to check and return:

    result, err := doSomething()
    if err != nil {
        return fmt.Errorf("doSomething failed: %w", err)
    }

Use fmt.Errorf with the %w verb to wrap errors, preserving the original error chain. This allows callers to use errors.Is and errors.As to inspect wrapped errors.

For sentinel errors, define package-level variables:

    var ErrNotFound = errors.New("not found")

Custom error types are useful when you need to attach structured data to an error, such as HTTP status codes or field names.

Avoid ignoring errors silently. If you intentionally discard an error, document why.
