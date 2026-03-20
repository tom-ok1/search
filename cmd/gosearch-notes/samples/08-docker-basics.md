---
title: Docker Basics for Go Applications
tags: docker, containers, deployment
category: devops
priority: 4
---
Docker packages applications into portable containers. A multi-stage Dockerfile for Go keeps the final image small:

    FROM golang:1.22 AS builder
    WORKDIR /app
    COPY go.mod go.sum ./
    RUN go mod download
    COPY . .
    RUN CGO_ENABLED=0 go build -o /server .

    FROM alpine:3.19
    COPY --from=builder /server /server
    ENTRYPOINT ["/server"]

Build and run:

    docker build -t myapp .
    docker run -p 8080:8080 myapp

Use .dockerignore to exclude unnecessary files from the build context. Keep images minimal by using scratch or alpine base images for the final stage.

For local development, docker compose simplifies multi-service setups with databases, caches, and your application running together.
