# Stage 1: Build
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
# No go.sum yet, will be generated if needed
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o search-node ./cmd/node/main.go

# Stage 2: Run (Minimal footprint)
FROM scratch
COPY --from=builder /app/search-node /search-node
ENTRYPOINT ["/search-node"]
