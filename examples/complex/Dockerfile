FROM golang:1.22

# Install required dependencies for CGO
RUN apt-get update && apt-get install -y gcc libc6-dev ca-certificates && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app
WORKDIR /app
COPY . .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main main.go

CMD ["./main"]
