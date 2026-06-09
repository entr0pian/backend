FROM golang:1.24

WORKDIR /app

# Copy all source code
COPY . .

# Download dependencies and build binary
RUN go mod tidy
RUN go build -o app

# App listens on 8080
EXPOSE 8080

# Run the app
CMD ["./app"]
