# Use the official Golang image for building
FROM golang:latest

# Set the working directory in the container
WORKDIR /bgpfinder

# Copy the entire project to the container
COPY . .

# Download dependencies based on go.mod and go.sum
RUN go mod download

# Build the Go application (this assumes your app is located under cmd/bgpfinder-server)
RUN cd cmd/bgpfinder-server && go build -o bgpfinder-server

# Make the binary executable
RUN chmod +x /bgpfinder/cmd/bgpfinder-server/bgpfinder-server

# Expose the port for the Go application
EXPOSE 8080

# Set the default command to run the Go application
CMD ["/bgpfinder/cmd/bgpfinder-server/bgpfinder-server", "--port=8080", "--use-db", "--env-file", "../../example.env"]
