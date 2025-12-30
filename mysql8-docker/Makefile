.PHONY: build-primary build-replica start stop clean

# Build the primary and replica images
build-primary:
	docker build -t mysql-primary .

build-replica:
	docker build --build-arg build_type=replica -t mysql-replica .

# Start the primary and replica
start: build-primary build-replica
	docker-compose up -d --wait

# Stop the primary and replica
stop:
	docker-compose down -v --remove-orphans

# Clean up the images
clean:
	-docker rmi mysql-primary mysql-replica
	-docker volume rm mysql-primary-data mysql-replica-data
