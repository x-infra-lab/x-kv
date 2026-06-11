.PHONY: build test package clean docker-build docker-up docker-down fmt

build:
	mvn compile -q

test:
	mvn test

package:
	mvn package -DskipTests -q

clean:
	mvn clean -q

docker-build:
	docker build -f docker/Dockerfile.pd -t x-pd:latest .
	docker build -f docker/Dockerfile.kv -t x-kv:latest .

docker-up:
	cd docker && docker-compose up -d

docker-down:
	cd docker && docker-compose down

fmt:
	@echo "fmt: no formatter configured yet"
