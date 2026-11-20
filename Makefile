SERVICE_DIR := $(abspath ../gap-traffic-data-processing)
JDK21 ?= /usr/lib/jvm/java-21-openjdk-amd64
BASE := controller kafka-1 kafka-2 kafka-3 kafka-ui timescaledb

.PHONY: jar up down run test itest

# The service targets Java 21 and its mvnw has no exec bit; fall back to a container if the host JDK 21 is missing.
jar:
	mkdir -p build
	cd $(SERVICE_DIR) && JAVA_HOME=$(JDK21) sh ./mvnw -q -pl gap-streaming -am package -DskipTests || \
	  docker run --rm --user $$(id -u):$$(id -g) -e MAVEN_CONFIG=/tmp/.m2 -v $$HOME/.m2:/tmp/.m2 -v $(SERVICE_DIR):/src -w /src \
	    maven:3.9-eclipse-temurin-21 mvn -q -o -pl gap-streaming -am package -DskipTests
	cp $$(ls $(SERVICE_DIR)/gap-streaming/target/gap-streaming-*.jar | grep -v plain | head -1) build/service.jar

up:
	touch .env
	docker compose up -d $(BASE) gap-svc-1

down:
	docker compose --profile scale down -v

run:
	go run ./cmd/lab

test:
	go test ./...

itest:
	go test -tags integration ./...
