# Build context is the REPO ROOT (not this directory) — see docker-compose.yml.
# Every service's pom.xml points at ../pom.xml (the multi-module reactor parent),
# and Maven needs to see all four module directories to validate that parent's
# <modules> list, even though we only build this one with -pl. Each service's
# Dockerfile rebuilds independently (no shared layer cache across images) —
# slower than a single shared build stage, but simpler, and build time isn't a
# concern for a project this size.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

COPY pom.xml .
COPY order-service/pom.xml order-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml
COPY payment-service/src payment-service/src

RUN mvn -q -pl payment-service -am clean package -DskipTests

FROM eclipse-temurin:17-jre-jammy
# curl for docker-compose's healthcheck — not in the base slim JRE image.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/payment-service/target/*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
