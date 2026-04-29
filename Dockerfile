FROM eclipse-temurin:17-jdk-alpine AS build

WORKDIR /workspace

COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -B -DskipTests dependency:go-offline

COPY src src
RUN ./mvnw -B -DskipTests package

FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

RUN addgroup -S scheduler && adduser -S scheduler -G scheduler

COPY --from=build /workspace/target/*.jar app.jar

USER scheduler

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
