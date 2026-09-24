# --- build stage -------------------------------------------------------
# Java + Maven live only in this stage, so you don't need either on your
# machine.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src

# Copy pom.xml first so Docker can cache the downloaded dependencies as a
# separate layer — they only get re-downloaded when pom.xml changes, not on
# every source-code edit.
COPY pom.xml ./
RUN mvn -q -B dependency:go-offline

COPY src ./src
# Tests are run separately (see README.md), so a failing test while you're
# mid-exercise doesn't stop the app from starting.
RUN mvn -q -B -DskipTests package

# --- run stage -----------------------------------------------------------
# Just a Java runtime and the single "fat" jar with all dependencies.
FROM eclipse-temurin:21-jre
COPY --from=build /src/target/order-analytics.jar /app.jar
EXPOSE 8082
ENTRYPOINT ["java", \
  "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", \
  "-jar", "/app.jar"]
