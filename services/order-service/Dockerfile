# --- build stage -------------------------------------------------------
FROM golang:1.24-alpine AS build
WORKDIR /src

# Copy go.mod/go.sum first so Docker can cache the downloaded dependencies
# as a separate layer — they only get re-downloaded when these two files
# change, not on every source-code edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /app .

# --- run stage -----------------------------------------------------------
# A tiny final image: just the compiled binary, no Go toolchain, no source.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /app /app
EXPOSE 8081
ENTRYPOINT ["/app"]
