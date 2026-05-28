FROM golang:1.26 AS build

WORKDIR /src

COPY go.work go.work.sum ./
COPY rune/go.mod rune/go.sum ./rune/
COPY sdk/go/go.mod sdk/go/go.sum ./sdk/go/
RUN go mod download

COPY . .
RUN make build

FROM gcr.io/distroless/static-debian12

COPY --from=build /src/bin/rune /rune

EXPOSE 7946 9090
ENTRYPOINT ["/rune"]
