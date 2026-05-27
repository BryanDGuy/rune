FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN make build

FROM gcr.io/distroless/static-debian12

COPY --from=build /src/bin/rune /rune

EXPOSE 7946 9090
ENTRYPOINT ["/rune"]
