# Multi-stage build for AgentMesh hub (meshd) + CLI (mesh).
FROM golang:1.23-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/meshd ./cmd/meshd \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mesh ./cmd/mesh

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/meshd /usr/local/bin/meshd
COPY --from=build /out/mesh /usr/local/bin/mesh
EXPOSE 8080
ENV MESH_HOST=0.0.0.0 MESH_PORT=8080
# Non-loopback requires TLS or MESH_INSECURE=true (see DESIGN §6).
USER nobody
ENTRYPOINT ["meshd"]
CMD ["serve"]
