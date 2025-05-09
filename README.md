# MCP Proxy Server

An MCP proxy server that aggregates and serves multiple MCP resource servers through a single HTTP server.

## Features

- **Proxy Multiple MCP Clients**: Connects to multiple MCP resource servers and aggregates their tools and capabilities.
- **SSE Support**: Provides an SSE (Server-Sent Events) server for real-time updates.
- **Flexible Configuration**: Supports multiple client types (`stdio` and `sse`) with customizable settings.

## Installation

### Build from Source
 ```bash
git clone https://github.com/TBXark/mcp-proxy.git
cd mcp-proxy
go build -o mcp-proxy main.go
./mcp-proxy --config path/to/config.json
```

### Install by go
```bash
go install github.com/TBXark/mcp-proxy@latest
````

### Docker
> The Docker image supports two MCP calling methods by default: `npx` and `uvx`.
```bash
docker run -d -p 8080:8080 -v /path/to/config.json:/config/config.json ghcr.io/tbxark/mcp-proxy:latest
```

## Configuration

The server is configured using a JSON file. Below is an example configuration:

```json
{
  "server": {
    "baseURL": "http://localhost:8080",
    "addr": ":8080",
    "name": "MCP Proxy",
    "version": "1.0.0"
  },
  "clients": {
    "fetch": {
      "type": "stdio",
      "config": {
        "command": "uvx",
        "env": {},
        "args": ["mcp-server-fetch"]
      }
    },
    "amap": {
      "type": "sse",
      "config": {
        "url": "https://router.mcp.so/sse/xxxxx"
      }
    }
  }
}
```

- **Server Configuration**:
  - `baseURL`: The base URL for the SSE server.
  - `addr`: The address the server listens on.
  - `name`: The name of the server.
  - `version`: The version of the server.

- **Clients Configuration**:
  - `type`: The type of the client (`stdio` or `sse`).
  - `config`: The specific configuration for the client type.

## Usage
```
Usage of mcp-proxy:
  -config string
        path to config file or a http(s) url (default "config.json")
  -help
        print help and exit
  -version
        print version and exit
```
1. The server will start and aggregate the tools and capabilities of the configured MCP clients.
2. You can access the server at the specified address (e.g., `http://localhost:8880/fetch/sse`).
3. If you are worried about URL leakage, you can change the key in `clients` such as `fetch` to a random string, and then access it via `/random-string/sse`.

## Thanks

This project was inspired by the [adamwattis/mcp-proxy-server](https://github.com/adamwattis/mcp-proxy-server) project

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.