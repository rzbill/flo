# Flo API

Go module for Flo proto definitions and generated stubs.

## Installation

```bash
go get github.com/rzbill/flo/api@latest
```

## Usage

```go
import flov1 "github.com/rzbill/flo/api/flo/v1"

// Use the generated types
client := flov1.NewStreamsServiceClient(conn)
```

## Services

- **StreamsService** - Pub/sub messaging (`streams.proto`)
- **WorkQueuesService** - Task queue (`workqueues.proto`)
- **HealthService** - Health checks (`health.proto`)
- **AdminService** - Admin operations (`admin.proto`)

## Development

This submodule is auto-generated from proto files in `../proto/`.

### Regenerate Stubs

```bash
cd ..
make proto
```

### Update Dependencies

```bash
cd api
go mod tidy
```

## Versioning

This module follows semantic versioning independently of the main `flo` module:

- `api/v0.1.0` - Initial release
- `api/v0.2.0` - Breaking changes
- `api/v0.2.1` - Bug fixes

## Releases

Releases are automated via GitHub Actions when you push a tag:

```bash
git tag api/v0.1.0
git push origin api/v0.1.0
```

## Proto Files

Source files are in `../proto/flo/v1/`:
- `streams.proto` - Streams (pub/sub)
- `workqueues.proto` - WorkQueues (task queue)
- `health.proto` - Health checks
- `admin.proto` - Admin operations
- `error.proto` - Error definitions

## License

Apache 2.0 - See [LICENSE](../LICENSE) in the main repository.

