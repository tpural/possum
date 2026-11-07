# Writing a driver

A driver connects POSSUM to something that can switch power: a PoE switch, a smart plug, a PDU,
IPMI, Wake-on-LAN. The core doesn't know any vendor details. It only sees the interface in
[`pkg/power`](../pkg/power/power.go):

```go
type Driver interface {
	Name() string
	State(ctx context.Context, t Target) (State, error)
	PowerOn(ctx context.Context, t Target) error
	PowerOff(ctx context.Context, t Target) error
}
```

- **`Target`** is an opaque string that you define, such as `"<switch-mac>/<port>"`, `"plug-3"` or
  `"10.0.0.50"`. Users write it in the config.
- **`State`** returns `power.StateOn`, `power.StateOff` or `power.StateUnknown`.
- **`PowerOn` and `PowerOff`** must be idempotent and safe for concurrent use. They may return before
  the change takes effect; POSSUM polls `State` until it does.

Return the shared errors so the core can react without knowing your backend:

| Error | Meaning |
|---|---|
| `power.ErrNotFound` | The target doesn't exist. |
| `power.ErrRateLimited` | The backend is throttling; POSSUM retries later. |
| `power.ErrUnsupported` | The target can't be switched, for example a port without PoE. |

Wrap them with `%w`.

## Optional: discovery

If your backend knows which outlet a MAC address is behind, implement `power.Discoverer`. Users can
then leave out `target`, and POSSUM checks the wiring before every shutdown.

```go
type Discoverer interface {
	Discover(ctx context.Context, macs []net.HardwareAddr) (map[string]Target, error)
}
```

## Registering

Register a factory from `init` and add a blank import to `cmd/possum/main.go`:

```go
package myplug

func init() {
	power.Register("myplug", func(raw json.RawMessage) (power.Driver, error) {
		var cfg Config
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		return New(cfg)
	})
}
```

`raw` is the group's `driver.config`. Read secrets from environment variables rather than the
config, so they can come from a Kubernetes Secret.

## Testing

[`drivers/fake`](../drivers/fake/fake.go) is the smallest complete driver and a good template.
[`drivers/unifi`](../drivers/unifi) shows how to test against an `httptest` server that imitates the
real API, including its failure modes.
