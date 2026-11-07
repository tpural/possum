# Scale-to-zero gate

`possum gate` is a small reverse proxy that lets an HTTP backend, such as llama-swap, scale to zero
replicas when nobody is using it. Combined with a POSSUM node group, the machine behind the backend
can then power off too.

```
client ──▶ possum gate ──▶ backend Service ──▶ backend Deployment (0 or 1 replicas)
```

- **Model listings never wake anything.** `GET /v1/models` and `/models` are answered from the
  llama-swap config file (`--models-config`). UIs like open-webui can list models while the GPU sleeps.
- **The first real request wakes the backend.** The gate scales the Deployment to 1, holds the request
  until a replica is ready and reachable (up to `--wake-timeout`), and then proxies it. Concurrent
  requests share one wake. Responses stream, so server-sent events work.
- **Idle backends go back to zero.** After `--idle` with no requests in flight, it scales the
  Deployment to 0.
- **Health and metrics** are on a separate port (`--admin-addr`): `/healthz`, plus `/metrics` with
  `possum_gate_wakes_total`, `possum_gate_sleeps_total`, `possum_gate_wake_seconds` and
  `possum_gate_inflight_requests`.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--upstream` | | Backend URL, e.g. `http://llama-swap-backend.llama-swap.svc:8080` |
| `--deployment` | | Backend Deployment, as `namespace/name` |
| `--models-config` | | llama-swap `config.yaml`; leave empty to proxy `/v1/models` like any other request |
| `--idle` | `30m` | Scale to zero after this long without requests |
| `--wake-timeout` | `15m` | How long a request waits for the backend |
| `--listen` | `:8080` | Proxy address |
| `--admin-addr` | `:9090` | Health and metrics address |

## Permissions

The gate's ServiceAccount needs `get` and `patch` on `deployments` in the backend's namespace. Leave
`replicas` out of the backend Deployment you deploy with GitOps, so your GitOps tool doesn't fight the gate.

## Waking a node

If the backend's pod can only run on a powered-off node (for example a GPU node with a taint the pod
tolerates), the scale-up leaves it Pending. The Cluster Autoscaler then asks POSSUM to power the node on,
and the gate keeps the request waiting the whole time. Set the client's own timeout above the expected
wake time.
