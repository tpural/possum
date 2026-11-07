# Scale-to-zero gate

`possum gate` is a small reverse proxy that lets an HTTP backend, such as llama-swap, scale to zero
replicas when nobody is using it. Combined with a POSSUM node group, the machine behind the backend
can then power off too.

```
client ──▶ possum gate ──▶ backend Service ──▶ backend Deployment (0 or 1 replicas)
```

- **Model listings don't wake anything by default.** `GET /v1/models` and `/models` are answered from the
  llama-swap config file (`--models-config`). UIs like open-webui can list models while the GPU sleeps.
  With `--prewarm-on-models`, a listing still answers at once but also starts waking the backend in the
  background. Opening the UI then gets the machine booting while you type.
- **The first real request wakes the backend.** The gate scales the Deployment to 1, holds the request
  until a replica is ready and reachable (up to `--wake-timeout`), and then proxies it. Concurrent
  requests share one wake. Responses stream, so server-sent events work.
- **Idle backends go back to zero.** After `--idle` with no requests in flight, it scales the
  Deployment to 0.
- **Different clients can keep it awake for different times.** Each `--idle-rule` matches a request
  by host, client address range, or header, and sets how long the backend stays up after that request.
  The first matching rule applies, and requests that match no rule use `--idle`. A request never
  shortens an existing deadline: an agent's short window can't end your longer interactive one.
- **Wake on demand.** `POST /wake?for=2h` on the admin port wakes the backend and keeps it up for that
  long (default `--idle`). Point a CronJob or a phone shortcut at it.
- **Wake the node too.** With `--wake-url`, the gate POSTs to that URL each time it scales up from zero.
  Point it at the POSSUM wake API (`http://possum.possum.svc:8080/api/groups/gpu/wake`) to power the
  node on in parallel with the scale-up, skipping the autoscaler's 30 second wait for new GPU pods.
- **Health, status and metrics** are on a separate port (`--admin-addr`): `/healthz`, `/status`, plus `/metrics` with
  `possum_gate_wakes_total`, `possum_gate_sleeps_total`, `possum_gate_wake_seconds` and
  `possum_gate_inflight_requests`.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--upstream` | | Backend URL, e.g. `http://llama-swap-backend.llama-swap.svc:8080` |
| `--deployment` | | Backend Deployment, as `namespace/name` |
| `--models-config` | | llama-swap `config.yaml`; leave empty to proxy `/v1/models` like any other request |
| `--idle` | `30m` | Scale to zero after this long without requests |
| `--idle-rule` | | Repeatable `<kind> <pattern> <duration>`, where kind is `host`, `cidr` or `header` (`<Name>=<regexp>`) |
| `--wake-timeout` | `15m` | How long a request waits for the backend |
| `--prewarm-on-models` | `0` | Start waking the backend when a client lists models, and keep it up this long; `0` disables |
| `--wake-url` | | URL to POST when scaling up from zero, e.g. the POSSUM wake API |
| `--listen` | `:8080` | Proxy address |
| `--admin-addr` | `:9090` | Health and metrics address |

## Example: interactive vs. agents

Keep the GPU warm for an hour after someone chats in open-webui, but let overnight agents and
pipelines release it after 10 minutes:

```
possum gate \
  --upstream http://llama-swap-backend.llama-swap.svc:8080 \
  --deployment llama-swap/llama-swap \
  --models-config /etc/llama-swap/config.yaml \
  --idle 10m \
  --idle-rule 'host llama-swap.llama-swap.svc.cluster.local 1h'
```

Here open-webui uses the in-cluster name and matches the one-hour rule. Agents call
`llm.lan.example.com` and fall back to `--idle`. `/status` on the admin port shows when the backend may
next sleep.

## Permissions

The gate's ServiceAccount needs `get` and `patch` on `deployments` in the backend's namespace. Leave
`replicas` out of the backend Deployment you deploy with GitOps, so your GitOps tool doesn't fight the gate.

## Waking a node

If the backend's pod can only run on a powered-off node (for example a GPU node with a taint the pod
tolerates), the scale-up leaves it Pending. The Cluster Autoscaler then asks POSSUM to power the node on,
and the gate keeps the request waiting the whole time. Set the client's own timeout above the expected
wake time.
