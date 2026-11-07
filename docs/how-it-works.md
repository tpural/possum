# How it works

POSSUM has three parts:

| Component | Runs as | Job |
|---|---|---|
| Cluster Autoscaler | Deployment | Decides when to add or remove nodes |
| `possum provider` | Deployment, one replica | Implements the externalgrpc cloud provider and drives power |
| `possum agent` | DaemonSet on managed nodes | Powers its own host off gracefully when asked |

## Node lifecycle

```
          IncreaseSize                 node registers
   Off ───────────────▶ Booting ─────────────────────▶ Running
    ▲                                                     │
    │      power off, delete Node      DeleteNodes        │
    └──────────────────── ShuttingDown ◀──────────────────┘
```

Each member is in one phase at a time:

- **Off**: the driver reports no power and no Node object exists.
- **Booting**: power is on but the node hasn't registered yet. It is reported to the autoscaler as
  `instanceCreating`. If it doesn't register within `timeouts.boot`, it is reported with an error,
  and the autoscaler may give up on it and ask POSSUM to delete it.
- **Running**: a Node object exists. POSSUM sets `spec.providerID` to `possum://<group>/<node>`
  (only when it is empty) and applies the group's `labels`.
- **ShuttingDown**: the shutdown sequence below is in progress.
- **Unknown**: POSSUM won't act on this member, for example because it is a control-plane node, has
  someone else's providerID, or has no known power target.

## Shutdown sequence

1. If the host still answers, and its MAC is known, and the driver supports discovery, check that
   the node is still plugged into the configured target.
2. Set the annotation `possum.dev/shutdown=<RFC 3339 time>` on the Node. This happens every time,
   even when the host already looks down, because a network problem can look like a dead host.
3. The agent sees the annotation and calls `org.freedesktop.login1.Manager.PowerOff` on the host's
   system D-Bus. It ignores requests timestamped before the host booted, so a leftover annotation
   can't shut down a freshly started node.
4. The provider waits until **both** of these signals have held for `timeouts.offlineGrace`:
   - the kubelet port stops answering (a refused connection counts as *alive*, since the OS is still
     up);
   - the node's Lease in `kube-node-lease` stops being renewed.

   A network problem between the provider and the node can fake the first signal but not the
   second.
5. Power off through the driver, then poll until the driver reports `off`.
6. Remove the annotation and delete the Node object.

If step 4 doesn't finish within `timeouts.shutdown`, the annotation is removed, power stays on, and
a `ShutdownAborted` event is recorded. If step 6 fails after power is already off, the member stays
`ShuttingDown` and the next reconcile retries. Every step is safe to repeat.

Nodes that were powered on but never registered have nothing to shut down. The autoscaler reports
them by instance ID (`possum://<group>/<node>`), and POSSUM powers them off directly.

## Restarts

Member phases, targets discovered at runtime, and MACs are stored in the `possum-state` ConfigMap.
After a restart, members that were `ShuttingDown` resume the sequence. The gRPC API answers
`Unavailable` until the first reconcile has finished, so the autoscaler never sees a half-loaded
fleet.

## Target discovery

The agent writes the MAC of the interface holding the node's InternalIP to the annotation
`possum.dev/mac`. For members configured without a `target`, the provider asks the driver which
port that MAC is connected to, and remembers the answer. A node has to have run once with the agent
before POSSUM can power it off.

## Observability

- Events on the Node: `PoweredOn`, `PoweredOff`, `ShutdownRequested`, `ShutdownAborted`,
  `ShutdownIncomplete`, `DryRunPowerOn`, `DryRunPowerOff`.
- Prometheus metrics on `:8080/metrics`:
  - `possum_nodes{group,phase}`
  - `possum_power_actions_total{group,state}`
  - `possum_driver_errors_total{driver}`
- `:8080/` serves a status page and `:8080/api/status` the same data as JSON. `/healthz` and
  `/readyz` are for probes.

## Things to know before enabling scale-down

- **System pods.** By default the autoscaler won't remove a node running `kube-system` pods that
  aren't DaemonSets, such as CoreDNS or metrics-server. Either keep those pods off the managed
  nodes, give them PodDisruptionBudgets, or set `autoscaler.args.skip-nodes-with-system-pods: "false"`.
- **Access.** The chart's NetworkPolicy lets only the bundled autoscaler reach the gRPC port. The
  status port (8080) is read-only and stays open.
- **Agent privileges.** The agent runs as root with only `CAP_SYS_BOOT`, which logind requires to
  power off without polkit. It uses host networking so it can find the node's MAC address.
