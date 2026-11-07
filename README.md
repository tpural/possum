<p align="center">
  <img src="docs/images/possum.png" alt="POSSUM" width="600">
</p>

<p align="center"><b>Down, but not out.</b></p>

---

POSSUM powers idle Kubernetes nodes off and wakes them when pods need room. Picture a Raspberry Pi
cluster where spare Pis sit without PoE power until there's work for them.

It plugs into the [Cluster Autoscaler](https://github.com/kubernetes/autoscaler) as an
[externalgrpc](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler/cloudprovider/externalgrpc)
cloud provider. The autoscaler decides *when*; POSSUM decides *how*, through a pluggable power
driver. The first driver switches **PoE ports on UniFi switches**.

```
 pending pods ──▶ Cluster Autoscaler ──gRPC──▶ POSSUM provider ──▶ power driver (UniFi PoE, …)
                                                     │
 idle node   ◀── possum agent: graceful poweroff ◀───┘
```

## How it works

**Scale up.** The autoscaler sees unschedulable pods and asks POSSUM for more nodes. POSSUM picks a
member that is off and turns its power on. The machine boots, the kubelet registers, and the
pods schedule.

**Scale down.** The autoscaler drains an underused node and asks POSSUM to delete it. POSSUM then:

1. annotates the Node, and the agent on that node asks systemd to power off;
2. waits until the host stops answering;
3. cuts power, checks the port is off, and deletes the Node object.

If the host never goes quiet, POSSUM aborts and leaves the power on.

Measured on a Raspberry Pi 4 (network boot) on a UniFi US-16-PoE-150W:

| | |
|---|---|
| PoE on → ping | 54 s |
| PoE on → node Ready | 62 s |
| PoE on → pods running | ~2 min |
| Power saved per sleeping Pi | ~5 W |

## Safety

- **Dry run by default.** POSSUM records what it *would* do and changes nothing until you set
  `dryRun: false`. The chart also turns autoscaler scale-down off while in dry run.
- **Only listed nodes.** Members are an explicit allowlist. Control-plane nodes are always refused.
- **Never cut power under a live host.** Power is removed only after the host has stopped
  answering for a grace period.
- **Verified targets.** When a node's MAC is known, its switch port is re-checked before every
  shutdown. Driver writes change only the target port, and the driver checks that afterwards.
- **One power action at a time.** Progress is saved in a ConfigMap, so a restart resumes where it
  stopped.

## Quick start

```sh
helm install possum oci://ghcr.io/tpural/charts/possum -n possum --create-namespace -f values.yaml
```

```yaml
# values.yaml
config:
  dryRun: true
  nodeGroups:
    - name: pi-workers
      minSize: 1
      maxSize: 4
      driver:
        name: unifi
        config:
          url: https://192.168.1.2
          insecureSkipVerify: true
      nodes:
        - name: pi-1
          target: "74:ac:b9:00:00:01/1"
        - name: pi-2
          target: "74:ac:b9:00:00:01/2"
        - name: pi-3        # target discovered from the node's MAC
        - name: pi-4
credentials:
  existingSecret: possum-unifi   # keys: username, password
```

Each release also attaches an `install.yaml` for clusters without Helm.

Open the status page with `kubectl -n possum port-forward svc/possum 8080`. Once the dry-run
decisions look right, set `dryRun: false`.

## Drivers

| Driver | Switches | Target format |
|---|---|---|
| `unifi` | PoE ports through a UniFi OS console (Cloud Key, UDM, UCG) | `<switch-mac>/<port>` |
| `fake` | in-memory, for tests and demos | any string |

Writing a driver takes one Go package with four methods. See [docs/drivers.md](docs/drivers.md).

## Documentation

- [How it works](docs/how-it-works.md)
- [Configuration reference](docs/configuration.md)
- [Writing a driver](docs/drivers.md)

## License

Apache-2.0
