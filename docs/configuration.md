# Configuration reference

POSSUM reads one YAML document. The Helm chart builds it from `.Values.config` and adds
`apiVersion` and `kind`. Unknown fields are rejected.

```yaml
apiVersion: possum.dev/v1alpha1
kind: PossumConfig
dryRun: true
timeouts:
  boot: 10m
  shutdown: 5m
  offlineGrace: 20s
  powerSettle: 1m
nodeGroups:
  - name: pi-workers
    minSize: 1
    maxSize: 4
    driver:
      name: unifi
      config: {}
    labels:
      possum.dev/group: pi-workers
    template:
      cpu: "4"
      memory: 8Gi
      pods: 110
      labels:
        kubernetes.io/arch: arm64
    nodes:
      - name: pi-1
        target: "74:ac:b9:00:00:01/1"
      - name: pi-2
        mac: "dc:a6:32:00:00:02"
```

## Top level

| Field | Default | Description |
|---|---|---|
| `dryRun` | `true` | Log and record events only; never change power. |
| `timeouts.boot` | `10m` | How long a powered-on node has to register before it is reported as failed. |
| `timeouts.shutdown` | `5m` | How long the host has to stop answering before the shutdown is aborted. |
| `timeouts.offlineGrace` | `20s` | How long the host must both stop answering and stop renewing its node Lease before its power is cut. |
| `timeouts.powerSettle` | `1m` | How long to wait for the driver to report the new power state. |

## Node groups

| Field | Description |
|---|---|
| `name` | Group name, used as the autoscaler node group ID. |
| `minSize`, `maxSize` | Bounds on powered-on members. `maxSize` cannot exceed the number of nodes. |
| `dryRun` | Overrides the top-level `dryRun` for this group. |
| `timeouts` | Overrides any of the top-level timeouts for this group, e.g. a longer `powerSettle` for Wake-on-LAN. |
| `suspend` | Suspend members to RAM instead of powering them off. The Node stays registered, so waking takes seconds rather than a full boot. Needs a driver that can wake a sleeping host, such as `wol`. |
| `scaleDownUnneededTime` | How long a member must be unneeded before the autoscaler powers it off, overriding the autoscaler's `--scale-down-unneeded-time` for this group (e.g. `45m` for a GPU box you want to linger). |
| `taints` | Taints POSSUM keeps on member Nodes and adds to the scale-up template, so only pods that tolerate them can wake the group. |
| `driver.name` | A registered driver: `unifi`, `wol` or `fake`. |
| `driver.config` | Driver settings, passed to the driver as-is. |
| `labels` | Labels POSSUM applies to member Nodes. |
| `template` | Node shape for scale-up simulation, used only when no member is running to copy from. `template.resources` adds extended resources such as `nvidia.com/gpu`. |
| `nodes[].name` | Node name, as registered by the kubelet. |
| `nodes[].target` | Driver-specific power target. Optional when the driver can discover it. |
| `nodes[].mac` | MAC address used for discovery and verification. Defaults to the agent's `possum.dev/mac` annotation. |

## UniFi driver

```yaml
driver:
  name: unifi
  config:
    url: https://192.168.1.2        # UniFi OS console
    site: default
    insecureSkipVerify: true        # consoles ship self-signed certificates
    cacheTTL: 10s
```

- **Credentials.** They come from `POSSUM_UNIFI_USERNAME` and `POSSUM_UNIFI_PASSWORD`; the chart
  sets these from `credentials`. Create a **local** account on the console with the *Site Admin*
  role for the Network application. Cloud API keys from unifi.ui.com are not accepted by the local
  API.
- **Targets.** The form is `<switch-mac>/<port>`. The driver changes only that port's `poe_mode`
  (`off` or `auto`), and checks that every other port override is unchanged afterwards.
- **Rate limits.** UniFi OS rate-limits logins. The driver keeps one session, logs in again only
  after a 401, and backs off on 429.
- **Network access.** The provider pod needs HTTPS access to the console. On a segmented network,
  allow the cluster subnet to reach the console on port 443.

## Wake-on-LAN driver

```yaml
driver:
  name: wol
  config:
    broadcast: 192.168.1.255:9   # directed broadcast of the target's subnet
    probePort: 22                # TCP port used to tell whether the host is up
    probeTimeout: 2s
nodes:
  - name: gpu-box
    target: "d8:43:ae:24:7c:8a@192.168.1.10"
```

- **Targets.** The form is `<mac>@<host>`. `PowerOn` sends three magic packets. `PowerOff` does nothing,
  because the agent has already shut the host down gracefully.
- **State.** POSSUM infers on/off by probing `host:probePort`. A refused connection counts as on.
- **Network.** Magic packets must originate on the target's network segment. Packets from a pod network are
  normally dropped, so run the provider with `provider.hostNetwork: true` on a node in the same subnet.
- **Machine setup.** Enable Wake-on-LAN in the firmware (on many boards: ErP disabled, wake by PCI-E
  enabled) and in the OS (`ethtool -s <nic> wol g`, or netplan `wakeonlan: true`).
- **Timeouts.** Booting takes longer than PoE, so set a longer group `timeouts.powerSettle` (e.g. `3m`).
- **Suspend.** Wake-on-LAN usually works from suspend-to-RAM too (check `cat /sys/power/mem_sleep` shows
  `[deep]`). With `suspend: true` a wake takes seconds. On NVIDIA hosts, keep `nvidia-suspend` and
  `nvidia-resume` enabled.
