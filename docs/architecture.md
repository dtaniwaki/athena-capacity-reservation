# Athena Capacity Reservation Management

## Sequence Diagrams

### Normal Flow (activate → build → deactivate)

```mermaid
sequenceDiagram
    participant BP as Build Process
    participant CA as athena-capacity-reservation
    participant AT as Athena API
    participant SL as Slack

    Note over BP: build start
    BP->>CA: athena-capacity-reservation start --daemon
    Note over CA: cmd_activate()
    Note over CA: DPU count from ATHENA_CR_DPUS
    CA->>AT: create/update reservation
    AT-->>CA: OK
    loop poll (30s interval, up to 10min)
        CA->>AT: get_capacity_reservation
        AT-->>CA: status
    end
    CA->>SL: ⚡ Reservation activated (8 DPUs)
    Note over CA: fork → background monitor starts

    BP->>BP: build commands

    Note over BP: build end
    BP->>CA: athena-capacity-reservation stop
    Note over CA: SIGTERM → cmd_deactivate()
    CA->>AT: cancel_capacity_reservation
    AT-->>CA: OK
    CA->>SL: 🔋 Reservation deactivated
```

### Autoscale Flow (DPU adjustment during build)

The capacity monitor runs in the background during the build phase, polling CloudWatch DPU utilization and scaling out/in accordingly.

```mermaid
sequenceDiagram
    participant CA as athena-capacity-reservation (background)
    participant CW as CloudWatch
    participant AT as Athena API
    participant SL as Slack

    Note over CA: started by athena-capacity-reservation start

    loop poll (60s interval)
        CA->>CW: GetMetricData(DPUAllocated, DPUConsumed)
        CW-->>CA: utilization %
    end

    Note over CA: utilization > 80% detected (outside cooldown)
    CA->>AT: get_capacity_reservation
    AT-->>CA: current_dpus=24
    Note over CA: target = min(24+8, 120) = 32
    CA->>AT: update_capacity_reservation(target_dpus=32)
    AT-->>CA: OK
    CA->>SL: 📈 Athena DPU scale-out: 24→32

    Note over CA: utilization < 30% detected (outside cooldown)
    CA->>AT: get_capacity_reservation
    AT-->>CA: current_dpus=32
    Note over CA: target = max(32-8, 8) = 24
    CA->>AT: update_capacity_reservation(target_dpus=24)
    AT-->>CA: OK
    CA->>SL: 📉 Athena DPU scale-in: 32→24

    Note over CA: SIGTERM received (athena-capacity-reservation stop)
```
