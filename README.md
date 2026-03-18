# athena-capacity-reservation

A CLI tool for managing AWS Athena Capacity Reservations, with autoscale monitoring based on DPU utilization.

## Features

- **Activate / Deactivate** Athena Capacity Reservations, with polling until `ACTIVE`
- **Autoscale monitor** — adjusts DPU count in real-time based on CloudWatch utilization metrics
- **Daemon mode** — forks the monitor to the background so your queries can proceed immediately
- **Slack notifications** — posts scale events to Slack

## Installation

```bash
pip install athena-capacity-reservation
```

With Slack notification support:

```bash
pip install "athena-capacity-reservation[slack]"
```

## Usage

```
athena-capacity-reservation activate
athena-capacity-reservation deactivate
athena-capacity-reservation monitor start [--daemon] [--state-file FILE] [--pid-file FILE]
athena-capacity-reservation monitor stop  [--pid-file FILE]
athena-capacity-reservation start          [--daemon] [--state-file FILE] [--pid-file FILE]
athena-capacity-reservation stop           [--state-file FILE] [--pid-file FILE]
```

All environment variables can also be passed as CLI arguments. CLI arguments take precedence over environment variables. For example:

```bash
athena-capacity-reservation start \
  --reservation-name my-reservation \
  --workgroup-names primary,secondary \
  --dpus 8 \
  --min-dpus 8 --max-dpus 64 \
  --daemon --log-file /tmp/monitor.log
```

Run `athena-capacity-reservation <subcommand> --help` for the full argument list per subcommand.

### Example CI pipeline

Here's an example of codebuild.

```yaml
phases:
  build:
    commands:
      - athena-capacity-reservation start --daemon
      - # Run Athena queries
  post_build:
    finally:
      - athena-capacity-reservation stop
```

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed sequence diagrams.

## Fallback Strategies

See [docs/fallback.md](docs/fallback.md) for recommended cleanup patterns to prevent orphaned reservations.

## Configuration

### Required environment variables

| Variable | Description |
|---|---|
| `ATHENA_CR_RESERVATION_NAME` | Name of the Athena Capacity Reservation |
| `ATHENA_CR_WORKGROUP_NAMES` | Comma-separated list of Athena workgroup names |
| `ATHENA_CR_DPUS` | Initial DPU count when activating the reservation (positive integer) |

### Optional environment variables

| Variable | Default | Description |
|---|---|---|
| `ATHENA_CR_MIN_DPUS` | `ATHENA_CR_DPUS` | Scale-in lower bound (positive integer) |
| `ATHENA_CR_MAX_DPUS` | `ATHENA_CR_MIN_DPUS` (no autoscaling) | Scale-out upper bound (positive integer) |
| `ATHENA_CR_SCALE_STEP_DPUS` | `8` | DPU step per scale event (positive integer) |
| `ATHENA_CR_SCALE_OUT_THRESHOLD` | `80` | Utilization % to trigger scale-out (`0 < value < 100`) |
| `ATHENA_CR_SCALE_IN_THRESHOLD` | `30` | Utilization % to trigger scale-in (`0 < value < 100`) |
| `ATHENA_CR_MONITOR_INTERVAL` | `60` | Poll interval in seconds (positive integer) |
| `ATHENA_CR_COOLDOWN_SECONDS` | `300` | Minimum seconds between scale events (positive integer) |
| `ATHENA_CR_QUEUED_TICKS_FOR_SCALE_OUT` | `2` | Consecutive ticks with queued queries before scale-out (positive integer) |
| `ATHENA_CR_LOW_TICKS_FOR_SCALE_IN` | `2` | Consecutive ticks below scale-in threshold before scale-in (positive integer) |
| `ATHENA_CR_SLACK_CHANNEL` | — | Slack channel ID for notifications (falls back to `SLACK_CHANNEL`) |
| `ATHENA_CR_SLACK_TOKEN` | — | Slack API token (falls back to `SLACK_TOKEN`) |
| `ATHENA_CR_SLACK_STATE_FILE` | `<tempdir>/slack_state.json` | Path to JSON file storing Slack notification state |
| `ATHENA_CR_CAPACITY_PID_FILE` | `<tempdir>/capacity_monitor.pid` | Path to PID file for the background monitor |

### Autoscaling example

To enable autoscaling, set both `ATHENA_CR_MIN_DPUS` and `ATHENA_CR_MAX_DPUS`:

```bash
ATHENA_CR_DPUS=8        # initial DPU count on activate
ATHENA_CR_MIN_DPUS=8    # scale-in floor
ATHENA_CR_MAX_DPUS=64   # scale-out ceiling
```

If `ATHENA_CR_MAX_DPUS` is not set, the monitor runs with a fixed DPU count (no autoscaling).

`ATHENA_CR_DPUS` must be within `[MIN_DPUS, MAX_DPUS]` when both are set.

## Required AWS Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ManageCapacityReservation",
      "Effect": "Allow",
      "Action": [
        "athena:GetCapacityReservation",
        "athena:CreateCapacityReservation",
        "athena:UpdateCapacityReservation",
        "athena:CancelCapacityReservation",
        "athena:DeleteCapacityReservation"
      ],
      "Resource": "arn:aws:athena:<region>:<account-id>:capacity-reservation/<reservation-name>"
    },
    {
      "Sid": "AssignWorkgroupsToReservation",
      "Effect": "Allow",
      "Action": [
        "athena:PutCapacityAssignmentConfiguration"
      ],
      "Resource": [
        "arn:aws:athena:<region>:<account-id>:capacity-reservation/<reservation-name>",
        "arn:aws:athena:<region>:<account-id>:workgroup/<workgroup-name>"
      ]
    },
    {
      "Sid": "DetectQueuedQueries",
      "Effect": "Allow",
      "Action": [
        "athena:ListQueryExecutions",
        "athena:BatchGetQueryExecution"
      ],
      "Resource": "arn:aws:athena:<region>:<account-id>:workgroup/<workgroup-name>"
    },
    {
      "Sid": "FetchDpuUtilizationMetrics",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricData"
      ],
      "Resource": "*"
    }
  ]
}
```

- `ManageCapacityReservation` — create, update, cancel, and delete the reservation
- `AssignWorkgroupsToReservation` — bind workgroups to the reservation
- `DetectQueuedQueries` — detect queued queries for scale-out decisions (autoscale monitor only)
- `FetchDpuUtilizationMetrics` — fetch DPU utilization from CloudWatch (autoscale monitor only, `Resource: "*"` is required because CloudWatch `GetMetricData` does not support resource-level permissions)

## Development

```bash
pip install -e ".[dev,slack]"
pytest
```

## License

MIT
