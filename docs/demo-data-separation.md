# Demo Data Separation

B77 separates demo/sample behavior from production behavior.

## Runtime Flags

| Variable | Purpose |
| --- | --- |
| `MUFINANCES_MODE` | `development`, `test`, or `production` |
| `MUFINANCES_ALLOW_DEMO_SEED` | Allows demo seed data outside production |
| `MUFINANCES_ALLOW_SAMPLE_LOGINS` | Allows sample users outside production |
| `MUFINANCES_ALLOW_MOCK_CONNECTORS` | Allows mock connector data outside production |
| `MUFINANCES_ALLOW_UNSAFE_DEFAULTS` | Explicit development-only escape hatch |

## Production Blockers

In `production` mode:

- Demo seed data is blocked.
- Sample logins are blocked.
- Mock connectors are blocked.
- Unsafe defaults are blocked.

## Files

- `services/demo_data.py`
- `tests/test_demo_data_guard.py`

## Route And Startup Rule

Startup should call:

```python
DemoDataGuard().assert_production_safe()
```

Routes or scripts that seed data, create sample users, or use mock connector responses must call the matching guard method before doing work.

