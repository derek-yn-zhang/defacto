# Defacto

An entity engine that turns operational events into temporal, queryable state.

## Install

```bash
pip install defacto
```

## Usage

```python
from defacto import Defacto

d = Defacto("definitions/")
d.ingest("app", events, process=True)

d.table("customer").execute()                        # current state
d.history("customer").as_of("2024-01-15").execute()  # point-in-time
d.history("customer").execute()                      # full history
```

## Examples

See [`examples/quickstart/`](examples/quickstart) and [`examples/showcase/`](examples/showcase).
