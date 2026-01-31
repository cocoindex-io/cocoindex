---
title: Multiple Environments
description: Creating isolated CocoIndex environments for database separation, library development, multi-tenancy, and testing.
---

# Multiple Environments

By default, all CocoIndex apps share the same environment, which manages a shared database and context. For most use cases, this default behavior is sufficient. However, you can create multiple isolated environments when you need:

- **Library development**: Libraries that depend on CocoIndex can use their own environment to avoid sharing state with other libraries or the application
- **Database isolation**: Different apps using separate databases
- **Multi-tenant deployments**: Isolated data per tenant
- **Testing**: Isolated test environments that don't interfere with production

## Creating an Environment

Create an explicit environment by providing `Settings` with a `db_path`:

```python
import cocoindex as coco
import pathlib

env = coco.Environment(coco.Settings.from_env(db_path=pathlib.Path("./my_db.db")))
```

You can optionally name the environment for easier identification:

```python
env = coco.Environment(
    coco.Settings.from_env(db_path=pathlib.Path("./my_db.db")),
    name="production"
)
```

## Associating Apps with an Environment

Pass the environment to `AppConfig` when creating an app:

```python
app = coco.App(
    coco.AppConfig(name="MyApp", environment=env),
    main_fn,
)
```

Apps that don't specify an environment use the default environment (configured via `@coco.lifespan` or the `COCOINDEX_DB` environment variable).

## Example: Multiple Environments

This example creates two apps in different environments, each with its own database:

```python
import cocoindex as coco
import pathlib

# Create two environments with separate databases
env1 = coco.Environment(coco.Settings.from_env(db_path=pathlib.Path("./db1/cocoindex.db")))
env2 = coco.Environment(coco.Settings.from_env(db_path=pathlib.Path("./db2/cocoindex.db")))

@coco.function
def build1() -> None:
    # ... pipeline logic for env1 ...
    pass

@coco.function
def build2() -> None:
    # ... pipeline logic for env2 ...
    pass

# Apps in different environments
app1 = coco.App(coco.AppConfig(name="App1", environment=env1), build1)
app2 = coco.App(coco.AppConfig(name="App2", environment=env2), build2)
```

## Same App Name in Different Environments

Apps with the same name can coexist in different environments. This is useful for multi-tenant scenarios or running the same pipeline against different databases:

```python
import cocoindex as coco
import pathlib
from typing import Iterator

# Named environment for "alpha" tenant
env_alpha = coco.Environment(
    coco.Settings.from_env(db_path=pathlib.Path("./alpha/cocoindex.db")),
    name="alpha"
)

# Default environment via lifespan
@coco.lifespan
def _lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./default/cocoindex.db")
    yield

@coco.function
def build() -> None:
    # ... pipeline logic ...
    pass

# Same app name, different environments
app_alpha = coco.App(coco.AppConfig(name="MyApp", environment=env_alpha), build)
app_default = coco.App("MyApp", build)  # Uses default environment
```

## CLI Support

When working with multiple environments, the CLI groups apps by their environment. Use the `@env_name` syntax to target a specific app:

```bash
# List all apps grouped by environment
cocoindex ls ./multi_env.py

# Update a specific app in a named environment
cocoindex update ./multi_env.py:MyApp@alpha

# Update the app in the default environment
cocoindex update ./multi_env.py:MyApp@default
```

## Testing with Isolated Environments

For tests, create isolated environments to avoid interference between test runs:

```python
import cocoindex as coco
import pathlib
import tempfile

def create_test_env(test_name: str) -> coco.Environment:
    db_path = pathlib.Path(tempfile.mkdtemp()) / f"{test_name}.db"
    return coco.Environment(coco.Settings.from_env(db_path=db_path))

# In your test
def test_my_pipeline():
    env = create_test_env("test_my_pipeline")
    app = coco.App(
        coco.AppConfig(name="TestApp", environment=env),
        my_main_fn,
    )
    app.update()
    # ... assertions ...
```

## When to Use Multiple Environments

| Use Case | Approach |
|----------|----------|
| Single app, single database | Default environment (no explicit environment needed) |
| Multiple apps sharing state | Default environment |
| Apps needing separate databases | Explicit environments with different `db_path` |
| Multi-tenant isolation | Named environments per tenant |
| Testing | Temporary isolated environments |
