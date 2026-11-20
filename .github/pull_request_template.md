## Summary

-

## Validation

- [ ] `uv sync --frozen --all-groups` succeeds.
- [ ] `uv run pytest -q` passes.
- [ ] CLI help and version checks pass.
- [ ] No secrets, recordings, machine paths, or private repository URLs are included.
- [ ] License notices are updated when dependencies or bundled resources change.

## Deployment and safety

- [ ] The change does not weaken the default loopback-only network boundary.
- [ ] Any physical robot validation used independent safety controls and an operator-accessible emergency stop.

Describe deployment, network exposure, and physical validation performed:
