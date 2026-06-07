# CLAUDE.md

## Running PHP

Never run PHP, Composer, or `bin/console` on the host. Every PHP process runs in
the `php` service via ephemeral containers:

```bash
docker compose run --rm php php bin/console <command>
docker compose run --rm php composer <script>
```

## Validation

All work must pass these three gates before it is considered done:

```bash
docker compose run --rm php composer ecs       # coding standard
docker compose run --rm php composer phpstan   # static analysis
docker compose run --rm php composer test      # phpunit
```

Run all three after any change. Fix violations rather than suppressing them.
