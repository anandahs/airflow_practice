## Flower (Celery monitoring UI)

Flower is disabled by default. Start it with the `flower` profile:

```bash
docker compose --profile flower up -d
```

Then open: http://localhost:5555

To bring everything down, including flower, use the same profile flag:

```bash
docker compose --profile flower down
```
