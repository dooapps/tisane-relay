# tisane-relay

## Gateway

Depois de qualquer `gcloud run deploy`, rode `scripts/update_gateway.sh`.

Observação: confirme a região do gateway com:

```
gcloud api-gateway gateways list --format="table(name,location,state)"
```

Se a coluna `location` mostrar `global`, atualize `REGION="global"` no script `scripts/update_gateway.sh`.
