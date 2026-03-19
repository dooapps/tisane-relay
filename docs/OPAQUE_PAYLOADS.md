# Opaque Payloads

## Objetivo

`tisane-relay` e transporte assinado.
Ele nao define schema de dominio para consumidores como `Mellis`.

## Regra

- `payload_json` pode carregar qualquer payload valido e assinado
- a interpretacao do payload pertence ao consumidor
- dados sensiveis devem atravessar protegidos por `infusion_ffi`
- `tisane-relay` nao conhece nem valida shape financeiro, fiscal ou editorial

## Aplicacao pratica

Se uma aplicacao usuaria quiser enviar dados para a `Mellis`:

- a origem funcional monta o payload
- `tisane` cuida da trilha e classificacao
- `infusion_ffi` protege material sensivel
- `tisane-relay` entrega o evento assinado
- `Mellis` valida o schema que ela mesma define

## O que isso evita

- acoplamento do relay a um dominio financeiro especifico
- vazamento de PII por schema acidental
- reuso ruim do relay como repositorio de regras de negocio
