***

# 📘 Arquitetura Assíncrona, Escalabilidade e Backpressure (Consumidor Sobrecarregado)

***

## 🧠 🎯 Objetivo da Fase

Nesta fase você evoluiu de uma aplicação simples para um sistema **orientado a eventos**, capaz de:

*   processar grandes volumes
*   desacoplar responsabilidades
*   absorver picos de carga
*   escalar horizontalmente

***

# 🏗️ 🔗 Visão Geral da Arquitetura

```text
┌──────────────┐
│  Controller  │
│ (HTTP API)   │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ OrderService │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Database   │ (PostgreSQL)
│   (orders)   │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Publisher   │
│ (RabbitMQ)   │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ payment.queue│
└──────┬───────┘
       ▼
┌──────────────┐
│ PaymentConsumer │
└──────┬────────┘
       ▼
┌──────────────┐
│ email.queue  │
└──────┬───────┘
       ▼
┌──────────────┐
│ EmailConsumer│
└──────────────┘
```

***

# 🚀 🔥 O QUE FOI IMPLEMENTADO

Ao executar o endpoint `/stress`:

```text
POST /orders/stress
```

O sistema:

✅ cria **1000 pedidos**
✅ persiste no banco
✅ publica **1000 eventos**
✅ envia **1000 mensagens ao RabbitMQ**

***

# ⚙️ ✅ PASSO 1 — Disparar carga

```bash
POST http://localhost:8080/orders/stress
```

Resultado:

👉 sistema gera carga massiva simulando um cenário real de produção.

***

# 🐳 ✅ PASSO 2 — Subir infraestrutura

```bash
docker compose up -d rabbitmq postgres
```

***

# 🌐 ✅ PASSO 3 — Acessar RabbitMQ

```text
http://localhost:15672
```

Login:

```text
guest / guest
```

***

# 📊 🔍 O QUE OBSERVAR

## 🔹 Queues

```text
payment.queue
email.queue
```

***

## 📈 Métricas importantes

### ✅ Ready

Mensagens aguardando processamento

***

### ✅ Unacked

Mensagens em processamento pelos consumers

***

### ✅ Total

Total geral (Ready + Unacked)

***

# 🎯 📊 CENÁRIO ESPERADO

Ao disparar 1000 mensagens:

```text
Ready: 1000
```

Depois:

```text
Ready: 700
Ready: 200
Ready: 0
```

👉 Isso mostra a fila sendo drenada.

***

# 🧠 ✅ O QUE ISSO PROVA

O sistema está:

✅ desacoplado
✅ assíncrono
✅ resiliente
✅ escalável

***

# 🐢 ✅ PASSO 4 — Simular gargalo (backpressure)

## 📂 PaymentConsumer

```java
Thread.sleep(2000);
```

***

## 🔥 Impacto

Cada mensagem leva:

```text
2 segundos
```

***

👉 Resultado:

```text
Fila cresce
Ready ↑
Unacked ↑
```

***

# 🧠 💡 CONCEITO: BACKPRESSURE

👉 Ocorre quando:

```text
Producer > Consumer
```

👉 Resultado:

*   acúmulo na fila
*   pressão controlada pelo RabbitMQ

***

# 🔥 O QUE VOCÊ OBSERVA

✅ API continua rápida
✅ Rabbit segura a carga
✅ consumers ficam ocupados
✅ sistema NÃO trava

***

# ⚡ ✅ PASSO 5 — Escalar consumers

## Configuração

```yaml
spring:
  rabbitmq:
    listener:
      simple:
        concurrency: 5
        max-concurrency: 10
```

OU (melhor prática):

```java
@RabbitListener(concurrency = "5-10")
```

***

# 🧠 O QUE ISSO FAZ

Antes:

```text
1 consumer
```

Depois:

```text
5 a 10 consumers paralelos
```

***

# 📈 RESULTADO

Antes:

```text
1 msg / 2s
```

Depois:

```text
5 msgs / 2s
```

***

# 🚀 💡 CONCEITO: ESCALABILIDADE HORIZONTAL

👉 aumentar o número de consumidores → aumenta a capacidade de processamento

***

# 📊 ✅ PASSO 6 — Monitoramento

## 🔍 RabbitMQ

Observe:

*   crescimento de fila
*   taxa de consumo
*   unacked
*   redelivery

***

## 🧾 Logs

```text
Thread-1 processando pedido
Thread-2 processando pedido
Thread-3 processando pedido
```

👉 múltiplos consumers trabalhando em paralelo

***

## 🗄️ PostgreSQL

*   inserts em massa
*   concorrência de escrita

***

# 🧠 🧩 CONCEITOS APRENDIDOS

***

## ✅ 1. Processamento assíncrono

API não espera processamento:

```text
Recebe → responde rápido → processa depois
```

***

## ✅ 2. Event-Driven Architecture (EDA)

Comunicação baseada em eventos:

```text
OrderCreated → Payment → Email
```

***

## ✅ 3. Throughput

Quantidade de mensagens processadas por segundo.

***

## ✅ 4. Backpressure

Sistema segurando carga quando não consegue processar tudo imediatamente.

***

## ✅ 5. Escalabilidade horizontal

Mais consumers = mais capacidade.

***

## ✅ 6. Elasticidade (max-concurrency)

Sistema escala automaticamente sob demanda.

***

## ✅ 7. Resiliência

Mesmo com carga:

✅ sistema não trava
✅ não perde mensagens

***

# ⚠️ ⚙️ PROBLEMAS REAIS IDENTIFICADOS

Durante os testes você observou:

❌ fila crescendo
❌ apenas 1 consumer
❌ baixa taxa de processamento

***

## ✅ Correção aplicada

```java
@RabbitListener(concurrency = "5-10")
```

***

👉 Resultado:

✅ mais consumers
✅ maior throughput
✅ fila drenando mais rápido

***

# 🧠 💡 INSIGHTS IMPORTANTES

***

## 🔥 1. RabbitMQ não resolve tudo

Ele apenas:

👉 segura pressão

Mas quem resolve:

👉 são os consumers

***

## 🔥 2. Concorrência é arma poderosa (e perigosa)

Mais consumers:

✅ aumenta performance

Mas também:

⚠️ aumenta concorrência
⚠️ pode gerar race condition
⚠️ pode sobrecarregar banco

***

## 🔥 3. Sistemas reais lidam com isso o tempo todo

Esse cenário replica:

*   e-commerce
*   sistemas financeiros
*   plataformas de streaming
*   sistemas de mensageria massiva

***

# 🧠 📌 CONCLUSÃO FINAL

Nesta fase você construiu um sistema que:

✅ escala
✅ desacopla
✅ suporta alta carga
✅ processa de forma assíncrona

***
