# Decisões Arquiteturais: Saga Orquestrada no Fluxo de Compra

Documento técnico que acompanha o [README](../README.md). Aqui ficam as decisões, os riscos que elas mitigam e as estratégias de resiliência da Saga Pedido → Estoque → Pagamento.

## Sumário

1. [Registro de decisões (ADRs)](#1-registro-de-decisões-adrs)
2. [Contratos de comunicação](#2-contratos-de-comunicação)
3. [Modelo de dados de controle](#3-modelo-de-dados-de-controle)
4. [Matriz de decisão: idempotência e mensagens duplicadas](#4-matriz-de-decisão-idempotência-e-mensagens-duplicadas)
5. [Timeouts de rede e a tabela de estado da saga](#5-timeouts-de-rede-e-a-tabela-de-estado-da-saga)
6. [Dead-Letter Queues](#6-dead-letter-queues)
7. [Retry e Circuit Breaker](#7-retry-e-circuit-breaker)
8. [Riscos mitigados e riscos residuais](#8-riscos-mitigados-e-riscos-residuais)
9. [Observabilidade](#9-observabilidade)

---

## 1. Registro de decisões (ADRs)

### ADR-001: Orquestração em vez de Coreografia

- Status: aceita.
- Contexto: três domínios, uma etapa de resultado incerto (o gateway externo), compensações que importam na ordem e necessidade de auditoria financeira.
- Decisão: um Saga Orchestrator guarda o estado e comanda os participantes por mensagens de comando e resposta.
- Consequências: ganho-se fluxo explícito, estado consultável, timeouts centralizados e participantes simples, sem dependência entre si. Em troca, passa a existir um componente a mais para operar, e há o risco de concentrar a lógica de fluxo. Esse último é contido mantendo no orquestrador apenas as transições de estado, nunca a regra de negócio dos domínios.

### ADR-002: Mensageria assíncrona entre serviços

- Decisão: comandos e respostas trafegam por broker (Kafka ou RabbitMQ). A chamada síncrona fica só na borda, do cliente para o Order e do Payment para o gateway externo.
- Motivo: desacoplamento temporal. Se o Inventory cair, a mensagem espera na fila em vez de derrubar toda a cadeia de chamadas síncronas.
- Consequência: a entrega é at-least-once. Duplicata é esperada, e por isso a ADR-004 deixa de ser opcional.

### ADR-003: Transactional Outbox

- Decisão: a mudança de estado de negócio e a mensagem a publicar vão para a mesma transação local, numa tabela `outbox`. Um relay (ou CDC, via Debezium) publica no broker depois.
- Motivo: evitar o dual write. Gravar no banco e falhar ao publicar, ou o inverso, deixaria a saga travada ou inconsistente.

### ADR-004: Idempotent Consumer em todos os participantes

- Decisão: todo consumidor registra o `message_id` que processou em `processed_message`, na mesma transação do efeito de negócio.
- Motivo: o broker reentrega. Sem esse registro, um `ReserveStock` duplicado reservaria o estoque duas vezes.

### ADR-005: Compensações semânticas, idempotentes e sem falha definitiva

- Decisão: cada transação tem a sua compensação declarada (tabela abaixo). Compensações são idempotentes e podem ser retentadas por tempo indeterminado, sempre com alerta.
- Motivo: se T3 falha e a compensação C2 também falha de vez, o estoque fica preso. Então falha de compensação é tratada como incidente (DLQ mais alerta), e não como um estado final aceitável.

| Transação | Compensação | Observação |
|---|---|---|
| T1 · `CreateOrder` | C1 · `CancelOrder` | Pedido vai a `CANCELLED`; não é apagado, por auditoria |
| T2 · `ReserveStock` | C2 · `ReleaseStock` | Devolve a quantidade reservada ao disponível |
| T3 · `ProcessPayment` | C3 · `CancelPayment` / `Refund` | Só quando o pagamento pode ter ocorrido, como no timeout |
| T4 · `ConfirmStock` | — | É o passo depois do ponto sem retorno; falha aqui é retentada, não compensada |

Há um ponto sem retorno nesse fluxo, a chamada pivot transaction: a aprovação do pagamento. Depois dela a saga só avança, com retry até concluir. Antes dela, ainda dá para reverter.

### ADR-006: Pagamento "desconhecido" não libera o estoque antes da confirmação

- Decisão: num timeout do gateway, o pagamento entra em `UNKNOWN`. O orquestrador só compensa o estoque depois de receber `PaymentCancelled`, que vem de uma consulta de status e, se preciso, de um estorno.
- Motivo: liberar o estoque e só então descobrir que o cartão foi cobrado produz o pior resultado possível, um cliente cobrado sem pedido.

---

## 2. Contratos de comunicação

### Envelope padrão de toda mensagem

```json
{
  "messageId": "b1f0a0e2-6a4e-4c1a-9a3b-1b6e1d2f7a10",
  "sagaId":    "5d3c0e1a-33a8-4a0e-8a52-0c9bb1f04c11",
  "type":      "ReserveStock",
  "occurredAt":"2025-01-15T14:03:22Z",
  "causationId":"…id da mensagem que causou esta…",
  "payload":   { "orderId": "…", "items": [ { "sku": "ABC-1", "qty": 2 } ] }
}
```

O `messageId` identifica a mensagem e é a base da deduplicação. O `sagaId` correlaciona tudo de uma mesma saga, e também serve de chave de partição do broker, o que preserva a ordem dentro de cada saga. O `causationId` deixa reconstruir a cadeia causal na hora de depurar.

### Catálogo de mensagens

| Tipo | Mensagem | Origem → Destino | Quando |
|---|---|---|---|
| Evento | `OrderCreated` | Order → Orchestrator | Pedido criado |
| Comando | `ReserveStock` | Orchestrator → Inventory | Início da reserva |
| Resposta | `StockReserved` / `StockRejected` | Inventory → Orchestrator | Resultado da reserva |
| Comando | `ProcessPayment` | Orchestrator → Payment | Após reserva |
| Resposta | `PaymentApproved` / `PaymentRefused` | Payment → Orchestrator | Resultado da cobrança |
| Comando | `ConfirmStock` | Orchestrator → Inventory | Após pagamento aprovado |
| Resposta | `StockConfirmed` | Inventory → Orchestrator | Reserva virou baixa |
| Comando | `ApproveOrder` | Orchestrator → Order | Fim do caminho de sucesso |
| Resposta | `OrderApproved` | Order → Orchestrator | Saga `COMPLETED` |
| Comando (compensação) | `ReleaseStock` | Orchestrator → Inventory | Reverte T2 |
| Resposta | `StockReleased` | Inventory → Orchestrator | Reserva devolvida |
| Comando (compensação) | `CancelOrder` | Orchestrator → Order | Reverte T1 |
| Resposta | `OrderCancelled` | Order → Orchestrator | Saga `CANCELLED` |
| Comando (compensação) | `CancelPayment` | Orchestrator → Payment | Pagamento incerto (timeout) |
| Resposta | `PaymentCancelled` | Payment → Orchestrator | Cobrança inexistente ou estornada |

### Contrato síncrono de borda

| Chamada | Contrato |
|---|---|
| `POST /orders` | Header `Idempotency-Key` obrigatório. Responde `202 Accepted {orderId, sagaId}`. A mesma chave devolve a mesma resposta, sem criar um segundo pedido. |
| `GET /orders/{id}` | Status: `PENDING`, `APPROVED` ou `CANCELLED` (com `motivo`). |
| Payment → Gateway | Header `Idempotency-Key = sagaId`. Timeout de 3 s. |

---

## 3. Modelo de dados de controle

Sintaxe PostgreSQL, simplificada.

```sql
-- Estado da saga (banco do Order Service / Orchestrator)
CREATE TABLE saga_instance (
  saga_id        UUID PRIMARY KEY,
  order_id       UUID NOT NULL UNIQUE,
  state          VARCHAR(30) NOT NULL,      -- STARTED, STOCK_RESERVING, ..., COMPLETED, CANCELLED
  current_step   VARCHAR(30) NOT NULL,
  payload        JSONB       NOT NULL,      -- dados necessários para compensar
  retry_count    INT         NOT NULL DEFAULT 0,
  deadline_at    TIMESTAMPTZ,               -- prazo da etapa atual (base dos timeouts)
  version        BIGINT      NOT NULL,      -- optimistic locking
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_saga_deadline ON saga_instance (deadline_at)
  WHERE state IN ('STOCK_RESERVING','PAYMENT_PROCESSING','CONFIRMING_STOCK',
                  'APPROVING_ORDER','COMPENSATING','CANCELLING_ORDER');

-- Trilha de auditoria: cada passo e cada compensação
CREATE TABLE saga_step_log (
  id          BIGSERIAL PRIMARY KEY,
  saga_id     UUID        NOT NULL REFERENCES saga_instance(saga_id),
  step        VARCHAR(30) NOT NULL,
  direction   VARCHAR(12) NOT NULL,         -- FORWARD | COMPENSATION
  status      VARCHAR(12) NOT NULL,         -- SENT | SUCCEEDED | FAILED
  message_id  UUID        NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotent Consumer: uma tabela por serviço consumidor
CREATE TABLE processed_message (
  message_id   UUID        NOT NULL,
  consumer     VARCHAR(50) NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (message_id, consumer)        -- a unicidade é o mecanismo
);

-- Transactional Outbox
CREATE TABLE outbox (
  id            BIGSERIAL PRIMARY KEY,
  aggregate_id  UUID        NOT NULL,       -- sagaId, usado como chave de partição
  type          VARCHAR(50) NOT NULL,
  payload       JSONB       NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);
```

---

## 4. Matriz de decisão: idempotência e mensagens duplicadas

### 4.1 Como o Idempotent Consumer funciona

Quando uma mensagem chega, o consumidor faz tudo numa única transação local:

1. `INSERT INTO processed_message (message_id, consumer)`.
2. Se der violação de chave primária, a mensagem já tinha sido processada antes. O efeito é descartado e a resposta original é reenviada (o orquestrador pode estar esperando por ela).
3. Se o insert passar, executa a regra de negócio e grava a mensagem de resposta no `outbox`.
4. Faz `COMMIT` e só então dá `ack` ao broker.

Como o registro de deduplicação e o efeito de negócio estão na mesma transação, não há janela em que um aconteça sem o outro. Esse detalhe é o que torna a garantia confiável.

### 4.2 Matriz: cenário → risco → mecanismo

| # | Cenário | Risco sem proteção | Mecanismo | Resultado esperado |
|---|---|---|---|---|
| 1 | `ReserveStock` entregue duas vezes | Reserva em dobro | `processed_message` (chave `message_id + consumer`) | Segunda entrega descartada; reenvia `StockReserved` |
| 2 | `ReleaseStock` entregue duas vezes | Estoque devolvido em dobro (inflado) | Idempotent Consumer mais `reservation` com status (`RESERVED → RELEASED`); só libera se estiver `RESERVED` | Estoque devolvido uma vez |
| 3 | Cliente reenvia `POST /orders` (duplo clique, retry do app) | Dois pedidos | `Idempotency-Key` com `UNIQUE` | Mesma resposta, um pedido |
| 4 | Payment reenvia a cobrança após timeout | Cobrança duplicada | `Idempotency-Key = sagaId` no gateway | Gateway devolve o resultado original |
| 5 | Resposta chega depois do estado ter avançado (ex.: `PaymentApproved` tardio após timeout) | Estado regride ou compensa errado | Orquestrador valida a transição: só aceita o evento se o par `(state, evento)` for válido; evento tardio vira ação corretiva (estorno) ou é registrado e ignorado | Estado nunca regride |
| 6 | Dois consumidores do orquestrador pegam a mesma saga ao mesmo tempo | Corrida: duas transições | `version` (optimistic locking) mais partição por `sagaId` | Um vence; o outro refaz a leitura ou descarta |
| 7 | Mensagens fora de ordem | Transição inválida | Partição por `sagaId` (ordem por saga) mais validação de transição na máquina de estados | Ordem preservada |
| 8 | Orquestrador cai depois de gravar o estado e antes de publicar | Saga travada | Outbox: a mensagem já está na transação e sai pelo relay | Publica após a recuperação |
| 9 | Compensação falha | Estoque preso ou pedido em limbo | Retry, DLQ, `COMPENSATION_FAILED` e alerta | Intervenção humana rastreável |
| 10 | Mensagem venenosa (malformada) | Loop infinito de reentrega | Retry limitado mais DLQ | Não bloqueia a partição |

### 4.3 Retenção da tabela de deduplicação

A `processed_message` cresce sem parar, então precisa de política de retenção. A regra é reter por um prazo maior que a janela máxima de reentrega do broker e que o prazo máximo da saga, digamos sete dias, com expurgo periódico. Um `message_id` mais velho que a retenção deixa de estar protegido; é por isso que a retenção tem que cobrir qualquer cenário realista de reentrega, com folga.

---

## 5. Timeouts de rede e a tabela de estado da saga

### 5.1 O problema

Um timeout não diz o que aconteceu. Para quem chamou, "sem resposta" é uma de três coisas: a requisição não chegou, chegou e falhou, ou chegou, teve sucesso e a resposta se perdeu no caminho. Não dá para distinguir uma da outra. Essa ambiguidade é a razão de nunca tratar timeout como falha; ele é um estado próprio, `UNKNOWN` ou `PAYMENT_TIMEOUT`.

### 5.2 Como a `saga_instance` lida com isso

Ao entrar numa etapa que depende de resposta, o orquestrador grava o `state` e o `deadline_at = now() + prazo` na mesma transação que enfileira o comando pelo outbox. Um job de varredura consulta periodicamente o índice parcial `idx_saga_deadline`:

```sql
SELECT saga_id FROM saga_instance
WHERE deadline_at < now()
  AND state IN ('STOCK_RESERVING','PAYMENT_PROCESSING', ...)
ORDER BY deadline_at
LIMIT 100
FOR UPDATE SKIP LOCKED;     -- várias instâncias do orquestrador sem disputar a mesma linha
```

Para cada saga vencida, o que fazer depende do estado em que ela parou:

| Estado ao vencer o prazo | Ação | Justificativa |
|---|---|---|
| `STOCK_RESERVING` | Reenvia `ReserveStock` (idempotente). Após N tentativas, vai a `COMPENSATING` com `ReleaseStock` | Repetir é seguro por causa da deduplicação. A reserva pode ter ocorrido sem a resposta chegar, e o `ReleaseStock` desfaz; uma reserva tardia chega depois do release e é recusada pelo Inventory |
| `PAYMENT_PROCESSING` | Vai a `PAYMENT_TIMEOUT` e envia `CancelPayment`. Não libera o estoque ainda | O pagamento pode ter ocorrido (ADR-006) |
| `CONFIRMING_STOCK` / `APPROVING_ORDER` | Reenvia o comando indefinidamente, com alerta | Passado o ponto sem retorno, só se avança |
| `COMPENSATING` / `CANCELLING_ORDER` | Reenvia a compensação. Após N tentativas, `COMPENSATION_FAILED`, DLQ e alerta | Compensação não pode falhar de vez |

### 5.3 Resolução do pagamento incerto

Quando o Payment recebe `CancelPayment`, ele consulta o gateway pela chave de idempotência (`sagaId`), e o desfecho depende do que encontra:

1. Se a cobrança existe e foi aprovada, emite um estorno (com a chave `sagaId-refund`, também idempotente) e responde `PaymentCancelled`.
2. Se a cobrança não existe, marca `payment(CANCEL_REQUESTED)`. Caso uma resposta tardia do gateway apareça depois, ela dispara o estorno automático, que leva ao `PaymentCancelled`.

Só com o `PaymentCancelled` em mãos o orquestrador manda `ReleaseStock` e, na sequência, `CancelOrder`. O resultado é o que importa: nunca há cliente cobrado sem pedido, nem estoque liberado com cobrança ainda ativa.

### 5.4 A falta de isolamento (o "I" que a Saga não tem)

Entre T1 e o fim da saga, outros leitores enxergam estados intermediários. Para conviver com isso sem estrago, o desenho usa três contramedidas clássicas:

- Semantic lock: o pedido em `PENDING` e a reserva em `RESERVED` são estados explícitos, que as demais regras já tratam (o estoque reservado não conta como disponível, o pedido `PENDING` não é faturado).
- Commutative updates: `ReleaseStock` é uma soma, comutativa com outras reservas.
- Reread value, ou version: a atualização do pedido confere a `version` antes de gravar.

---

## 6. Dead-Letter Queues

### 6.1 Quando uma mensagem vai para a DLQ

| Causa | Exemplo | Tratamento |
|---|---|---|
| Erro transitório que esgotou os retries | Inventory fora do ar por mais tempo que a política de retry | DLQ, reprocessar quando o serviço voltar |
| Mensagem malformada (poison message) | JSON inválido, campo obrigatório ausente | DLQ, corrigir o produtor, descartar ou reparar a mensagem |
| Violação inesperada de regra de negócio | SKU inexistente no catálogo | DLQ, análise manual; a saga recebe rejeição quando aplicável |
| Compensação que não conclui | `ReleaseStock` falhando repetidamente | DLQ, alerta crítico e `COMPENSATION_FAILED` |

### 6.2 Política

Uma DLQ por tópico ou fila de consumo (`cmd.inventory.dlq`, `evt.payment.dlq`, e assim por diante), sempre preservando a mensagem original e os metadados: motivo, número de tentativas, stack trace, timestamp.

Vale um alerta sempre que a profundidade da DLQ passar de zero, porque DLQ com mensagem esquecida é uma falha silenciosa, o pior tipo. O reprocessamento é controlado, por uma ferramenta ou endpoint que devolve a mensagem à fila original, e isso é seguro justamente porque os consumidores são idempotentes.

As DLQs de compensação (`ReleaseStock`, `CancelPayment`, `CancelOrder`) têm severidade alta, por um motivo direto: lidam com dinheiro e estoque presos. Cada tipo de mensagem tem o seu runbook, dizendo quem avalia, em quanto tempo e o que fazer (reprocessar, corrigir o dado ou encerrar a saga à mão).

---

## 7. Retry e Circuit Breaker

### 7.1 Retry

| Ponto | Política (ponto de partida) |
|---|---|
| Consumidor de mensagens | 3 tentativas imediatas curtas, depois retry topic com backoff e, por fim, DLQ |
| Payment → Gateway | 3 tentativas, backoff exponencial (ex.: 200 ms, 400 ms, 800 ms) com jitter, sempre com a mesma `Idempotency-Key` |
| Orquestrador reenviando comando | Backoff exponencial limitado, controlado por `retry_count` e `deadline_at` |
| Compensações | Retentáveis por mais tempo, com alerta; nunca descartadas em silêncio |

Duas regras fecham o assunto. Só vale retry para erro transitório (timeout, 5xx, falha de conexão); erro de negócio, como um `402` recusado ou um `400`, não se repete. E o jitter está ali para que os clientes não reajam todos no mesmo instante, o que evita o retry storm.

### 7.2 Circuit Breaker (Payment → Gateway)

Quando o gateway degrada, insistir só piora: esgota threads, enche filas e atrasa até os pagamentos que ainda passariam. O breaker corta isso com três estados:

| Estado | Comportamento |
|---|---|
| Fechado | Chamadas normais; o breaker conta falhas e chamadas lentas na janela deslizante |
| Aberto | Falha na hora (fail fast) por um período (ex.: 30 s), sem chamar o gateway |
| Semiaberto | Deixa passar poucas chamadas de teste; se passam, fecha; se falham, reabre |

Como ponto de partida: janela de 20 chamadas, abre com 50% de falha ou 60% de chamadas acima de 1 s, 30 s em aberto e 5 chamadas de teste no semiaberto.

O ponto importante é como isso conversa com a saga. Com o breaker aberto, o `ProcessPayment` não é descartado. O Payment segura a mensagem em retry até o `deadline_at`; vencido o prazo, o orquestrador cai no fluxo de `PAYMENT_TIMEOUT` da seção 5.3. Ou seja, a saga absorve a indisponibilidade em vez de empurrá-la como erro para o cliente.

Completam o arranjo três coisas: timeout em toda chamada (3 s no gateway); bulkhead, isto é, pool de threads e conexões isolado por dependência, para que a lentidão do gateway não consuma os recursos de outras operações do serviço; e um health check do orquestrador separado das dependências externas.

---

## 8. Riscos mitigados e riscos residuais

### Mitigados

| Risco | Mitigação |
|---|---|
| Inconsistência entre serviços (estoque preso, pedido sem cobrança) | Compensações semânticas em ordem inversa |
| Cobrança duplicada | Idempotency-Key mais Idempotent Consumer |
| Perda de mensagem entre banco e broker | Transactional Outbox |
| Pagamento de resultado incerto | Estado `UNKNOWN`, consulta de status, estorno idempotente, estoque liberado só depois |
| Orquestrador como ponto único de falha | Estado persistido, instâncias stateless, `FOR UPDATE SKIP LOCKED`, partição por `sagaId` |
| Cascata por lentidão do gateway | Timeout, retry com jitter, circuit breaker e bulkhead |
| Mensagem venenosa bloqueando a fila | Retry limitado mais DLQ |

### Residuais (aceitos e monitorados)

| Risco residual | Por que persiste | Como é tratado |
|---|---|---|
| Janela de estado intermediário visível (falta de isolamento) | É inerente às Sagas | Semantic lock e uma UX que mostra "pedido em processamento" |
| Compensação que falha de forma permanente | Pode haver bug ou dado corrompido | `COMPENSATION_FAILED`, DLQ de alta severidade, runbook |
| Crescimento da `processed_message` e da `saga_step_log` | Cada mensagem deixa rastro | Expurgo por retenção e particionamento de tabela |
| Orquestrador concentra o conhecimento do fluxo | É o preço da orquestração | Manter nele só transições de estado, sem regra de domínio, e testar a máquina de estados |
| Reconciliação financeira com o gateway | Ainda cabem divergências raras | Rotina diária de conciliação (pagamentos do gateway contra os do Payment DB) |

---

## 9. Observabilidade

Uma saga só é operável se for visível. Na prática, isso se traduz em cinco frentes.

A correlação começa pelo `sagaId`, propagado em logs, métricas e traces de todos os serviços, via headers ou atributos da mensagem. Sobre ele se montam as métricas que importam: sagas por estado, duração por etapa, taxa de compensação, `retry_count`, profundidade das DLQs, idade da saga mais antiga em estado não terminal e estado do circuit breaker.

Os alertas disparam nos sinais que não podem passar: DLQ com mensagem, saga parada em estado intermediário além do SLA, `COMPENSATION_FAILED`, taxa de recusa fora do normal e breaker aberto por mais de X minutos. Para a parte forense, a `saga_step_log` guarda cada passo, de ida e de compensação, e responde à pergunta de sempre, "o que aconteceu com o pedido X?".

Por fim, os próprios cenários deste documento (duplicata, timeout, compensação com falha, gateway lento) rendem bons experimentos de Chaos Engineering: injeta-se a falha e verifica-se se a saga converge para um estado final coerente.

---

*Ver também: [README](../README.md) · [diagramas](../diagrams/) · [Saga pattern (microservices.io)](https://microservices.io/patterns/data/saga.html)*
