# ROADMAP — Plano de Ação para o qKafka

Este documento serve como o guia estratégico e plano de ação para o desenvolvimento do **qKafka** (ou **QKafka**). O objetivo principal é construir uma alternativa ao MassTransit para Kafka no ecossistema .NET que seja **gratuita (MIT)**, **Kafka-First** (respeitando a arquitetura do broker) e **infinitamente mais simples de configurar e depurar**.

---

## 🎯 A Visão do Projeto
O `qKafka` resolve as complexidades do Apache Kafka no .NET combinando a robustez de Sagas e Outbox do MassTransit com a alta performance e simplicidade operacional.

### Diferenciais Competitivos
1. **MIT License:** 100% gratuito, livre das amarras comerciais da v9 do MassTransit.
2. **Kafka-First:** Não tenta simular filas clássicas (como RabbitMQ). Abraça partições, chaves e offsets lineares nativamente.
3. **Proteção de Loop:** Thread de Polling separada dos Workers, impedindo *Rebalance Storms* causados por códigos lentos.
4. **Configuração via Convenção:** Source Generators eliminam o excesso de boilerplate de configuração (DX de ponta).
5. **Dashboard Embutido:** Visibilidade de Lag, fluxo de mensagens e status das Sagas em tempo real sem dependências externas.

---

## 🛠️ Arquitetura de Referência

```
                        [ Banco de Dados ] 
                         ▲            │
            Inbox Check  │            ▼ Outbox Commits & Saga State
                         │      ┌─────────────┐
                         │      │   qKafka    │
                         │      │ Dispatcher  │
                         │      └──────┬──────┘
                         │             │ Publish events
                         │             ▼
[ Kafka Consumer ] ──► [ Inbox ] ──► [ Saga State Machine ] ──► [ Outbox ] ──► [ Kafka Producer ]
```

---

## 📅 Plano de Ação Passo a Passo (Fases)

### Fase 1: O Motor Core (Consumo e Envio)
*Foco: Estabelecer a comunicação base e o loop de consumo protegido.*
- [ ] Implementar a abstração do `ConsumerLoop` sobre o `Confluent.Kafka`.
- [ ] Criar a separação física da Thread de Polling e do Thread Pool de processamento de mensagens.
- [ ] Implementar o particionamento em memória por chave (`Partition Key`) para garantir que mensagens com o mesmo ID rodem sequencialmente, mantendo a ordem correta.
- [ ] Criar a infraestrutura de tratamento automático de *Poison Pills* (redirecionar falhas de serialização para tópicos de erro e comitar o offset automaticamente).
- [ ] Adicionar suporte nativo à propagação de cabeçalhos do **OpenTelemetry** (W3C Tracing).

### Fase 2: Confiabilidade Extrema (Outbox & Inbox)
*Foco: Garantias de entrega At-Least-Once e Exactly-Once (idempotência).*
- [ ] **Outbox Pattern:** Criar interceptador de transações do banco de dados (Postgres/SQL/Mongo) para salvar as mensagens de saída na mesma transação lógica das mutações de estado da aplicação.
- [ ] **Inbox Pattern:** Criar middleware automático de verificação de duplicidade de mensagens baseada em chave única de idempotência antes de disparar o consumidor.
- [ ] Criar o componente de publicação assíncrona em segundo plano para ler da tabela do Outbox e enviar ao Kafka de forma resiliente.

### Fase 3: O Motor de Sagas (`QKafka.Sagas`)
*Foco: Transações distribuídas fáceis de programar e depurar.*
- [ ] Implementar as classes base de Saga e os repositórios plugáveis de persistência de estado (`ISagaRepository`).
- [ ] Criar o roteador de correlação automática de mensagens baseado em propriedades de payload ou cabeçalhos (`CorrelationId`).
- [ ] Desenvolver a infraestrutura para rodar ações compensatórias automáticas em ordem reversa caso uma etapa da Saga falhe.
- [ ] Garantir atomicidade absoluta: A persistência do estado da Saga e a gravação de novos eventos no Outbox devem ocorrer em um único comando de banco.

### Fase 4: Telemetria e Dashboard (`QKafka.Dashboard`)
*Foco: Visibilidade operacional imediata para o desenvolvedor.*
- [ ] Criar o painel visual integrado à aplicação (sem infraestrutura extra).
- [ ] **Lag Monitor:** Visualização em tempo real de mensagens pendentes por partição e grupo de consumo.
- [ ] **Saga Timeline:** Visualizar o histórico e linha do tempo de transições de estado de qualquer instância de Saga ativa ou finalizada.
- [ ] **Message Streamer:** Visualização ao vivo (JSON formatado) do payload das mensagens trafegando nos tópicos locais.

### Fase 5: In-Memory Test Harness (`QKafka.Testing`)
*Foco: Permitir testes unitários rápidos e mock de alta fidelidade.*
- [ ] Desenvolver o `InMemoryKafkaTestHarness` para emular tópicos, partições, produtores, consumidores e comportamento de Sagas na memória do computador.
- [ ] Permitir asserções limpas nos testes (ex: `harness.AssertPublished<OrderPlacedEvent>()`).

---

## 🛡️ Invariantes do Código
Qualquer nova funcionalidade no projeto deve respeitar as seguintes regras (conforme o [foundation-minimal.md](file:///home/luciano/git/qKafka/ai-method/core/00-foundation-minimal.md)):
1. O banco de dados de persistência é a **única fonte da verdade**.
2. **Heartbeats do Kafka** nunca devem ser atrasados por processos de negócio.
3. Toda funcionalidade deve produzir testes cobrindo a **Matriz 3N** (Positivo, Negativo, Inválido/Limite).
4. Compilação **Release** exige **zero avisos** (warnings).
