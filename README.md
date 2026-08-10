# Dotnet.KafkaFlow — multi-domain message processor

A Kafka consumer/producer built on **KafkaFlow** (.NET 9) that processes **multiple domains from one
codebase**. Everything a domain shares — message envelope, field builders, data-type filtering, the
consumer handler, metrics, health — lives in `Processor.Core` and is parameterized by the domain's
payload type. A domain contributes a payload class, a message class, one builder, and a module.

The same image is deployed **once per domain**; only `Processor:Domain` differs between deployments.

## Table of Contents

- [Layout](#layout)
- [Multi-domain design](#multi-domain-design)
  - [The generic message envelope](#the-generic-message-envelope)
  - [Shared vs. domain-specific builders](#shared-vs-domain-specific-builders)
  - [Domain modules and DI](#domain-modules-and-di)
  - [Adding a domain](#adding-a-domain)
- [Message flow](#message-flow)
- [Data-type filtering (Oracle)](#data-type-filtering-oracle)
- [Configuration](#configuration)
- [Consumer worker tuning](#consumer-worker-tuning)
- [Health & resilience](#health--resilience)
- [Observability](#observability)
- [Running locally](#running-locally)
- [Testing](#testing)

---

## Layout

```
Dotnet.KafkaFlow/
├── src/
│   ├── Processor.Core/                  # everything shared between domains
│   │   ├── Messages/                    # IInputMessage, InputMessage<T>, OutputMessage<T>, DeadLetterMessage<T>
│   │   ├── Building/                    # BuildStatus/Outcome, field builders, OutputMessageBuilder<TInput,TData>
│   │   ├── Application/                 # MessageHandler<TInput,TData>, options
│   │   ├── DataTypes/                   # Oracle + in-memory stores, caching repository, refresh service
│   │   ├── Domains/                     # IDomainModule, DomainModule<TInput,TData>, DomainModuleSelector
│   │   ├── Diagnostics/                 # metrics, Kafka statistics bridge, log handler
│   │   └── Health/                       # data store + settings readiness checks
│   ├── Processor.Domains.Posts/         # domain A: social-network posts
│   ├── Processor.Domains.Profiles/      # domain B: social-network profiles
│   └── Processor.Host/                  # the deployable: composition, endpoints, appsettings.json
└── tests/
    ├── TestData/                        # JSON test cases, one sub-directory per domain
    │   ├── Posts/
    │   └── Profiles/
    ├── Shared/                          # test-case model + loader shared by both e2e suites
    ├── Processor.Core.Tests/            # unit tests for the shared pipeline (synthetic domain)
    ├── Processor.Domains.Posts.Tests/   # unit tests for the posts rules
    ├── Processor.Domains.Profiles.Tests/# unit tests for the profiles rules
    ├── Processor.Host.Tests/            # host composition: domain selection, DI graph, options
    ├── Processor.MockTests/             # e2e through the real DI graph, mocked Kafka + in-memory store
    └── Processor.HostE2ETests/          # e2e through the real host, real Kafka + real Oracle
```

Both domains are chosen from the same business area (social networks) on purpose: they differ in
*entity*, not in problem space, which is the case the generic base is for.

---

## Multi-domain design

### The generic message envelope

`InputMessage<TDomainData>` holds the fields every domain shares. A domain inherits it and closes the
type parameter, which is what gives that domain's message its own strongly-typed payload field:

```csharp
public abstract class InputMessage<TDomainData> : IInputMessage
    where TDomainData : class, IDomainData, new()
{
    public string Id { get; set; }
    public string Content { get; set; }
    public DateTime Timestamp { get; set; }
    public TDomainData DomainData { get; set; } = new();   // ← the domain-specific field
}

public sealed class PostInputMessage    : InputMessage<PostData>    { }
public sealed class ProfileInputMessage : InputMessage<ProfileData> { }
```

`OutputMessage<TDomainData>` and `DeadLetterMessage<TDomainData>` are parameterized the same way, so a
dead-lettered message keeps its domain payload intact for replay instead of being flattened.

`IInputMessage` is the non-generic view of the shared fields. It exists so Core's shared field builders
can be written once against it — see below.

### Shared vs. domain-specific builders

`IOutputFieldBuilder<in TInput, TValue>` is **contravariant** in `TInput`. That is what lets a single
shared builder satisfy every domain's dependency without per-domain copies:

```csharp
// One implementation, declared against the interface…
public class OutputIdBuilder : IOutputFieldBuilder<IInputMessage, string> { … }

// …and it satisfies IOutputFieldBuilder<PostInputMessage, string> too.
```

| Builder | Scope | Ok | DeadLetter | Drop |
|---|---|---|---|---|
| `OutputIdBuilder` | shared | non-empty id | missing id | — |
| `ProcessedContentBuilder` | shared | non-empty content | — | empty content |
| `ProcessedAtBuilder` | shared | always | — | — |
| `ProcessorNameBuilder` | shared | always | — | — |
| `PostDataBuilder` | posts | valid post payload | no author / no network / negative counters | — |
| `ProfileDataBuilder` | profiles | valid profile payload | no handle / no network / negative counters | no followers |

A domain supplies exactly one builder, an `IDomainDataBuilder<TInput, TDomainData>`.
`OutputMessageBuilder<TInput, TDomainData>` orchestrates the shared builders and then the domain one,
honouring its dead-letter/drop decisions identically. Domain processing runs **last**, so a message the
shared rules would reject never pays for it.

Note the two domains make *different* routing choices for a similar situation — posts dead-letter a
malformed payload, profiles *drop* a follower-less account — and the same Core orchestration handles
both.

### Domain modules and DI

KafkaFlow's registration API needs closed generic types
(`AddSingleTypeDeserializer<PostInputMessage, …>`) that the host cannot name without knowing the
domain. `DomainModule<TInput, TDomainData>` closes them from inside the domain project, so a domain
module is all a domain has to expose:

```csharp
public sealed class PostsDomainModule : DomainModule<PostInputMessage, PostData>
{
    public override string Name => PostData.Domain;   // "posts"

    protected override void RegisterDomainServices(IServiceCollection services) =>
        services.AddSingleton<IDomainDataBuilder<PostInputMessage, PostData>, PostDataBuilder>();
}
```

The host compiles in **every** module and activates exactly **one**, chosen by configuration:

```csharp
var module = DomainModuleSelector.Select(AvailableDomains(), configuration["Processor:Domain"]);
module.RegisterServices(services);          // the only domain-specific registrations
```

An unknown or missing domain name **fails startup** with the list of valid names — a typo must never
silently run the wrong processor against a deployment's topics. A posts deployment does not even have
the profiles pipeline in its container.

### Adding a domain

1. `PaymentData : IDomainData` — the payload, with a `DomainName`.
2. `PaymentInputMessage : InputMessage<PaymentData>` — empty body; the base supplies everything.
3. `PaymentDataBuilder : IDomainDataBuilder<PaymentInputMessage, PaymentData>` — the domain rules.
4. `PaymentsDomainModule : DomainModule<PaymentInputMessage, PaymentData>` — name + register the builder.
5. Add the module to `ProcessorHostExtensions.AvailableDomains()`, and the project reference.
6. Add `tests/TestData/Payments/*.json` and a two-line theory in each e2e suite.

No change to Core, the handler, the orchestrating builder, metrics, or health.

---

## Message flow

```
Input topic (one per domain deployment)
       ↓
[MessageHandler<TInput,TData> consumes]
       ↓  data type id from header `data-type-id`, else the message key
[OutputMessageBuilder<TInput,TData>.Build(input, dataTypeId)]
       ├─→ data-type filter (Oracle-backed snapshot)
       │     ├─→ missing / unknown / inactive ──→ Filtered: log + count, commit offset, produce nothing
       │     └─→ active? continue ↓
       ├─→ shared field builders (id, content, processedAt, processorName)
       │     ├─→ DeadLetter? ──→ dead-letter topic
       │     ├─→ Drop?       ──→ log + count only
       │     └─→ Ok? continue ↓
       ├─→ domain data builder (PostDataBuilder / ProfileDataBuilder)
       │     ├─→ DeadLetter? ──→ dead-letter topic
       │     ├─→ Drop?       ──→ log + count only
       │     └─→ Ok? continue ↓
       ↓
[BuildOutcome<TData>]  ──→ Ok: OutputMessage<TData> to the output topic (keyed by message id)
```

`Filtered` and `Drop` are distinct outcomes: the first means "this data type is switched off", the
second "this message has nothing to process". Both commit the offset and produce nothing; they are
counted separately.

---

## Data-type filtering (Oracle)

Records are filtered by **data type** before any processing. Each data type has a row with an active
flag; only records whose data type is active continue.

- **Data type id source**: the Kafka header `data-type-id` (configurable), falling back to the message key.
- **Filter reasons** are bounded category codes, safe as a metric label:
  `missing_data_type_id`, `unknown_data_type`, `inactive_data_type`.
- **Rows are scoped by domain**, so one table serves every deployment and a profiles row can never
  switch a posts processor on.

### Schema

```sql
CREATE TABLE DATA_TYPE_SETTINGS (
  DOMAIN_NAME  VARCHAR2(64)  NOT NULL,
  DATA_TYPE_ID VARCHAR2(128) NOT NULL,
  IS_ACTIVE    NUMBER(1)     DEFAULT 0 NOT NULL,
  CONSTRAINT PK_DATA_TYPE_SETTINGS PRIMARY KEY (DOMAIN_NAME, DATA_TYPE_ID)
);

INSERT INTO DATA_TYPE_SETTINGS VALUES ('posts', 'engagement', 1);
```

`src/Processor.Host/oracle/init/01_data_type_settings.sql` creates and seeds this on first container
start. Reads use **Dapper** over `Oracle.ManagedDataAccess.Core`.

### Load lifecycle

`DataTypeSettingsRefreshService` (a hosted service registered **before** the KafkaFlow one, so it runs
first) owns the snapshot:

- **Startup**: attempts the load up to `StartupAttempts` times (default **3**, spaced by
  `StartupRetryDelaySeconds`). If all attempts fail it **throws** — the host exits non-zero and the pod
  restarts. The processor never consumes without settings.
- **Steady state**: reloads every `RefreshMinutes` (default **10**). A failed refresh logs a warning and
  **keeps the current snapshot** — it never clears settings and never crashes. The next tick retries.
- **Hot path**: reads hit only the in-memory snapshot, so a database outage produces **zero** per-message
  queries regardless of throughput.

The store is swappable via `DataTypeSettings:Store` (`oracle` | `inmemory`). Only the store changes —
caching, refresh and fail-fast startup are identical either way, so a local `inmemory` run exercises the
same code path as production.

---

## Configuration

`appsettings.json` is the **only** settings file (no per-environment variants); environment-specific
values come from environment variables or the deployment's config.

| Key | Meaning |
|---|---|
| `Processor:Domain` | **The domain this deployment runs** (`posts` / `profiles`). Required. |
| `Kafka:Brokers` | Broker list. Required — no default, because silently pointing at localhost is worse than refusing to start.¹ |
| `Kafka:InputTopic` / `OutputTopic` / `DeadLetterTopic` | Per-deployment topics. |
| `Kafka:ConsumerGroupId` | Per-domain consumer group. |
| `Kafka:WorkersCount` / `BufferSize` | Parallelism — see below. |
| `Kafka:AutoOffsetReset` | `earliest` (default) or `latest`.² |
| `Kafka:StatisticsIntervalMs` | librdkafka statistics interval. |
| `Oracle:ConnectionString` / `SettingsTable` | Settings store. `SettingsTable` must be a bare identifier — it is interpolated into SQL and validated as such. |
| `DataTypeSettings:Store` | `oracle` (default) or `inmemory`. |
| `DataTypeSettings:RefreshMinutes` | Snapshot reload interval (default 10). |
| `DataTypeSettings:StartupAttempts` | Initial-load attempts before startup fails (default 3). |
| `DataTypeSettings:HeaderName` | Header carrying the data type id. |
| `Metrics:Port` | Port for `/metrics` and the probes. |
| `Benchmark:WorkMicros` | Synthetic per-message CPU work for load testing; `0` = off. |

¹ Note the .NET configuration binder **appends** to a non-empty collection default rather than replacing
it, so a default broker list would survive into a production deployment. `Brokers` is therefore empty by
default and validated at startup.

² This overrides KafkaFlow's own `latest` default, deliberately: the processor consumes every message it
can. With `latest`, a new deployment silently skips the existing backlog, and anything produced before
the consumer group's first partition assignment completes is lost outright. Nothing in the pipeline
depends on starting at the head of the topic.

Deploying the two domains means the same image with two configs:

```yaml
# posts deployment                      # profiles deployment
Processor__Domain: posts                Processor__Domain: profiles
Kafka__InputTopic: posts-input          Kafka__InputTopic: profiles-input
Kafka__OutputTopic: posts-output        Kafka__OutputTopic: profiles-output
Kafka__DeadLetterTopic: posts-dlq       Kafka__DeadLetterTopic: profiles-dlq
Kafka__ConsumerGroupId: posts-group     Kafka__ConsumerGroupId: profiles-group
```

---

## Consumer worker tuning

| Setting | Meaning |
|---------|---------|
| `Kafka:WorkersCount` | Worker loops processing messages in parallel. Workers are **not** partition consumers — one Kafka consumer fetches and the distribution strategy routes each message to a worker. |
| `Kafka:BufferSize` | Per-worker bounded prefetch channel. Total in-flight ≈ `WorkersCount × BufferSize`. |

The consumer uses **`FreeWorkerDistributionStrategy`** because this system is **keyless** with respect to
ordering. With the default `BytesSum` strategy, null-key messages all hash to worker 0 — i.e.
single-threaded regardless of `WorkersCount`.

A load-test sweep (`loadtests/workers-buffer-tuning`) found throughput scales with workers up to ≈ the
host core count, then flattens while per-message latency rises; buffer size has negligible effect on a
keyless/CPU-bound workload. **Rule of thumb: `WorkersCount` ≈ the pod's CPU allotment, `BufferSize` ~100.**

---

## Health & resilience

| Endpoint | Checks | Fail behavior |
|----------|--------|---------------|
| `GET /health/live` | process only (no dependencies) | a database blip won't get the pod killed |
| `GET /health/ready` | data store reachable + settings snapshot loaded and fresh | NotReady (503) during an outage; **Degraded** while serving a snapshot older than 3× the refresh interval |

```yaml
livenessProbe:  { httpGet: { path: /health/live,  port: 8080 }, periodSeconds: 10 }
readinessProbe: { httpGet: { path: /health/ready, port: 8080 }, periodSeconds: 10 }
```

---

## Observability

`GET http://localhost:8080/metrics` — Prometheus exposition format, via OpenTelemetry. Traces and
metrics also go out over OTLP to `OpenTelemetry:OtlpEndpoint` when available.

Every message-outcome metric carries a **`domain`** label, so one dashboard compares the per-domain
deployments running this same code.

| Metric | Type | Meaning |
|--------|------|---------|
| `messages_processed_total{domain}` | counter | Messages sent to the output topic |
| `messages_dead_lettered_total{domain}` | counter | Messages routed to the dead-letter topic |
| `messages_dropped_total{domain}` | counter | Messages dropped |
| `messages_filtered_total{domain,reason}` | counter | Filtered by data-type settings |
| `messages_processing_duration_milliseconds{domain}` | histogram | Per-message handling latency |
| `datastore_operations_total{operation,status}` | counter | Settings store round-trips (`load_all` / `probe`, `ok` / `error`) |
| `datastore_operation_duration_milliseconds` | histogram | Settings store latency |
| `datatype_settings_reloads_total` / `..._load_failures_total` | counter | Successful vs. failed reloads |
| `datatype_settings_snapshot_size` / `..._since_load_seconds` | gauge | Snapshot size; seconds since last successful load |
| `kafka_consumer_lag` / `..._assignment_partitions` / `..._fetchq_messages` | gauge | From librdkafka statistics |
| `kafka_consumer_rebalances_total` / `..._rx_messages_total` | counter | Rebalances; messages received |
| `kafka_broker_rtt_avg_milliseconds` / `..._max_...` | gauge | Broker round-trip time |
| `dotnet_*` | various | .NET runtime instrumentation |

---

## Running locally

```bash
cd src/Processor.Host

# 1. Kafka + Oracle + Prometheus + Grafana (skip the heavier ELK stack)
docker compose up -d zookeeper broker oracle prometheus grafana

# 2. Run the posts domain (appsettings.json default)
dotnet run --project .

# …or the profiles domain, same code:
Processor__Domain=profiles \
Kafka__InputTopic=profiles-input Kafka__OutputTopic=profiles-output \
Kafka__DeadLetterTopic=profiles-dlq Kafka__ConsumerGroupId=profiles-group \
  dotnet run --project .
```

Produce a few posts. The key carries the data type id (`key.separator` avoids the `:` inside JSON), and
JSON property names are **PascalCase**:

```bash
printf '%s\n' \
  'engagement|{"Id":"post-1","Content":"hello #kafka","DomainData":{"AuthorHandle":"@ada","Network":"X","Likes":10,"Shares":2}}' \
  'engagement|{"Id":"","Content":"no id -> dead letter","DomainData":{"AuthorHandle":"ada","Network":"x"}}' \
  'retired-feed|{"Id":"post-3","Content":"inactive -> filtered","DomainData":{"AuthorHandle":"ada","Network":"x"}}' \
  | docker exec -i broker kafka-console-producer --bootstrap-server broker:9092 \
      --topic posts-input --property parse.key=true --property key.separator='|'
```

Then open **<http://localhost:8080/metrics>**, **<http://localhost:9090>** (Prometheus), and
**<http://localhost:3000>** (Grafana — anonymous access, dashboard auto-provisioned).

No Oracle handy? `DataTypeSettings__Store=inmemory` runs the identical pipeline against an empty
settings set (everything filters as `unknown_data_type`).

---

## Testing

```bash
dotnet build
dotnet test                                        # everything, including the container-backed e2e suite
dotnet test tests/Processor.Core.Tests             # shared pipeline
dotnet test tests/Processor.Domains.Posts.Tests    # posts rules
dotnet test tests/Processor.MockTests              # fast end-to-end
dotnet test tests/Processor.HostE2ETests           # real Kafka + real Oracle (needs Docker; minutes)
```

| Project | What it covers | Needs Docker |
|---|---|---|
| `Processor.Core.Tests` | Shared pipeline, exercised against a **synthetic domain** so it can't lean on a real domain's rules. Filtering, short-circuit ordering, handler routing, header/key resolution, caching repository, the 3-attempt startup contract, health checks. | no |
| `Processor.Domains.Posts.Tests` | Posts rules: handle/network normalization, hashtag extraction and de-duplication, share-weighted engagement score, derived fields never trusted from the wire. | no |
| `Processor.Domains.Profiles.Tests` | Profiles rules: follower ratio (including the divide-by-zero guard), audience-tier boundaries, display-name fallback, drop-vs-dead-letter precedence. | no |
| `Processor.Host.Tests` | Host composition: domain selection from config, the DI graph resolving fully closed, that a posts deployment carries no profiles types, hosted-service **ordering** (settings before Kafka), options binding. | no |
| `Processor.MockTests` | Every JSON test case end-to-end through the **real DI graph and real startup**, with Kafka producers mocked and the in-memory store. Plus fail-fast startup, outage tolerance, and live settings changes. | no |
| `Processor.HostE2ETests` | The same JSON cases through the **real host**: real Kafka broker and real Oracle (Testcontainers), including the schema and table. Plus Oracle SQL/column mapping, per-domain row scoping, and both domains running side by side from one codebase. | **yes** |

### Data-driven test cases

Cases live in `tests/TestData/<Domain>/test_case_*.json` and are driven by **both** e2e suites, so the
mock and real-infrastructure runs assert the same expectations. Each file is one xUnit case, so a
failure names the file.

```json
{
  "dataTypeId": "engagement",
  "dataTypeSettings": [
    { "dataTypeId": "engagement", "isActive": true },
    { "dataTypeId": "retired-feed", "isActive": false }
  ],
  "input": {
    "id": "post-002",
    "content": "shipping #KafkaFlow with #dotnet",
    "domainData": { "authorHandle": "grace", "network": "mastodon", "likes": 5, "shares": 5 }
  },
  "expectedOutcome": "output",
  "expectedOutput": {
    "id": "post-002",
    "processedContent": "SHIPPING #KAFKAFLOW WITH #DOTNET",
    "domainData": {
      "authorHandle": "grace", "network": "mastodon", "likes": 5, "shares": 5,
      "hashtags": ["kafkaflow", "dotnet"], "engagementScore": 20
    }
  }
}
```

| Field | Purpose |
|---|---|
| `dataTypeId` | Sent as the Kafka key. `null` means no key and no header. |
| `dataTypeSettings` | **The complete settings store for this case** — every data type the processor should see, and whether each is active. `isActive` defaults to `true`. |
| `expectedOutcome` | `output` \| `deadletter` \| `dropped` \| `filtered`. |
| `expectedOutput` | Asserted fields, including the processed `domainData` (structurally compared — see below). `processedAt` and `processorName` are environment-dependent and deliberately not asserted. |
| `expectedDeadLetterReason` / `expectedDropReason` / `expectedFilterReason` | Required for their outcome. |

### Per-case settings, and why they can't collide

Each case declares the store's entire contents, and each case gets its **own store**:

- **Mock suite** — its own `InMemoryDataTypeSettingsStore` instance per case.
- **Host suite** — its own **Oracle table**, created fresh and empty for that case. Since
  `Oracle:SettingsTable` is configurable, the host under test is simply pointed at it. No case can read,
  overwrite, or delete another's rows, and the suite stays correct if it is ever run in parallel.

The three filter outcomes therefore fall out of the list itself rather than from flags:

| To exercise | Declare |
|---|---|
| `missing_data_type_id` | `dataTypeId: null` (settings may still be present — they're irrelevant) |
| `unknown_data_type` | a list that does **not** contain `dataTypeId` — ideally with *other* active types, which proves selection is by id rather than "whatever is present" |
| `inactive_data_type` | `dataTypeId` listed with `isActive: false`, ideally beside an active sibling |

Because the list is the whole store, a case can also assert something the old boolean flags couldn't
express: that an active data type is still selected correctly when inactive decoys sit alongside it.

A malformed or internally inconsistent case **fails loudly at parse time** rather than passing
silently. That check earns its keep — omitting a data type from an `output` case would otherwise turn it
into an unnoticed `unknown_data_type` filter whose assertions ("nothing produced, nothing
dead-lettered") still hold, for entirely the wrong reason. Instead:

```
test_case_3_zero_engagement.json: outcome 'output' requires 'dataTypeSettings' to list
'engagement' as active, otherwise the message is filtered before it can be output.
```

The rules — duplicates, blank ids, and each filter reason matching the settings that would actually
cause it — are themselves tested in `Processor.MockTests/TestCaseValidationTests.cs`.

### Why the end-to-end suite is fast

Asserting "nothing was produced" against a real broker invites a fixed wait, and a fixed wait per case
is what makes an integration suite slow — the original version sat out 8 seconds per test.

Instead, each case produces two **watermark** messages after the message under test: one that reaches
the output topic and one that dead-letters (an empty id, which the shared id rule rejects for every
domain). Reads run *until the watermark*, so once it is observed the processor has demonstrably
finished with everything ahead of it and an empty result is a fact rather than the absence of evidence.
There is no fixed wait anywhere in the suite — 30 tests against real Kafka and Oracle run in ~20s.

This is why the hosts under test run a **single worker**: the watermark must be *handled* after the
message under test, not merely produced after it. Concurrency is a throughput concern, covered by the
load tests rather than here. A broken consumer fails with an explicit "the watermark never arrived"
timeout instead of silently passing an emptiness assertion.

The test-case model and loader are shared source (`tests/Shared/`, linked via `tests/TestData.targets`)
rather than a seventh project.

### How payloads are compared

`TestCaseJson.AssertMatches` walks both payloads as JSON and reports **every** difference by path, then
prints both payloads in full. Property order is normalized (so ordering never matters) but array order
is significant, and comparison is case-sensitive — the domain builders normalize casing, so a casing
regression has to fail.

```
Processed domain payload for test_case_2_hashtags.json does not match the expected payload.

3 differences:
  engagementScore: expected 20 but was 15
  hashtags[1]: expected "dotnet" but was "netcore"
  shares: expected 5 but was 4

expected:
{ … full payload, camelCase, indented … }
actual:
{ … }
```

It replaced an `Assert.Equal` over canonical JSON strings, which reported only the first differing
*character* in a truncated one-line window with no field name, and printed PascalCase that didn't match
the fixtures. The comparison has its own tests (`Processor.MockTests/TestCaseJsonTests.cs`) — if it
failed to spot a difference, every payload assertion in both e2e suites would pass vacuously.

---

## Load tests

`loadtests/` holds the historical load-test suites and their reports. Note the two `redis-*` suites
predate the move to Oracle and are kept for their findings, not as runnable tooling.

## Dependencies

KafkaFlow 4.1.0 · Dapper 2.1.66 · Oracle.ManagedDataAccess.Core 23.9.1 · OpenTelemetry 1.11.2 ·
xUnit 2.9.3 · Moq 4.20.72 · Testcontainers 4.0.0 · .NET 9

## License

MIT
