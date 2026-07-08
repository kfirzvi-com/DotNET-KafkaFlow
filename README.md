# Dotnet.KafkaFlow - Message Processor

A sophisticated Kafka message processing application built with .NET 9.0 using the **KafkaFlow** library. This project demonstrates advanced architectural patterns for message handling, including the Builder Pattern for separating business logic concerns and comprehensive data-driven testing.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
  - [Project Structure](#project-structure)
  - [Builder Pattern](#builder-pattern)
  - [Message Flow](#message-flow)
- [KafkaFlow Integration](#kafkaflow-integration)
- [Building & Testing](#building--testing)
  - [Unit Testing Strategy](#unit-testing-strategy)
  - [Data-Driven Tests](#data-driven-tests)
  - [VS Code Test Explorer](#vs-code-test-explorer)

---

## Project Overview

**Dotnet.KafkaFlow** is a Kafka consumer/producer application that processes input messages and routes them to different output queues based on validation and business logic:

- **Input Topic**: `input-topic` - Receives `InputMessage` objects
- **Output Topic**: `output-topic` - Valid messages sent here as `OutputMessage`
- **Dead Letter Topic**: `dead-letter-topic` - Messages that fail validation
- **Dropped Messages**: Logged but not sent anywhere (no persistence)

Each field in the output message is computed independently by its own builder class, allowing for:
- ✅ Isolated unit testing
- ✅ Clear separation of concerns
- ✅ Easy extension and modification
- ✅ Flexible validation and routing decisions

---

## Architecture

### Project Structure

```
Dotnet.KafkaFlow/
├── Processor/                           # Main application
│   ├── Program.cs                       # KafkaFlow configuration & DI setup
│   ├── Handlers/
│   │   └── MessageHandler.cs            # Consumes messages, orchestrates builders
│   ├── Builders/
│   │   ├── Core/                        # Core builder infrastructure
│   │   │   ├── BuildStatus.cs           # Enum: Ok, DeadLetter, Drop
│   │   │   ├── FieldBuildResult.cs      # Generic result wrapper for field builders
│   │   │   ├── BuildOutcome.cs          # Final message outcome
│   │   │   ├── IOutputFieldBuilder.cs   # Interface for field builders
│   │   │   ├── IOutputMessageBuilder.cs # Interface for message coordinator
│   │   │   └── OutputMessageBuilder.cs  # Orchestrates field builders
│   │   └── FieldBuilders/               # Individual field implementations
│   │       ├── OutputIdBuilder.cs       # Validates & formats message ID
│   │       ├── ProcessedContentBuilder.cs # Transforms content, decides routing
│   │       ├── ProcessedAtBuilder.cs    # Timestamps the message
│   │       └── ProcessorNameBuilder.cs  # Adds processor metadata
│   ├── Messages/
│   │   ├── InputMessage.cs              # Message from Kafka topic
│   │   ├── OutputMessage.cs             # Message to output topic
│   │   └── DeadLetterMessage.cs         # Message for dead letter handling
│   └── Properties/
│       └── launchSettings.json
├── Processor.Tests/                     # Comprehensive test suite
│   ├── MessageHandlerTests.cs           # Integration tests with outcomes
│   ├── Builders/
│   │   ├── OutputIdBuilderTests.cs      # Unit tests for ID validation
│   │   └── ProcessedContentBuilderTests.cs # Unit tests for content processing
│   ├── Helpers/
│   │   └── TestDataLoader.cs            # Loads test cases from JSON files
│   └── TestsData/                       # JSON test case definitions
│       ├── test_case_1.json             # Output outcome test
│       ├── test_case_2.json             # Output outcome test
│       ├── test_case_3.json             # Output outcome test
│       ├── test_case_4.json             # Output outcome test
│       ├── test_case_5.json             # Output outcome test
│       ├── test_case_deadletter.json    # DeadLetter outcome test
│       └── test_case_dropped.json       # Drop outcome test
├── Dotnet.KafkaFlow.sln
└── .gitignore
```

### Builder Pattern

The **Builder Pattern** is used to compute each field of `OutputMessage` independently:

#### Core Concepts

1. **BuildStatus Enum** - Determines message routing:
   ```csharp
   public enum BuildStatus
   {
       Ok,           // Message is valid, send to output
       DeadLetter,   // Validation failed, send to DLQ
       Drop          // Message should be silently dropped (logged only)
   }
   ```

2. **FieldBuildResult<T>** - Generic wrapper for field computation:
   ```csharp
   public class FieldBuildResult<T>
   {
       public BuildStatus Status { get; }  // Ok/DeadLetter/Drop
       public T? Value { get; }            // Computed field value
       public string? Reason { get; }      // Why it failed (if applicable)
   }
   ```

3. **OutputMessageBuilder** - Orchestrates field builders:
   - Calls each field builder in sequence
   - Short-circuits on `DeadLetter` or `Drop` status
   - Aggregates results into final `BuildOutcome`

#### Field Builders

Each builder implements `IOutputFieldBuilder<T>` and independently decides the message outcome:

| Builder | Responsibility | Ok Condition | DeadLetter | Drop |
|---------|---|---|---|---|
| **OutputIdBuilder** | Validate message ID | Non-empty ID | Missing ID | — |
| **ProcessedContentBuilder** | Transform content | Non-empty content | — | Empty content |
| **ProcessedAtBuilder** | Add timestamp | Always succeeds | — | — |
| **ProcessorNameBuilder** | Add processor metadata | Always succeeds | — | — |

Example: If `OutputIdBuilder` returns `DeadLetter("Missing message id")`, the entire message is sent to the dead-letter queue with that reason.

### Message Flow

```
Input Message
       ↓
[MessageHandler consumes from input-topic]
       ↓
[OutputMessageBuilder.Build(input)]
       ├─→ OutputIdBuilder.Build(input)
       │   ├─→ DeadLetter? ──→ Send to dead-letter-topic
       │   └─→ Ok? Continue ↓
       ├─→ ProcessedContentBuilder.Build(input)
       │   ├─→ Drop? ──→ Log & discard (no persistence)
       │   ├─→ DeadLetter? ──→ Send to dead-letter-topic
       │   └─→ Ok? Continue ↓
       ├─→ ProcessedAtBuilder.Build(input)
       │   └─→ Ok? Continue ↓
       ├─→ ProcessorNameBuilder.Build(input)
       │   └─→ Ok? Continue ↓
       ↓
[BuildOutcome with status & message/reason]
       ├─→ Ok: Send OutputMessage to output-topic
       ├─→ DeadLetter: Send DeadLetterMessage to dead-letter-topic
       └─→ Drop: Log warning & discard
```

---

## KafkaFlow Integration

KafkaFlow is a .NET library that simplifies Kafka producer/consumer implementation:

### Configuration (Program.cs)

```csharp
services.AddKafka(kafka => kafka
    .AddCluster(cluster => cluster
        .WithBrokers(new[] { "localhost:9092" })
        
        // Consumer: Reads from input-topic
        .AddConsumer(consumer => consumer
            .Topic("input-topic")
            .WithGroupId("message-processor-group")
            .WithBufferSize(100)
            .WithWorkersCount(10)
            .AddMiddlewares(middlewares => middlewares
                .AddDeserializer<JsonCoreDeserializer>()
                .AddTypedHandlers(handlers => handlers
                    .AddHandler<MessageHandler>()
                )
            )
        )
        
        // Producer: Sends to output-topic
        .AddProducer<OutputMessage>(producer => producer
            .DefaultTopic("output-topic")
            .AddMiddlewares(middlewares => middlewares
                .AddSerializer<JsonCoreSerializer>()
            )
        )
        
        // Dead Letter Producer: Sends to dead-letter-topic
        .AddProducer<DeadLetterMessage>(producer => producer
            .DefaultTopic("dead-letter-topic")
            .AddMiddlewares(middlewares => middlewares
                .AddSerializer<JsonCoreSerializer>()
            )
        )
    )
);
```

### Message Handler

The `MessageHandler` consumes `InputMessage` objects and uses builders to process them:

```csharp
public class MessageHandler : IMessageHandler<InputMessage>
{
    public async Task Handle(IMessageContext context, InputMessage message)
    {
        var outcome = _outputMessageBuilder.Build(message);
        
        switch (outcome.Status)
        {
            case BuildStatus.Ok:
                await _producer.ProduceAsync(message.Id, outcome.Message!);
                break;
            case BuildStatus.DeadLetter:
                await _deadLetterProducer.ProduceAsync(message.Id, dlqMessage);
                break;
            case BuildStatus.Drop:
                _logger.LogWarning("Message dropped: {Reason}", outcome.Reason);
                break;
        }
    }
}
```

---

## Data-Type Filtering (Redis)

Records are filtered by **data type** before any processing. Each data type has a setting in Redis
with a `dataTypeId` and an `isActive` flag; only records whose data type is active continue through
the pipeline.

- **Data type id source**: the Kafka message **header** `data-type-id` (configurable), falling back
  to the message **key**.
- **Filter rule**: a record is *filtered* (dropped, logged, offset committed) when its data type is
  inactive, unknown (no setting), or the id is missing. This is a distinct outcome from a content
  `Drop`, and is counted separately via `messages_filtered_total{reason=...}` where `reason` is a
  bounded category (`inactive_data_type`, `unknown_data_type`, `missing_data_type_id`).
- **Repository pattern**: `IDataTypeSettingsRepository` exposes `FindAllAsync` / `FindByIdAsync`.
  The Redis implementation (`RedisDataTypeSettingsRepository`) serves reads from an in-memory cache
  and reloads from Redis only after a TTL (`DataTypeSettings:RefreshSeconds`, default 15s) has
  passed, so the hot path never hits Redis per message. Uncached (`UseCache: false`) issues a `GET`
  per lookup instead.
- **Filter location**: the check runs at the start of `OutputMessageBuilder.Build(...)`.

### Redis storage

Each setting is stored under its **own Redis String key** `{DataTypeSettings:KeyPrefix}{dataTypeId}`
(prefix default `datatypesettings:`) holding a JSON object (a bare boolean is also accepted):

```bash
# one key per data type
redis-cli SET datatypesettings:news '{"dataTypeId":"news","isActive":false}'
redis-cli SET datatypesettings:weather '{"dataTypeId":"weather","isActive":true}'
```

The cache reload enumerates keys with `SCAN datatypesettings:*` and fetches them with `MGET`; an
uncached lookup is a single `GET datatypesettings:{id}`.

### Producing records with a data type id

```bash
# key carries the data type id (key.separator avoids the ':' inside JSON)
printf '%s\n' \
  'weather|{"Id":"m1","Content":"sunny"}' \
  'news|{"Id":"m2","Content":"headline"}' \
  | docker exec -i broker kafka-console-producer --bootstrap-server broker:9092 \
      --topic input-topic --property parse.key=true --property key.separator='|'
```

Configuration (`appsettings.json`):

```json
"Redis": { "ConnectionString": "localhost:6379" },
"DataTypeSettings": { "HashKey": "datatype:settings", "UseCache": true, "RefreshSeconds": 15, "HeaderName": "data-type-id" }
```

Run Redis with the rest of the stack: `docker compose up -d redis`.

### Cache vs. per-lookup

`UseCache` toggles how the Redis repository serves reads:

- **`true`** (default) — reads come from an in-memory snapshot, reloaded via `HGETALL` only after the
  TTL; the hot path never hits Redis per message.
- **`false`** — every lookup issues a Redis `HGET`. Useful for measuring the cost of caching.

Every Redis round-trip is timed via `redis_operations_total{operation,status}` and
`redis_operation_duration_milliseconds`, so Redis latency/throughput bottlenecks are visible on the
Grafana dashboard. A load-test comparison of the two modes is in `REDIS_CACHE_LOADTEST.pdf`
(cached issued 3 Redis ops for 50k messages vs. 50,000 uncached).

---

## Consumer Worker Tuning

The consumer's parallelism is configurable:

| Setting | Meaning |
|---------|---------|
| `Kafka:WorkersCount` | Number of worker loops (threads/tasks) processing messages in parallel. Workers are **not** partition consumers — one Kafka consumer fetches and the distribution strategy routes each message to a worker. |
| `Kafka:BufferSize` | Per-worker bounded prefetch channel. Total in-flight ≈ `WorkersCount × BufferSize`. |
| `Benchmark:WorkMicros` | Synthetic CPU work per message (µs) for load testing; `0` = disabled (no effect on normal runs). |

The consumer uses **`FreeWorkerDistributionStrategy`** because this system is **keyless**. With the
default `BytesSum` strategy, null-key messages all hash to worker 0 — i.e. single-threaded regardless
of `WorkersCount`. `FreeWorker` routes each message to any free worker (no per-key ordering).

A load-test sweep (`WORKERS_BUFFER_TUNING.pdf`) found: throughput scales with workers up to ≈ the
host core count, then flattens while per-message latency rises (CPU oversubscription); buffer size has
negligible effect on a keyless/CPU-bound workload. **Rule of thumb: set `WorkersCount` ≈ the pod's CPU
allotment, keep `BufferSize` ~100.**

---

## Health & Resilience

The processor is designed to fail loudly and stay serving through transient Redis blips:

- **Fail-fast startup.** The settings refresh service loads the snapshot before the consumer starts; if
  Redis is unreachable or the initial load fails, `StartAsync` throws and **the host fails to start**
  (non-zero exit) so Kubernetes restarts the pod. It never begins consuming without settings.
- **Stale-tolerant refresh.** A background worker reloads the snapshot every `RefreshSeconds`. A failed
  refresh **keeps the previous snapshot**, logs a warning, and increments
  `datatype_settings_load_failures_total` — it never clears settings (so a data type isn't wrongly
  treated as inactive because a reload failed).
- **No hammering.** In cached mode the message path reads only the in-memory snapshot, so a Redis
  outage produces **zero** per-message Redis calls regardless of throughput.

### Kubernetes probes

Exposed on the same port as `/metrics`:

| Endpoint | Checks | Fail behavior |
|----------|--------|---------------|
| `GET /health/live` | process only (no dependencies) | a Redis blip won't get the pod killed |
| `GET /health/ready` | Redis reachable (PING) + settings snapshot loaded/fresh | reports NotReady (503) during a Redis outage; Degraded while serving a stale snapshot |

```yaml
livenessProbe:  { httpGet: { path: /health/live,  port: 8080 }, periodSeconds: 10 }
readinessProbe: { httpGet: { path: /health/ready, port: 8080 }, periodSeconds: 10 }
```

## Observability (OpenTelemetry + Prometheus + Grafana)

The processor is an ASP.NET Core host that runs the KafkaFlow consumer/producers **and** exposes an
OpenTelemetry-backed metrics endpoint for Prometheus to scrape.

### Metrics endpoint

- **`GET http://localhost:8080/metrics`** — Prometheus exposition format (OpenTelemetry `AddPrometheusExporter`).
- Port is configurable via `Metrics:Port` in `appsettings.json`.

Exported metrics include:

| Metric | Type | Meaning |
|--------|------|---------|
| `messages_processed_total` | counter | Messages sent to the output topic |
| `messages_dead_lettered_total` | counter | Messages routed to the dead-letter topic |
| `messages_dropped_total` | counter | Messages dropped (empty content) |
| `messages_filtered_total{reason}` | counter | Messages filtered by data-type settings (`inactive_data_type` / `unknown_data_type` / `missing_data_type_id`) |
| `messages_processing_duration_milliseconds` | histogram | Per-message handling latency (p50/p95/p99) |
| `redis_operations_total{operation,status}` | counter | Redis round-trips (`get` / `load_all`, `ok` / `error`) |
| `redis_operation_duration_milliseconds` | histogram | Redis round-trip latency |
| `datatype_settings_reloads_total` / `datatype_settings_load_failures_total` | counter | Successful vs. failed settings reloads from Redis |
| `datatype_settings_snapshot_size` / `datatype_settings_since_load_seconds` | gauge | Snapshot entry count; seconds since last successful load |
| `kafka_consumer_lag` / `kafka_consumer_assignment_partitions` / `kafka_consumer_fetchq_messages` | gauge | From librdkafka statistics: total lag, assigned partitions, fetch-queue depth |
| `kafka_consumer_rebalances_total` / `kafka_consumer_rx_messages_total` | counter | Rebalance count; messages received from brokers |
| `kafka_broker_rtt_avg_milliseconds` / `kafka_broker_rtt_max_milliseconds` | gauge | Broker round-trip time (from statistics) |
| `dotnet_*` | various | .NET runtime instrumentation (GC, memory, CPU, threads) |
| KafkaFlow OpenTelemetry | traces | Consumer/producer spans (KafkaFlow exports traces, not metrics — Kafka metrics above come from librdkafka statistics) |

Traces and metrics are also pushed via OTLP to the Elastic APM server (`OpenTelemetry:OtlpEndpoint`) when it is running.

### Running the observability stack

```bash
cd Processor

# 1. Start Kafka + Prometheus + Grafana (skip the heavier ELK stack)
docker compose up -d zookeeper broker prometheus grafana

# 2. Run the processor (exposes /metrics on :8080)
dotnet run --project .

# 3. Produce a few messages (note: JSON keys are PascalCase — Id / Content)
printf '%s\n' \
  '{"Id":"msg-001","Content":"hello kafka"}' \
  '{"Id":"","Content":"missing id -> dead letter"}' \
  '{"Id":"msg-002","Content":""}' \
  | docker exec -i broker kafka-console-producer --bootstrap-server broker:9092 --topic input-topic
```

Then open:

- **Metrics**: <http://localhost:8080/metrics>
- **Prometheus**: <http://localhost:9090> (target `kafkaflow-processor` should be `UP` under Status → Targets)
- **Grafana**: <http://localhost:3000> — anonymous access is enabled; the **KafkaFlow Processor** dashboard is
  auto-provisioned with a Prometheus datasource.

Prometheus reaches the host-run app via `host.docker.internal:8080` (configured in `prometheus/prometheus.yml`).
In Kubernetes, drop this static target and let Prometheus scrape the pod's `/metrics` endpoint directly.

---

## Building & Testing

### Building the Project

```bash
# Restore NuGet packages and compile
dotnet build

# Run the application
dotnet run --project Processor
```

### Unit Testing Strategy

Tests are organized into three categories:

#### 1. **Field Builder Unit Tests** (`Processor.Tests/Builders/`)

Isolated tests for each field builder logic:

```csharp
[Fact]
public void OutputIdBuilder_ReturnsDeadLetter_WhenIdIsMissing()
{
    var builder = new OutputIdBuilder();
    var input = new InputMessage { Id = " ", Content = "hello" };
    
    var result = builder.Build(input);
    
    Assert.Equal(BuildStatus.DeadLetter, result.Status);
    Assert.Equal("Missing message id", result.Reason);
}
```

**Benefits:**
- ✅ Fast execution (milliseconds)
- ✅ Test individual business logic in isolation
- ✅ Easy to understand and maintain
- ✅ No external dependencies (no Kafka, mocking)

#### 2. **Integration Tests** (`Processor.Tests/MessageHandlerTests.cs`)

Data-driven tests using JSON test cases to verify the complete message processing pipeline:

```csharp
[Theory]
[MemberData(nameof(TestDataLoader.TestCases), MemberType = typeof(TestDataLoader))]
public async Task Handle_ProcessesMessageCorrectly(string fileName, string fileContent)
{
    var data = DeserializeTestData(fileName, fileContent);
    
    // Creates mocks for producers
    var mockProducer = new Mock<IMessageProducer<OutputMessage>>();
    var mockDLQProducer = new Mock<IMessageProducer<DeadLetterMessage>>();
    
    // Executes handler
    await handler.Handle(mockContext.Object, data.Input!);
    
    // Verifies based on expected outcome
    switch (data.ExpectedOutcome)
    {
        case "output":
            mockProducer.Verify(p => p.ProduceAsync(...), Times.Once);
            break;
        case "deadletter":
            mockDLQProducer.Verify(p => p.ProduceAsync(...), Times.Once);
            break;
        case "dropped":
            mockProducer.Verify(p => p.ProduceAsync(...), Times.Never);
            break;
    }
}
```

**Benefits:**
- ✅ Test complete message flow
- ✅ Data-driven approach (separate test data from test logic)
- ✅ Easy to add new test cases
- ✅ Verifies mocked Kafka producers were called correctly

### Data-Driven Tests

Test cases are defined in **JSON files** in `Processor.Tests/TestsData/`:

#### Output Case (test_case_1.json)
```json
{
  "input": {
    "id": "msg-001",
    "content": "hello kafka"
  },
  "expectedOutcome": "output",
  "expectedOutput": {
    "id": "msg-001",
    "processedContent": "HELLO KAFKA"
  }
}
```

#### Dead Letter Case (test_case_deadletter.json)
```json
{
  "input": {
    "id": "",
    "content": "invalid message id"
  },
  "expectedOutcome": "deadletter",
  "expectedDeadLetterReason": "Missing message id",
  "expectedOutput": null
}
```

#### Drop Case (test_case_dropped.json)
```json
{
  "input": {
    "id": "msg-drop-001",
    "content": ""
  },
  "expectedOutcome": "dropped",
  "expectedDropReason": "Content is empty",
  "expectedOutput": null
}
```

#### Test Data Loading (TestDataLoader.cs)

```csharp
public static IEnumerable<object[]> TestCases
{
    get
    {
        var testFiles = Directory.GetFiles(testDataDir, "test_case_*.json")
            .OrderBy(f => f);
        
        foreach (var file in testFiles)
        {
            var fileName = Path.GetFileName(file);
            var fileContent = File.ReadAllText(file);
            
            // Yield raw strings to enable test discovery
            yield return new object[] { fileName, fileContent };
        }
    }
}
```

**Key Design Decisions:**
- **Primitives Only**: Object arrays contain only primitives (`string`, `int`) to enable xUnit test enumeration
- **Lazy Deserialization**: JSON deserialization happens inside the test method
- **Automatic Discovery**: Tests are discovered and enumerated individually per JSON file
- **Auto-Cleanup**: `CleanTestData` MSBuild target removes stale test files from output directory

### VS Code Test Explorer

The project is fully integrated with VS Code's Test Explorer, providing a modern IDE experience:

#### Features

✅ **Individual Test Discovery**
- Each JSON test case appears as a separate, individually runnable test
- Tests are enumerated with their file names for easy identification
- No test aggregation or grouped execution

✅ **Easy Navigation**
- Click on any test to jump directly to the test method
- View test results in the inline code editor
- Hover over test results for detailed error messages

✅ **Advanced Filtering**
- Run/debug tests by outcome type (filter by `deadletter`, `dropped`, etc.)
- Run only builder unit tests
- Run full integration suite

#### How It Works

The key to proper test enumeration is using **primitive types in test data**:

```csharp
// ✅ Works: Primitives allow enumeration
yield return new object[] { fileName, fileContent };

// ❌ Doesn't enumerate: Complex objects prevent discovery
yield return new object[] { new TestData { ... } };
```

When xUnit encounters primitive types (`string`, `int`, `DateTime`), it enumerates each test case and displays them individually in Test Explorer. Complex objects prevent this discovery.

---

## Running Tests

```bash
# Run all tests
dotnet test

# Run only builder unit tests
dotnet test Processor.Tests/Builders/

# Run with verbosity
dotnet test -v normal

# List all discovered tests
dotnet test --list-tests
```

### Example Test Output

```
The following Tests are available:
    Handle ProcessesMessageCorrectly(fileName: "test_case_1.json", ...)
    Handle ProcessesMessageCorrectly(fileName: "test_case_2.json", ...)
    Handle ProcessesMessageCorrectly(fileName: "test_case_deadletter.json", ...)
    Handle ProcessesMessageCorrectly(fileName: "test_case_dropped.json", ...)
    OutputIdBuilder_ReturnsOk_WhenIdIsPresent
    OutputIdBuilder_ReturnsDeadLetter_WhenIdIsMissing
    ProcessedContentBuilder_ReturnsOk_WhenContentIsPresent
    ProcessedContentBuilder_ReturnsDrop_WhenContentIsMissing
```

---

## Key Design Patterns

### 1. Builder Pattern for Separation of Concerns
Each output field has its own builder class with:
- Single responsibility (compute one field)
- Independent validation logic
- Ability to decide message routing (`Ok`/`DeadLetter`/`Drop`)
- Unit testable in isolation

### 2. Orchestrator Pattern
`OutputMessageBuilder` coordinates field builders:
- Invokes builders sequentially
- Handles outcome aggregation
- Short-circuits on failure
- Returns unified result

### 3. Data-Driven Testing
Test cases live in JSON files:
- Easy to add new scenarios without code changes
- Business requirements readable by non-developers
- Test data versioned alongside code
- Simple to generate test reports

---

## Dependencies

- **KafkaFlow**: 4.1.0 - Kafka consumer/producer framework
- **xUnit**: 2.9.3 - Testing framework
- **Moq**: 4.20.72 - Mocking library
- **.NET**: 9.0

---

## Contributing

When adding new builders:
1. Create class in `Processor/Builders/FieldBuilders/`
2. Implement `IOutputFieldBuilder<T>`
3. Add unit tests in `Processor.Tests/Builders/`
4. Register in `Program.cs` DI container
5. Integrate into `OutputMessageBuilder`

---

## License

MIT
