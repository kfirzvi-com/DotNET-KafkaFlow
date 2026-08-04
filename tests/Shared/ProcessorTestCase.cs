using System.Text.Json;
using System.Text.Json.Serialization;
using Processor.Core.Messages;

namespace Processor.Tests.Shared;

/// <summary>
/// One JSON test case, parameterized by the domain under test so a single file format drives both
/// domains and both end-to-end suites. The generic parameters are what let <c>Input</c> deserialize
/// straight into the domain's real message type — no per-domain test-case classes.
/// </summary>
public class ProcessorTestCase<TInput, TDomainData>
    where TInput : InputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    /// <summary>
    /// Data type id sent with the message (Kafka key). Null means the message is produced with no
    /// key and no header, which exercises the <c>missing_data_type_id</c> filter.
    /// </summary>
    public string? DataTypeId { get; set; }

    /// <summary>
    /// Whether the settings store has a row for <see cref="DataTypeId"/>. False exercises the
    /// <c>unknown_data_type</c> filter.
    /// </summary>
    public bool DataTypeRegistered { get; set; } = true;

    /// <summary>Whether that row is active. False exercises the <c>inactive_data_type</c> filter.</summary>
    public bool DataTypeActive { get; set; } = true;

    public TInput? Input { get; set; }

    /// <summary>One of <c>output</c>, <c>deadletter</c>, <c>dropped</c>, <c>filtered</c>.</summary>
    public string ExpectedOutcome { get; set; } = "output";

    public ExpectedOutput<TDomainData>? ExpectedOutput { get; set; }

    public string? ExpectedDeadLetterReason { get; set; }

    public string? ExpectedDropReason { get; set; }

    public string? ExpectedFilterReason { get; set; }
}

/// <summary>
/// The parts of the output message a test case asserts on. Deliberately not the full
/// <see cref="OutputMessage{TDomainData}"/>: <c>ProcessedAt</c> and <c>ProcessorName</c> are
/// environment-dependent, so asserting them would make the cases machine-specific.
/// </summary>
public class ExpectedOutput<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    public string? Id { get; set; }

    public string? ProcessedContent { get; set; }

    /// <summary>Expected processed payload. Compared field-by-field via canonical JSON.</summary>
    public TDomainData? DomainData { get; set; }
}

/// <summary>Outcome names used in the JSON files.</summary>
public static class TestOutcomes
{
    public const string Output = "output";
    public const string DeadLetter = "deadletter";
    public const string Dropped = "dropped";
    public const string Filtered = "filtered";
}

/// <summary>Shared JSON handling for the test-case files.</summary>
public static class TestCaseJson
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never
    };

    /// <summary>
    /// Parses a test-case file, failing loudly on a malformed or internally inconsistent case — a
    /// broken fixture must not read as a passing test.
    /// </summary>
    public static ProcessorTestCase<TInput, TDomainData> Parse<TInput, TDomainData>(
        string fileName, string fileContent)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        ProcessorTestCase<TInput, TDomainData>? testCase;
        try
        {
            testCase = JsonSerializer.Deserialize<ProcessorTestCase<TInput, TDomainData>>(fileContent, Options);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"{fileName}: error parsing JSON: {ex.Message}", ex);
        }

        if (testCase?.Input is null)
        {
            throw new InvalidOperationException($"{fileName}: invalid test data — missing 'input'.");
        }

        var outcome = testCase.ExpectedOutcome.ToLowerInvariant();

        switch (outcome)
        {
            case TestOutcomes.Output when testCase.ExpectedOutput is null:
                throw new InvalidOperationException(
                    $"{fileName}: 'expectedOutput' is required for the 'output' outcome.");
            case TestOutcomes.DeadLetter when string.IsNullOrWhiteSpace(testCase.ExpectedDeadLetterReason):
                throw new InvalidOperationException(
                    $"{fileName}: 'expectedDeadLetterReason' is required for the 'deadletter' outcome.");
            case TestOutcomes.Dropped when string.IsNullOrWhiteSpace(testCase.ExpectedDropReason):
                throw new InvalidOperationException(
                    $"{fileName}: 'expectedDropReason' is required for the 'dropped' outcome.");
            case TestOutcomes.Filtered when string.IsNullOrWhiteSpace(testCase.ExpectedFilterReason):
                throw new InvalidOperationException(
                    $"{fileName}: 'expectedFilterReason' is required for the 'filtered' outcome.");
            case TestOutcomes.Output:
            case TestOutcomes.DeadLetter:
            case TestOutcomes.Dropped:
            case TestOutcomes.Filtered:
                break;
            default:
                throw new InvalidOperationException($"{fileName}: unknown expected outcome '{outcome}'.");
        }

        return testCase;
    }

    /// <summary>
    /// Canonical JSON for comparing domain payloads. Property order is normalized, so an assertion
    /// failure prints a readable diff of the whole payload instead of a per-field cascade.
    /// </summary>
    public static string Canonical<T>(T value)
    {
        var json = JsonSerializer.SerializeToNode(value, Options);
        return Sort(json)?.ToJsonString() ?? "null";
    }

    private static System.Text.Json.Nodes.JsonNode? Sort(System.Text.Json.Nodes.JsonNode? node)
    {
        switch (node)
        {
            case System.Text.Json.Nodes.JsonObject obj:
            {
                var sorted = new System.Text.Json.Nodes.JsonObject();
                foreach (var property in obj.OrderBy(p => p.Key, StringComparer.Ordinal))
                {
                    sorted[property.Key] = Sort(property.Value?.DeepClone());
                }

                return sorted;
            }
            case System.Text.Json.Nodes.JsonArray array:
            {
                var mapped = new System.Text.Json.Nodes.JsonArray();
                foreach (var item in array)
                {
                    mapped.Add(Sort(item?.DeepClone()));
                }

                return mapped;
            }
            default:
                return node;
        }
    }
}
