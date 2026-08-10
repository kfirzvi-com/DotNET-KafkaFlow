using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Processor.Core.Messages;
using Xunit.Sdk;

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
    /// The complete contents of the settings store for this case — every data type the processor should
    /// see, and whether each is active. Each case owns its own store (a dedicated Oracle table in the
    /// host suite, a dedicated in-memory store in the mock suite), so these never collide with another
    /// case's.
    /// <para>
    /// A case exercises the filter outcomes purely through this list: omit
    /// <see cref="DataTypeId"/> from it for <c>unknown_data_type</c>, include it with
    /// <c>isActive: false</c> for <c>inactive_data_type</c>. Listing unrelated data types alongside is
    /// meaningful too — it proves the processor selects by id rather than taking whatever is present.
    /// </para>
    /// </summary>
    public List<DataTypeSettingFixture> DataTypeSettings { get; set; } = new();

    public TInput? Input { get; set; }

    /// <summary>One of <c>output</c>, <c>deadletter</c>, <c>dropped</c>, <c>filtered</c>.</summary>
    public string ExpectedOutcome { get; set; } = "output";

    public ExpectedOutput<TDomainData>? ExpectedOutput { get; set; }

    public string? ExpectedDeadLetterReason { get; set; }

    public string? ExpectedDropReason { get; set; }

    public string? ExpectedFilterReason { get; set; }

    /// <summary>The setting for <paramref name="dataTypeId"/>, or null when the case does not declare one.</summary>
    public DataTypeSettingFixture? SettingFor(string? dataTypeId) =>
        dataTypeId is null
            ? null
            : DataTypeSettings.FirstOrDefault(
                s => string.Equals(s.DataTypeId, dataTypeId, StringComparison.OrdinalIgnoreCase));
}

/// <summary>
/// One row of a case's settings store: a data type id and whether it is active. Mirrors
/// <c>DATA_TYPE_SETTINGS</c> without depending on the production type, so the fixture format stays a
/// test concern.
/// </summary>
public class DataTypeSettingFixture
{
    public string DataTypeId { get; set; } = string.Empty;

    /// <summary>Defaults to true: a listed data type is normally one the processor should accept.</summary>
    public bool IsActive { get; set; } = true;
}

/// <summary>The bounded filter reason codes a case can expect.</summary>
public static class FilterReasons
{
    public const string MissingDataTypeId = "missing_data_type_id";
    public const string UnknownDataType = "unknown_data_type";
    public const string InactiveDataType = "inactive_data_type";
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

        ValidateDataTypeSettings(fileName, testCase, outcome);

        return testCase;
    }

    /// <summary>
    /// Checks a case's settings list against the outcome it expects. Without this a fixture could
    /// silently test something other than what it claims — forgetting to list a data type would turn an
    /// "output" case into an unnoticed <c>unknown_data_type</c> filter that still passed its assertions
    /// (nothing produced, nothing dead-lettered) for entirely the wrong reason.
    /// </summary>
    private static void ValidateDataTypeSettings<TInput, TDomainData>(
        string fileName, ProcessorTestCase<TInput, TDomainData> testCase, string outcome)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        var settings = testCase.DataTypeSettings;

        if (settings.Any(s => string.IsNullOrWhiteSpace(s.DataTypeId)))
        {
            throw new InvalidOperationException(
                $"{fileName}: 'dataTypeSettings' contains an entry with a blank 'dataTypeId'.");
        }

        var duplicates = settings
            .GroupBy(s => s.DataTypeId, StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        if (duplicates.Count > 0)
        {
            throw new InvalidOperationException(
                $"{fileName}: 'dataTypeSettings' lists {string.Join(", ", duplicates)} more than once; " +
                "the store is keyed by data type id, so a duplicate has no defined meaning.");
        }

        var setting = testCase.SettingFor(testCase.DataTypeId);

        // Anything that got past the filter must have had an active data type to get there.
        if (outcome is TestOutcomes.Output or TestOutcomes.DeadLetter or TestOutcomes.Dropped)
        {
            if (testCase.DataTypeId is null)
            {
                throw new InvalidOperationException(
                    $"{fileName}: outcome '{outcome}' requires a 'dataTypeId' — a message without one is filtered.");
            }

            if (setting is null || !setting.IsActive)
            {
                throw new InvalidOperationException(
                    $"{fileName}: outcome '{outcome}' requires 'dataTypeSettings' to list '{testCase.DataTypeId}' " +
                    $"as active, otherwise the message is filtered before it can be {outcome}.");
            }

            return;
        }

        // Filtered cases: the settings must actually produce the reason claimed.
        switch (testCase.ExpectedFilterReason)
        {
            case FilterReasons.MissingDataTypeId when testCase.DataTypeId is not null:
                throw new InvalidOperationException(
                    $"{fileName}: '{FilterReasons.MissingDataTypeId}' requires 'dataTypeId' to be null.");

            case FilterReasons.UnknownDataType when setting is not null:
                throw new InvalidOperationException(
                    $"{fileName}: '{FilterReasons.UnknownDataType}' requires 'dataTypeSettings' NOT to list " +
                    $"'{testCase.DataTypeId}'.");

            case FilterReasons.InactiveDataType when setting is null:
                throw new InvalidOperationException(
                    $"{fileName}: '{FilterReasons.InactiveDataType}' requires 'dataTypeSettings' to list " +
                    $"'{testCase.DataTypeId}' with 'isActive': false.");

            case FilterReasons.InactiveDataType when setting.IsActive:
                throw new InvalidOperationException(
                    $"{fileName}: '{FilterReasons.InactiveDataType}' requires '{testCase.DataTypeId}' to be " +
                    "inactive, but it is listed as active.");
        }
    }

    /// <summary>
    /// Write options for the comparison forms. camelCase so the printed JSON matches the casing used in
    /// the test-case files and can be eyeballed against them directly.
    /// </summary>
    private static readonly JsonSerializerOptions CanonicalOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true
    };

    /// <summary>
    /// Asserts two payloads match, reporting <em>every</em> difference by JSON path.
    /// <para>
    /// Comparing canonical JSON strings with <c>Assert.Equal</c> was not good enough: it reports only
    /// the first differing character, in a truncated single-line window, with no field name — so a
    /// payload with three wrong fields showed one of them and left you counting characters.
    /// </para>
    /// </summary>
    /// <param name="subject">What is being compared, e.g. the test-case file name.</param>
    public static void AssertMatches<T>(T expected, T actual, string subject)
    {
        var expectedNode = Sort(JsonSerializer.SerializeToNode(expected, CanonicalOptions));
        var actualNode = Sort(JsonSerializer.SerializeToNode(actual, CanonicalOptions));

        var differences = new List<string>();
        Compare(path: string.Empty, expectedNode, actualNode, differences);

        if (differences.Count == 0)
        {
            return;
        }

        var report = new StringBuilder();
        report.AppendLine($"{subject} does not match the expected payload.");
        report.AppendLine();
        report.AppendLine(differences.Count == 1 ? "1 difference:" : $"{differences.Count} differences:");
        foreach (var difference in differences)
        {
            report.AppendLine($"  {difference}");
        }

        report.AppendLine();
        report.AppendLine("expected:");
        report.AppendLine(Indent(expectedNode));
        report.AppendLine("actual:");
        report.AppendLine(Indent(actualNode));

        throw new XunitException(report.ToString().TrimEnd());
    }

    /// <summary>Canonical JSON for a payload — property order normalized, camelCase, indented.</summary>
    public static string Canonical<T>(T value) =>
        Indent(Sort(JsonSerializer.SerializeToNode(value, CanonicalOptions)));

    private static string Indent(JsonNode? node) =>
        node?.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) ?? "null";

    /// <summary>Walks both trees together, collecting one line per difference found.</summary>
    private static void Compare(string path, JsonNode? expected, JsonNode? actual, List<string> differences)
    {
        var label = string.IsNullOrEmpty(path) ? "(root)" : path;

        if (expected is null && actual is null)
        {
            return;
        }

        if (expected is null || actual is null)
        {
            differences.Add($"{label}: expected {Describe(expected)} but was {Describe(actual)}");
            return;
        }

        switch (expected)
        {
            case JsonObject expectedObject when actual is JsonObject actualObject:
            {
                var keys = expectedObject.Select(p => p.Key)
                    .Union(actualObject.Select(p => p.Key), StringComparer.Ordinal)
                    .OrderBy(k => k, StringComparer.Ordinal);

                foreach (var key in keys)
                {
                    var childPath = string.IsNullOrEmpty(path) ? key : $"{path}.{key}";
                    var hasExpected = expectedObject.TryGetPropertyValue(key, out var expectedChild);
                    var hasActual = actualObject.TryGetPropertyValue(key, out var actualChild);

                    if (!hasExpected)
                    {
                        differences.Add($"{childPath}: unexpected property, was {Describe(actualChild)}");
                    }
                    else if (!hasActual)
                    {
                        differences.Add($"{childPath}: expected {Describe(expectedChild)} but the property is absent");
                    }
                    else
                    {
                        Compare(childPath, expectedChild, actualChild, differences);
                    }
                }

                return;
            }

            case JsonArray expectedArray when actual is JsonArray actualArray:
            {
                if (expectedArray.Count != actualArray.Count)
                {
                    differences.Add(
                        $"{label}: expected {expectedArray.Count} item(s) but was {actualArray.Count}");
                }

                for (var i = 0; i < Math.Max(expectedArray.Count, actualArray.Count); i++)
                {
                    var childPath = $"{path}[{i}]";

                    if (i >= expectedArray.Count)
                    {
                        differences.Add($"{childPath}: unexpected item {Describe(actualArray[i])}");
                    }
                    else if (i >= actualArray.Count)
                    {
                        differences.Add($"{childPath}: expected {Describe(expectedArray[i])} but it is missing");
                    }
                    else
                    {
                        Compare(childPath, expectedArray[i], actualArray[i], differences);
                    }
                }

                return;
            }

            default:
            {
                // Values (and container/value type mismatches) compare by their JSON text.
                var expectedText = expected.ToJsonString();
                var actualText = actual.ToJsonString();
                if (!string.Equals(expectedText, actualText, StringComparison.Ordinal))
                {
                    differences.Add($"{label}: expected {expectedText} but was {actualText}");
                }

                return;
            }
        }
    }

    /// <summary>Short description of a node for a difference line.</summary>
    private static string Describe(JsonNode? node) => node switch
    {
        null => "null",
        JsonObject obj => $"an object with {obj.Count} property(ies)",
        JsonArray array => $"an array of {array.Count} item(s)",
        _ => node.ToJsonString()
    };

    private static JsonNode? Sort(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject obj:
            {
                var sorted = new JsonObject();
                foreach (var property in obj.OrderBy(p => p.Key, StringComparer.Ordinal))
                {
                    sorted[property.Key] = Sort(property.Value?.DeepClone());
                }

                return sorted;
            }
            case JsonArray array:
            {
                var mapped = new JsonArray();
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
