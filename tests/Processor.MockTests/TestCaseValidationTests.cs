using Processor.Domains.Posts;
using Processor.Tests.Shared;

namespace Processor.MockTests;

/// <summary>
/// Tests for the fixture consistency rules. These matter because an inconsistent fixture can pass its
/// own assertions for entirely the wrong reason: forget to list a data type on an "output" case and the
/// message is filtered instead, at which point "nothing produced, nothing dead-lettered" is true — so
/// the case has to be rejected at parse time rather than quietly testing the filter.
/// <para>
/// Fixture JSON is written with single quotes and converted, purely so the expected shapes stay
/// readable here instead of drowning in escapes.
/// </para>
/// </summary>
public class TestCaseValidationTests
{
    private const string ValidPostInput =
        "'input': { 'id': 'post-1', 'content': 'hello', " +
        "'domainData': { 'authorHandle': 'ada', 'network': 'x', 'likes': 1, 'shares': 0 } }";

    private const string OutputTail =
        "'expectedOutcome': 'output', 'expectedOutput': { 'id': 'post-1', 'processedContent': 'HELLO' }";

    private const string DeadLetterTail =
        "'expectedOutcome': 'deadletter', 'expectedDeadLetterReason': 'Missing message id'";

    private const string DroppedTail =
        "'expectedOutcome': 'dropped', 'expectedDropReason': 'Content is empty'";

    private static string FilteredTail(string reason) =>
        $"'expectedOutcome': 'filtered', 'expectedFilterReason': '{reason}'";

    /// <summary>Assembles a fixture from its data type id, settings list and outcome tail.</summary>
    private static string Fixture(string dataTypeId, string settings, string tail) =>
        ("{ 'dataTypeId': " + dataTypeId +
         ", 'dataTypeSettings': " + settings +
         ", " + ValidPostInput +
         ", " + tail + " }")
        .Replace('\'', '"');

    private static ProcessorTestCase<PostInputMessage, PostData> Parse(string json) =>
        TestCaseJson.Parse<PostInputMessage, PostData>("fixture.json", json);

    private static string ParseError(string json) =>
        Assert.Throws<InvalidOperationException>(() => Parse(json)).Message;

    [Fact]
    public void Accepts_AnOutputCase_ThatListsItsDataTypeAsActive()
    {
        var testCase = Parse(Fixture(
            "'feed'", "[{ 'dataTypeId': 'feed', 'isActive': true }]", OutputTail));

        Assert.Equal("feed", testCase.DataTypeId);
        Assert.Single(testCase.DataTypeSettings);
    }

    [Fact]
    public void Accepts_MultipleSettings_AndKeepsThemAll()
    {
        var testCase = Parse(Fixture(
            "'feed'",
            "[{ 'dataTypeId': 'retired', 'isActive': false }, " +
            "{ 'dataTypeId': 'feed', 'isActive': true }, " +
            "{ 'dataTypeId': 'backfill', 'isActive': false }]",
            OutputTail));

        Assert.Equal(3, testCase.DataTypeSettings.Count);
        Assert.True(testCase.SettingFor("feed")!.IsActive);
        Assert.False(testCase.SettingFor("retired")!.IsActive);
        Assert.False(testCase.SettingFor("backfill")!.IsActive);
    }

    [Fact]
    public void IsActive_DefaultsToTrue_WhenOmitted()
    {
        var testCase = Parse(Fixture("'feed'", "[{ 'dataTypeId': 'feed' }]", OutputTail));

        Assert.True(testCase.SettingFor("feed")!.IsActive);
    }

    [Fact]
    public void SettingFor_MatchesCaseInsensitively()
    {
        var testCase = Parse(Fixture("'Feed'", "[{ 'dataTypeId': 'feed' }]", OutputTail));

        Assert.NotNull(testCase.SettingFor("FEED"));
        Assert.Null(testCase.SettingFor("other"));
        Assert.Null(testCase.SettingFor(null));
    }

    [Theory]
    [InlineData("output")]
    [InlineData("deadletter")]
    [InlineData("dropped")]
    public void Rejects_APassingOutcome_WhoseDataTypeIsNotListed(string outcome)
    {
        var tail = outcome switch
        {
            "output" => OutputTail,
            "deadletter" => DeadLetterTail,
            _ => DroppedTail
        };

        var message = ParseError(Fixture("'feed'", "[]", tail));

        Assert.Contains($"outcome '{outcome}' requires 'dataTypeSettings' to list 'feed' as active", message);
    }

    [Fact]
    public void Rejects_AnOutputCase_WhoseDataTypeIsListedInactive()
    {
        var message = ParseError(Fixture(
            "'feed'", "[{ 'dataTypeId': 'feed', 'isActive': false }]", OutputTail));

        Assert.Contains("requires 'dataTypeSettings' to list 'feed' as active", message);
    }

    [Fact]
    public void Rejects_AnOutputCase_WithNoDataTypeId()
    {
        var message = ParseError(Fixture("null", "[{ 'dataTypeId': 'feed' }]", OutputTail));

        Assert.Contains("requires a 'dataTypeId'", message);
    }

    [Fact]
    public void Rejects_MissingDataTypeIdReason_WhenAnIdIsActuallySent()
    {
        var message = ParseError(Fixture(
            "'feed'", "[{ 'dataTypeId': 'feed' }]", FilteredTail("missing_data_type_id")));

        Assert.Contains("requires 'dataTypeId' to be null", message);
    }

    [Fact]
    public void Rejects_UnknownDataTypeReason_WhenTheIdIsListed()
    {
        var message = ParseError(Fixture(
            "'feed'",
            "[{ 'dataTypeId': 'feed', 'isActive': false }]",
            FilteredTail("unknown_data_type")));

        Assert.Contains("requires 'dataTypeSettings' NOT to list 'feed'", message);
    }

    [Fact]
    public void Rejects_InactiveDataTypeReason_WhenTheIdIsAbsent()
    {
        var message = ParseError(Fixture("'feed'", "[]", FilteredTail("inactive_data_type")));

        Assert.Contains("to list 'feed' with 'isActive': false", message);
    }

    [Fact]
    public void Rejects_InactiveDataTypeReason_WhenTheIdIsListedActive()
    {
        var message = ParseError(Fixture(
            "'feed'", "[{ 'dataTypeId': 'feed', 'isActive': true }]", FilteredTail("inactive_data_type")));

        Assert.Contains("to be inactive, but it is listed as active", message);
    }

    [Fact]
    public void Accepts_AValidUnknownDataTypeCase_WithUnrelatedActiveSettings()
    {
        // The strong form of the unknown case: other data types are active, just not this one.
        var testCase = Parse(Fixture(
            "'never-configured'",
            "[{ 'dataTypeId': 'feed' }, { 'dataTypeId': 'backfill' }]",
            FilteredTail("unknown_data_type")));

        Assert.Null(testCase.SettingFor("never-configured"));
        Assert.Equal(2, testCase.DataTypeSettings.Count);
    }

    [Fact]
    public void Accepts_AValidMissingDataTypeIdCase_EvenWithActiveSettings()
    {
        // Settings are irrelevant when the message carries no id at all — worth proving.
        var testCase = Parse(Fixture(
            "null", "[{ 'dataTypeId': 'feed' }]", FilteredTail("missing_data_type_id")));

        Assert.Null(testCase.DataTypeId);
        Assert.Single(testCase.DataTypeSettings);
    }

    [Fact]
    public void Rejects_DuplicateDataTypeIds()
    {
        var message = ParseError(Fixture(
            "'feed'",
            "[{ 'dataTypeId': 'feed' }, { 'dataTypeId': 'FEED', 'isActive': false }]",
            OutputTail));

        Assert.Contains("more than once", message);
    }

    [Theory]
    [InlineData("''")]
    [InlineData("'   '")]
    public void Rejects_ABlankDataTypeIdInTheList(string blank)
    {
        var message = ParseError(Fixture(
            "'feed'",
            "[{ 'dataTypeId': 'feed' }, { 'dataTypeId': " + blank + " }]",
            OutputTail));

        Assert.Contains("blank 'dataTypeId'", message);
    }

    [Fact]
    public void Rejects_AnUnknownOutcome()
    {
        var message = ParseError(Fixture(
            "'feed'", "[{ 'dataTypeId': 'feed' }]", "'expectedOutcome': 'exploded'"));

        Assert.Contains("unknown expected outcome", message);
    }

    [Fact]
    public void Rejects_AFixtureWithNoInput()
    {
        var message = ParseError(
            ("{ 'dataTypeId': 'feed', 'dataTypeSettings': [], " + OutputTail + " }").Replace('\'', '"'));

        Assert.Contains("missing 'input'", message);
    }
}
