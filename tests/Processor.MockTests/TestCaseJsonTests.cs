using Processor.Tests.Shared;
using Xunit.Sdk;

namespace Processor.MockTests;

/// <summary>
/// Tests for the payload comparison every test case relies on. Worth testing in its own right: if
/// <see cref="TestCaseJson.AssertMatches{T}"/> failed to spot a difference, every payload assertion in
/// both end-to-end suites would pass vacuously.
/// </summary>
public class TestCaseJsonTests
{
    private const string Subject = "Payload under test";

    /// <summary>Runs the comparison and returns the failure message, or null when it passed.</summary>
    private static string? Compare(object expected, object actual)
    {
        try
        {
            TestCaseJson.AssertMatches(expected, actual, Subject);
            return null;
        }
        catch (XunitException ex)
        {
            return ex.Message;
        }
    }

    [Fact]
    public void Passes_WhenPayloadsAreIdentical()
    {
        Assert.Null(Compare(
            new { handle = "ada", followers = 10 },
            new { handle = "ada", followers = 10 }));
    }

    [Fact]
    public void Passes_WhenOnlyPropertyOrderDiffers()
    {
        // Canonicalization is the whole reason property order must not matter.
        Assert.Null(Compare(
            new { handle = "ada", followers = 10 },
            new { followers = 10, handle = "ada" }));
    }

    [Fact]
    public void Passes_OnEmptyCollections()
    {
        Assert.Null(Compare(
            new { hashtags = Array.Empty<string>() },
            new { hashtags = Array.Empty<string>() }));
    }

    [Fact]
    public void Reports_ScalarDifference_WithPathAndBothValues()
    {
        var message = Compare(new { followers = 10 }, new { followers = 11 });

        Assert.NotNull(message);
        Assert.Contains(Subject, message);
        Assert.Contains("1 difference:", message);
        Assert.Contains("followers: expected 10 but was 11", message);
    }

    [Fact]
    public void Reports_EveryDifference_NotJustTheFirst()
    {
        // The failure mode of the old string comparison: it stopped at the first differing character.
        var message = Compare(
            new { a = 1, b = 2, c = 3 },
            new { a = 9, b = 8, c = 7 });

        Assert.NotNull(message);
        Assert.Contains("3 differences:", message);
        Assert.Contains("a: expected 1 but was 9", message);
        Assert.Contains("b: expected 2 but was 8", message);
        Assert.Contains("c: expected 3 but was 7", message);
    }

    [Fact]
    public void Reports_StringDifferences_CaseSensitively()
    {
        // The domain builders normalize casing, so a casing regression must not slip through.
        var message = Compare(new { handle = "ada" }, new { handle = "Ada" });

        Assert.NotNull(message);
        Assert.Contains("handle: expected \"ada\" but was \"Ada\"", message);
    }

    [Fact]
    public void Reports_ArrayElementDifference_WithIndexInThePath()
    {
        var message = Compare(
            new { hashtags = new[] { "kafka", "dotnet" } },
            new { hashtags = new[] { "kafka", "netcore" } });

        Assert.NotNull(message);
        Assert.Contains("hashtags[1]: expected \"dotnet\" but was \"netcore\"", message);
    }

    [Fact]
    public void Reports_ArrayOrderDifference()
    {
        // Array order is significant — hashtag order is asserted by the posts cases.
        var message = Compare(
            new { hashtags = new[] { "a", "b" } },
            new { hashtags = new[] { "b", "a" } });

        Assert.NotNull(message);
        Assert.Contains("hashtags[0]", message);
        Assert.Contains("hashtags[1]", message);
    }

    [Fact]
    public void Reports_MissingArrayItem_AndTheLengthChange()
    {
        var message = Compare(
            new { hashtags = new[] { "a", "b" } },
            new { hashtags = new[] { "a" } });

        Assert.NotNull(message);
        Assert.Contains("expected 2 item(s) but was 1", message);
        Assert.Contains("hashtags[1]: expected \"b\" but it is missing", message);
    }

    [Fact]
    public void Reports_ExtraArrayItem()
    {
        var message = Compare(
            new { hashtags = new[] { "a" } },
            new { hashtags = new[] { "a", "b" } });

        Assert.NotNull(message);
        Assert.Contains("expected 1 item(s) but was 2", message);
        Assert.Contains("hashtags[1]: unexpected item \"b\"", message);
    }

    [Fact]
    public void Reports_AbsentProperty()
    {
        var message = Compare(new { handle = "ada", followers = 10 }, new { handle = "ada" });

        Assert.NotNull(message);
        Assert.Contains("followers: expected 10 but the property is absent", message);
    }

    [Fact]
    public void Reports_UnexpectedProperty()
    {
        var message = Compare(new { handle = "ada" }, new { handle = "ada", followers = 10 });

        Assert.NotNull(message);
        Assert.Contains("followers: unexpected property, was 10", message);
    }

    [Fact]
    public void Reports_NestedDifference_WithADottedPath()
    {
        var message = Compare(
            new { outer = new { inner = new { value = 1 } } },
            new { outer = new { inner = new { value = 2 } } });

        Assert.NotNull(message);
        Assert.Contains("outer.inner.value: expected 1 but was 2", message);
    }

    [Fact]
    public void Reports_NullAgainstAValue()
    {
        var message = Compare(new { handle = (string?)null }, new { handle = "ada" });

        Assert.NotNull(message);
        Assert.Contains("handle: expected null but was \"ada\"", message);
    }

    [Fact]
    public void Reports_ValueAgainstNull()
    {
        var message = Compare(new { handle = "ada" }, new { handle = (string?)null });

        Assert.NotNull(message);
        Assert.Contains("handle: expected \"ada\" but was null", message);
    }

    [Fact]
    public void Reports_TypeMismatch()
    {
        var message = Compare(new { followers = 10 }, new { followers = "10" });

        Assert.NotNull(message);
        Assert.Contains("followers: expected 10 but was \"10\"", message);
    }

    [Fact]
    public void Reports_BooleanDifference()
    {
        var message = Compare(new { verified = true }, new { verified = false });

        Assert.NotNull(message);
        Assert.Contains("verified: expected true but was false", message);
    }

    [Fact]
    public void FailureMessage_IncludesBothFullPayloads_ForContext()
    {
        var message = Compare(new { handle = "ada", followers = 10 }, new { handle = "ada", followers = 11 });

        Assert.NotNull(message);
        Assert.Contains("expected:", message);
        Assert.Contains("actual:", message);
        // Indented, so the payload is readable rather than one long line.
        Assert.Contains("\"handle\": \"ada\"", message);
    }

    [Fact]
    public void Canonical_SortsPropertiesAndUsesCamelCase()
    {
        // camelCase so the printed payload matches the casing in the test-case JSON files.
        var canonical = TestCaseJson.Canonical(new { Zebra = 1, Apple = 2 });

        Assert.Contains("\"apple\": 2", canonical);
        Assert.Contains("\"zebra\": 1", canonical);
        Assert.True(
            canonical.IndexOf("apple", StringComparison.Ordinal)
            < canonical.IndexOf("zebra", StringComparison.Ordinal),
            "properties should be sorted");
    }
}
