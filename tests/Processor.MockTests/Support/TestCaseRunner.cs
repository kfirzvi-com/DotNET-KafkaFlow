using Processor.Core.Building;
using Processor.Core.DataTypes;
using Processor.Core.Domains;
using Processor.Core.Messages;
using Processor.Tests.Shared;

namespace Processor.MockTests.Support;

/// <summary>
/// Runs one JSON test case end-to-end through the assembled app and asserts the outcome. Domain-generic
/// so both domains share the same runner — the domain-specific part lives entirely in the data files.
/// </summary>
public static class TestCaseRunner
{
    public static async Task RunAsync<TInput, TDomainData>(
        IDomainModule module, string fileName, string fileContent)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        var testCase = TestCaseJson.Parse<TInput, TDomainData>(fileName, fileContent);

        await using var app = MockProcessorApp<TInput, TDomainData>.Create(module, SettingsFor(testCase));

        // Real startup: the refresh service must load the snapshot before any message is handled.
        await app.StartAsync();

        await app.DeliverAsync(testCase.Input!, testCase.DataTypeId);

        switch (testCase.ExpectedOutcome.ToLowerInvariant())
        {
            case TestOutcomes.Output:
                AssertProducedToOutput(app, testCase, fileName);
                break;

            case TestOutcomes.DeadLetter:
                AssertDeadLettered(app, testCase, fileName);
                break;

            case TestOutcomes.Dropped:
                // Terminal but silent: the offset is committed and nothing is produced.
                Assert.Empty(app.Produced);
                Assert.Empty(app.DeadLettered);
                await AssertBuildOutcomeAsync(app, testCase, BuildStatus.Drop, testCase.ExpectedDropReason);
                break;

            case TestOutcomes.Filtered:
                Assert.Empty(app.Produced);
                Assert.Empty(app.DeadLettered);
                await AssertBuildOutcomeAsync(
                    app, testCase, BuildStatus.Filtered, testCase.ExpectedFilterReason);
                break;
        }
    }

    /// <summary>
    /// The store contents for this case, exactly as its <c>dataTypeSettings</c> declares them. Every
    /// case gets its own <c>InMemoryDataTypeSettingsStore</c> instance, so no case can see or disturb
    /// another's settings.
    /// </summary>
    private static List<DataTypeSetting> SettingsFor<TInput, TDomainData>(
        ProcessorTestCase<TInput, TDomainData> testCase)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        return testCase.DataTypeSettings
            .Select(s => new DataTypeSetting { DataTypeId = s.DataTypeId, IsActive = s.IsActive })
            .ToList();
    }

    private static void AssertProducedToOutput<TInput, TDomainData>(
        MockProcessorApp<TInput, TDomainData> app,
        ProcessorTestCase<TInput, TDomainData> testCase,
        string fileName)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        Assert.Empty(app.DeadLettered);
        var produced = Assert.Single(app.Produced);

        var expected = testCase.ExpectedOutput!;

        if (expected.Id is not null)
        {
            Assert.Equal(expected.Id, produced.Id);
            // Messages are keyed by id so downstream partitioning is stable.
            Assert.Equal(expected.Id, Assert.Single(app.ProducedKeys)?.ToString());
        }

        if (expected.ProcessedContent is not null)
        {
            Assert.Equal(expected.ProcessedContent, produced.ProcessedContent);
        }

        // The domain is stamped from the payload type, never from configuration.
        Assert.Equal(new TDomainData().DomainName, produced.Domain);
        Assert.Equal(Environment.MachineName, produced.ProcessorName);
        Assert.NotEqual(default, produced.ProcessedAt);

        if (expected.DomainData is not null)
        {
            TestCaseJson.AssertMatches(
                expected.DomainData, produced.DomainData,
                $"Processed domain payload for {fileName}");
        }
    }

    private static void AssertDeadLettered<TInput, TDomainData>(
        MockProcessorApp<TInput, TDomainData> app,
        ProcessorTestCase<TInput, TDomainData> testCase,
        string fileName)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        Assert.Empty(app.Produced);
        var deadLettered = Assert.Single(app.DeadLettered);

        Assert.Equal(testCase.ExpectedDeadLetterReason, deadLettered.Reason);
        Assert.Equal(new TDomainData().DomainName, deadLettered.Domain);

        // The original must survive intact for replay, domain payload included.
        Assert.Equal(testCase.Input!.Id, deadLettered.OriginalMessage.Id);
        Assert.Equal(testCase.Input!.Content, deadLettered.OriginalMessage.Content);
        TestCaseJson.AssertMatches(
            testCase.Input!.DomainData, deadLettered.OriginalMessage.DomainData,
            $"Dead-lettered original payload for {fileName}");
        Assert.NotEqual(default, deadLettered.FailedAt);
    }

    /// <summary>
    /// Drops and filters produce nothing, so their status and reason are asserted against the builder
    /// the app resolved — the same singleton instance the handler just used.
    /// </summary>
    private static async Task AssertBuildOutcomeAsync<TInput, TDomainData>(
        MockProcessorApp<TInput, TDomainData> app,
        ProcessorTestCase<TInput, TDomainData> testCase,
        BuildStatus expectedStatus,
        string? expectedReason)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        var outcome = await app.BuildAsync(testCase.Input!, testCase.DataTypeId);

        Assert.Equal(expectedStatus, outcome.Status);
        Assert.Equal(expectedReason, outcome.Reason);
        Assert.Null(outcome.Message);
    }
}
