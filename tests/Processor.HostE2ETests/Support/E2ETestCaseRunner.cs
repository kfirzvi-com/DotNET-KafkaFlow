using Processor.Core.Messages;
using Processor.Tests.Shared;

namespace Processor.HostE2ETests.Support;

/// <summary>
/// Runs the same JSON test cases as the mock suite, but through the deployed article: a message is
/// produced to a real Kafka topic, consumed by the real host whose settings come from Oracle, and the
/// result is read back off the real output or dead-letter topic.
/// <para>
/// Assertions are watermarked rather than timed — see <see cref="Watermark{TInput,TDomainData}"/>.
/// There is no fixed wait anywhere in this runner.
/// </para>
/// </summary>
public static class E2ETestCaseRunner
{
    public static async Task RunAsync<TInput, TDomainData>(
        InfrastructureFixture infrastructure,
        string domain,
        string fileName,
        string fileContent,
        Func<string, string, TInput> messageFactory)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        var testCase = TestCaseJson.Parse<TInput, TDomainData>(fileName, fileContent);

        // Its own topics and its own Oracle table, so no case can observe or disturb another's.
        var discriminator = SanitizeForTopic(fileName);
        var topics = HostUnderTest.TopicsFor(domain, discriminator);
        await infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        // The table starts empty and receives exactly what the case declares (plus the watermark's own
        // row), so an unlisted data type is genuinely absent from the database rather than left over.
        var settingsTable = await infrastructure.CreateSettingsTableAsync(discriminator);
        await Watermark<TInput, TDomainData>.SeedAsync(settingsTable, domain, SettingsFor(testCase));

        await using var host = HostUnderTest.Create(infrastructure, domain, topics, settingsTable);
        await host.StartAsync();

        using var kafka = new KafkaClient(infrastructure.BootstrapServers);
        var watermark = new Watermark<TInput, TDomainData>(messageFactory);

        await kafka.ProduceAsync(topics.Input, testCase.Input!, testCase.DataTypeId);
        // Produced after the message under test, so both markers sit behind it in the partition.
        await watermark.ProduceAsync(kafka, topics.Input);

        var produced = watermark.ReadOutput(kafka, topics.Output);
        var deadLettered = watermark.ReadDeadLetters(kafka, topics.DeadLetter);

        switch (testCase.ExpectedOutcome.ToLowerInvariant())
        {
            case TestOutcomes.Output:
            {
                Assert.Empty(deadLettered);
                var message = Assert.Single(produced);

                var expected = testCase.ExpectedOutput!;
                if (expected.Id is not null)
                {
                    Assert.Equal(expected.Id, message.Id);
                }

                if (expected.ProcessedContent is not null)
                {
                    Assert.Equal(expected.ProcessedContent, message.ProcessedContent);
                }

                Assert.Equal(new TDomainData().DomainName, message.Domain);

                if (expected.DomainData is not null)
                {
                    TestCaseJson.AssertMatches(
                        expected.DomainData, message.DomainData,
                        $"Processed domain payload for {fileName}");
                }

                break;
            }

            case TestOutcomes.DeadLetter:
            {
                Assert.Empty(produced);
                var message = Assert.Single(deadLettered);

                Assert.Equal(testCase.ExpectedDeadLetterReason, message.Reason);
                Assert.Equal(new TDomainData().DomainName, message.Domain);
                Assert.Equal(testCase.Input!.Content, message.OriginalMessage.Content);
                TestCaseJson.AssertMatches(
                    testCase.Input!.DomainData, message.OriginalMessage.DomainData,
                    $"Dead-lettered original payload for {fileName}");

                break;
            }

            case TestOutcomes.Dropped:
            case TestOutcomes.Filtered:
                // Both outcomes commit the offset and produce nothing anywhere. Because the watermark
                // has already been observed on both topics, these are exact assertions.
                Assert.Empty(produced);
                Assert.Empty(deadLettered);
                break;
        }
    }

    /// <summary>The rows to insert for this case, exactly as its <c>dataTypeSettings</c> declares them.</summary>
    private static (string DataTypeId, bool IsActive)[] SettingsFor<TInput, TDomainData>(
        ProcessorTestCase<TInput, TDomainData> testCase)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new() =>
        testCase.DataTypeSettings.Select(s => (s.DataTypeId, s.IsActive)).ToArray();

    /// <summary>Turns a test-case file name into a legal, readable topic suffix.</summary>
    private static string SanitizeForTopic(string fileName)
    {
        var stem = Path.GetFileNameWithoutExtension(fileName).Replace("test_case_", string.Empty);
        var chars = stem.Select(c => char.IsLetterOrDigit(c) ? c : '-').ToArray();
        return new string(chars);
    }
}
