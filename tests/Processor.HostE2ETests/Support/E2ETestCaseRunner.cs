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

        // A topic set per case keeps cases independent despite sharing one broker.
        var discriminator = SanitizeForTopic(fileName);
        var topics = HostUnderTest.TopicsFor(domain, discriminator);
        await infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        // Oracle holds only what this case declares plus the watermark's own row, so unknown/inactive
        // data types are genuinely unknown/inactive in the database.
        await Watermark<TInput, TDomainData>.SeedAsync(infrastructure, domain, SettingsFor(testCase));

        await using var host = HostUnderTest.Create(infrastructure, domain, topics);
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
                    Assert.Equal(
                        TestCaseJson.Canonical(expected.DomainData),
                        TestCaseJson.Canonical(message.DomainData));
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
                Assert.Equal(
                    TestCaseJson.Canonical(testCase.Input!.DomainData),
                    TestCaseJson.Canonical(message.OriginalMessage.DomainData));

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

    private static (string, bool)[] SettingsFor<TInput, TDomainData>(
        ProcessorTestCase<TInput, TDomainData> testCase)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        if (testCase.DataTypeId is null || !testCase.DataTypeRegistered)
        {
            return Array.Empty<(string, bool)>();
        }

        return new[] { (testCase.DataTypeId, testCase.DataTypeActive) };
    }

    /// <summary>Turns a test-case file name into a legal, readable topic suffix.</summary>
    private static string SanitizeForTopic(string fileName)
    {
        var stem = Path.GetFileNameWithoutExtension(fileName).Replace("test_case_", string.Empty);
        var chars = stem.Select(c => char.IsLetterOrDigit(c) ? c : '-').ToArray();
        return new string(chars);
    }
}
