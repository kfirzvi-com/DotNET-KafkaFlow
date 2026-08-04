using Processor.Core.Messages;
using Processor.Tests.Shared;

namespace Processor.HostE2ETests.Support;

/// <summary>
/// Runs the same JSON test cases as the mock suite, but through the deployed article: a message is
/// produced to a real Kafka topic, consumed by the real host whose settings come from Oracle, and the
/// result is read back off the real output or dead-letter topic.
/// </summary>
public static class E2ETestCaseRunner
{
    private static readonly TimeSpan ProduceTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Long enough that a message *would* have arrived if the processor were going to emit one.</summary>
    private static readonly TimeSpan QuietPeriod = TimeSpan.FromSeconds(8);

    public static async Task RunAsync<TInput, TDomainData>(
        InfrastructureFixture infrastructure,
        string domain,
        string fileName,
        string fileContent)
        where TInput : InputMessage<TDomainData>
        where TDomainData : class, IDomainData, new()
    {
        var testCase = TestCaseJson.Parse<TInput, TDomainData>(fileName, fileContent);

        // A topic set per case keeps cases independent despite sharing one broker.
        var discriminator = SanitizeForTopic(fileName);
        var topics = HostUnderTest.TopicsFor(domain, discriminator);
        await infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        // Oracle holds only what this case declares, so unknown/inactive data types are genuinely
        // unknown/inactive in the database.
        await infrastructure.SeedSettingsAsync(domain, SettingsFor(testCase));

        await using var host = HostUnderTest.Create(infrastructure, domain, topics);
        await host.StartAsync();

        using var kafka = new KafkaClient(infrastructure.BootstrapServers);
        await kafka.ProduceAsync(topics.Input, testCase.Input!, testCase.DataTypeId);

        switch (testCase.ExpectedOutcome.ToLowerInvariant())
        {
            case TestOutcomes.Output:
            {
                var produced = kafka.Consume<OutputMessage<TDomainData>>(topics.Output, 1, ProduceTimeout);
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

                kafka.AssertEmpty(topics.DeadLetter, QuietPeriod);
                break;
            }

            case TestOutcomes.DeadLetter:
            {
                var deadLettered = kafka.Consume<DeadLetterMessage<TDomainData>>(
                    topics.DeadLetter, 1, ProduceTimeout);
                var message = Assert.Single(deadLettered);

                Assert.Equal(testCase.ExpectedDeadLetterReason, message.Reason);
                Assert.Equal(new TDomainData().DomainName, message.Domain);
                Assert.Equal(testCase.Input!.Content, message.OriginalMessage.Content);
                Assert.Equal(
                    TestCaseJson.Canonical(testCase.Input!.DomainData),
                    TestCaseJson.Canonical(message.OriginalMessage.DomainData));

                kafka.AssertEmpty(topics.Output, QuietPeriod);
                break;
            }

            case TestOutcomes.Dropped:
            case TestOutcomes.Filtered:
                // Both outcomes commit the offset and produce nothing anywhere.
                kafka.AssertEmpty(topics.Output, QuietPeriod);
                kafka.AssertEmpty(topics.DeadLetter, TimeSpan.FromSeconds(2));
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
