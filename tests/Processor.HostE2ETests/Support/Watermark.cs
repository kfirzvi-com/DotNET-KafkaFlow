using Processor.Core.DataTypes;
using Processor.Core.Messages;

namespace Processor.HostE2ETests.Support;

/// <summary>
/// A pair of marker messages produced <em>after</em> the message under test, one destined for the
/// output topic and one for the dead-letter topic. Once both markers have been observed, the
/// processor has provably finished with everything ahead of them, so "nothing was produced" is an
/// exact assertion rather than a timed guess.
/// <para>
/// This replaces the fixed quiet periods the suite used to wait out — the reason a single test case
/// dropped from ~8–10s to well under a second.
/// </para>
/// </summary>
public sealed class Watermark<TInput, TDomainData>
    where TInput : InputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    /// <summary>
    /// Data type the markers travel under. Always seeded active, and distinct from anything a test
    /// case uses, so the markers pass the filter no matter what the case is exercising.
    /// </summary>
    public const string DataTypeId = "__watermark__";

    /// <summary>Ceiling for marker arrival. A failure guard, not an expected wait.</summary>
    private static readonly TimeSpan Ceiling = TimeSpan.FromSeconds(60);

    private readonly Func<string, string, TInput> _factory;
    private readonly string _outputId;
    private readonly string _outputContent;
    private readonly string _deadLetterContent;

    /// <param name="factory">
    /// Builds a message valid for this domain from an id and content. Domain-supplied because only the
    /// domain knows what a passing payload looks like.
    /// </param>
    public Watermark(Func<string, string, TInput> factory)
    {
        _factory = factory;
        var token = Guid.NewGuid().ToString("N");
        _outputId = $"watermark-{token}";
        _outputContent = $"watermark output {token}";
        _deadLetterContent = $"watermark dead letter {token}";
    }

    /// <summary>The watermark's own settings row, to seed alongside the test case's.</summary>
    public static (string DataTypeId, bool IsActive) Setting => (DataTypeId, true);

    /// <summary>
    /// Produces both markers, in the order their outcomes are read. Must be called after the message
    /// under test so the markers sit behind it in the partition.
    /// </summary>
    public async Task ProduceAsync(KafkaClient kafka, string inputTopic)
    {
        // Empty id fails the shared id rule, so this one dead-letters — that rule is domain-agnostic,
        // which is what lets one watermark work for every domain.
        await kafka.ProduceAsync(inputTopic, _factory(string.Empty, _deadLetterContent), DataTypeId);

        // Valid message under an active data type, so this one reaches the output topic.
        await kafka.ProduceAsync(inputTopic, _factory(_outputId, _outputContent), DataTypeId);
    }

    /// <summary>Everything the case produced on the output topic, watermark excluded.</summary>
    public List<OutputMessage<TDomainData>> ReadOutput(KafkaClient kafka, string outputTopic) =>
        kafka.ConsumeUntilMarker<OutputMessage<TDomainData>>(
            outputTopic, m => m.Id == _outputId, Ceiling);

    /// <summary>Everything the case produced on the dead-letter topic, watermark excluded.</summary>
    public List<DeadLetterMessage<TDomainData>> ReadDeadLetters(KafkaClient kafka, string deadLetterTopic) =>
        kafka.ConsumeUntilMarker<DeadLetterMessage<TDomainData>>(
            deadLetterTopic,
            // Matched on content, not id: the marker's id is empty by design, and a test case may
            // legitimately dead-letter an empty id too.
            m => m.OriginalMessage.Content == _deadLetterContent,
            Ceiling);

    /// <summary>The settings rows a watermarked run needs: the case's, plus the watermark's own.</summary>
    public static (string DataTypeId, bool IsActive)[] SettingsWith(
        params (string DataTypeId, bool IsActive)[] caseSettings)
    {
        var conflict = caseSettings.FirstOrDefault(
            s => string.Equals(s.DataTypeId, DataTypeId, StringComparison.OrdinalIgnoreCase));

        if (conflict.DataTypeId is not null)
        {
            throw new InvalidOperationException(
                $"A test case may not declare the reserved watermark data type '{DataTypeId}'.");
        }

        return caseSettings.Append(Setting).ToArray();
    }

    /// <summary>Seeds the case's settings plus the watermark's own into that case's table.</summary>
    public static Task SeedAsync(
        SettingsTable table,
        string domain,
        params (string DataTypeId, bool IsActive)[] caseSettings) =>
        table.SeedAsync(domain, SettingsWith(caseSettings));
}

/// <summary>Domain-specific message factories the watermark needs.</summary>
public static class WatermarkFactories
{
    /// <summary>A posts message that passes every posts rule (given a non-empty id).</summary>
    public static Domains.Posts.PostInputMessage Post(string id, string content) => new()
    {
        Id = id,
        Content = content,
        DomainData = new Domains.Posts.PostData
        {
            AuthorHandle = "watermark",
            Network = "x",
            Likes = 0,
            Shares = 0
        }
    };

    /// <summary>A profiles message that passes every profiles rule (given a non-empty id).</summary>
    public static Domains.Profiles.ProfileInputMessage Profile(string id, string content) => new()
    {
        Id = id,
        Content = content,
        DomainData = new Domains.Profiles.ProfileData
        {
            Handle = "watermark",
            Network = "x",
            DisplayName = "Watermark",
            // Must be non-zero: a follower-less profile is deliberately dropped by the domain rules,
            // and a dropped watermark would never reach the output topic.
            Followers = 1,
            Following = 1
        }
    };
}
