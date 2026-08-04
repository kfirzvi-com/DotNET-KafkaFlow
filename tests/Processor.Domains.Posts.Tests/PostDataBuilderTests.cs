using Processor.Core.Building;
using Processor.Domains.Posts.Building;

namespace Processor.Domains.Posts.Tests;

/// <summary>Unit tests for the posts domain's only piece of domain-specific processing.</summary>
public class PostDataBuilderTests
{
    private static readonly PostDataBuilder Builder = new();

    private static PostInputMessage Input(
        string content = "a post",
        string authorHandle = "ada",
        string network = "x",
        int likes = 0,
        int shares = 0) => new()
        {
            Id = "post-1",
            Content = content,
            DomainData = new PostData
            {
                AuthorHandle = authorHandle,
                Network = network,
                Likes = likes,
                Shares = shares
            }
        };

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Build_DeadLetters_WhenAuthorHandleMissing(string handle)
    {
        var result = Builder.Build(Input(authorHandle: handle));

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Missing post author handle", result.Reason);
        Assert.Null(result.Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public void Build_DeadLetters_WhenNetworkMissing(string network)
    {
        var result = Builder.Build(Input(network: network));

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Missing post network", result.Reason);
    }

    [Theory]
    [InlineData(-1, 0)]
    [InlineData(0, -1)]
    [InlineData(-5, -5)]
    public void Build_DeadLetters_OnNegativeCounters(int likes, int shares)
    {
        var result = Builder.Build(Input(likes: likes, shares: shares));

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Negative engagement counters", result.Reason);
    }

    [Fact]
    public void Build_ReturnsOk_ForAValidPost()
    {
        var result = Builder.Build(Input(likes: 4, shares: 1));

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.NotNull(result.Value);
        Assert.Null(result.Reason);
    }

    [Theory]
    [InlineData("@Ada", "ada")]
    [InlineData("  @ADA  ", "ada")]
    [InlineData("Ada", "ada")]
    public void Build_NormalizesAuthorHandle(string input, string expected)
    {
        var result = Builder.Build(Input(authorHandle: input));

        Assert.Equal(expected, result.Value!.AuthorHandle);
    }

    [Theory]
    [InlineData("X", "x")]
    [InlineData("  Mastodon ", "mastodon")]
    public void Build_NormalizesNetwork(string input, string expected)
    {
        var result = Builder.Build(Input(network: input));

        Assert.Equal(expected, result.Value!.Network);
    }

    [Theory]
    [InlineData(0, 0, 0)]
    [InlineData(10, 0, 10)]
    [InlineData(0, 1, 3)]
    [InlineData(10, 2, 16)]
    [InlineData(5, 5, 20)]
    public void Build_WeightsSharesTripleInTheEngagementScore(int likes, int shares, int expected)
    {
        var result = Builder.Build(Input(likes: likes, shares: shares));

        Assert.Equal(expected, result.Value!.EngagementScore);
    }

    [Fact]
    public void Build_IgnoresAnyEngagementScoreSuppliedByTheProducer()
    {
        var input = Input(likes: 1, shares: 0);
        input.DomainData.EngagementScore = 9999;

        var result = Builder.Build(input);

        // Derived fields are recomputed, never trusted from the wire.
        Assert.Equal(1, result.Value!.EngagementScore);
    }

    [Fact]
    public void Build_ExtractsHashtagsFromContent_Lowercased()
    {
        var result = Builder.Build(Input(content: "shipping #KafkaFlow with #DotNet"));

        Assert.Equal(new[] { "kafkaflow", "dotnet" }, result.Value!.Hashtags);
    }

    [Fact]
    public void Build_DeduplicatesHashtags_CaseInsensitively()
    {
        var result = Builder.Build(Input(content: "#kafka #KAFKA #Kafka"));

        Assert.Equal(new[] { "kafka" }, result.Value!.Hashtags);
    }

    [Fact]
    public void Build_PreservesHashtagOrderOfFirstAppearance()
    {
        var result = Builder.Build(Input(content: "#beta #alpha #beta #gamma"));

        Assert.Equal(new[] { "beta", "alpha", "gamma" }, result.Value!.Hashtags);
    }

    [Fact]
    public void Build_ReturnsNoHashtags_WhenContentHasNone()
    {
        var result = Builder.Build(Input(content: "just a plain post"));

        Assert.Empty(result.Value!.Hashtags);
    }

    [Fact]
    public void Build_IgnoresABareHashWithNoWord()
    {
        var result = Builder.Build(Input(content: "a # b #ok"));

        Assert.Equal(new[] { "ok" }, result.Value!.Hashtags);
    }

    [Fact]
    public void Build_IgnoresHashtagsSuppliedByTheProducer()
    {
        var input = Input(content: "no tags here");
        input.DomainData.Hashtags = new List<string> { "injected" };

        var result = Builder.Build(input);

        Assert.Empty(result.Value!.Hashtags);
    }

    [Fact]
    public void Build_DoesNotMutateTheIncomingPayload()
    {
        var input = Input(authorHandle: "@Ada", network: "X", likes: 2, shares: 1);

        Builder.Build(input);

        // The handler dead-letters the *original* message, so the builder must leave it untouched.
        Assert.Equal("@Ada", input.DomainData.AuthorHandle);
        Assert.Equal("X", input.DomainData.Network);
        Assert.Equal(0, input.DomainData.EngagementScore);
    }

    [Fact]
    public void Build_PreservesRawCounters()
    {
        var result = Builder.Build(Input(likes: 7, shares: 3));

        Assert.Equal(7, result.Value!.Likes);
        Assert.Equal(3, result.Value!.Shares);
    }

    [Fact]
    public void PostData_ReportsItsDomainName()
    {
        Assert.Equal("posts", new PostData().DomainName);
        Assert.Equal(PostData.Domain, new PostData().DomainName);
    }
}
