using Processor.Core.Building;
using Processor.Domains.Profiles.Building;

namespace Processor.Domains.Profiles.Tests;

/// <summary>Unit tests for the profiles domain's only piece of domain-specific processing.</summary>
public class ProfileDataBuilderTests
{
    private static readonly ProfileDataBuilder Builder = new();

    private static ProfileInputMessage Input(
        string content = "a profile",
        string handle = "ada",
        string network = "x",
        string displayName = "Ada Lovelace",
        int followers = 100,
        int following = 10,
        bool verified = false) => new()
        {
            Id = "profile-1",
            Content = content,
            DomainData = new ProfileData
            {
                Handle = handle,
                Network = network,
                DisplayName = displayName,
                Followers = followers,
                Following = following,
                Verified = verified
            }
        };

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Build_DeadLetters_WhenHandleMissing(string handle)
    {
        var result = Builder.Build(Input(handle: handle));

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Missing profile handle", result.Reason);
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public void Build_DeadLetters_WhenNetworkMissing(string network)
    {
        var result = Builder.Build(Input(network: network));

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Missing profile network", result.Reason);
    }

    [Theory]
    [InlineData(-1, 10)]
    [InlineData(10, -1)]
    public void Build_DeadLetters_OnNegativeCounters(int followers, int following)
    {
        var result = Builder.Build(Input(followers: followers, following: following));

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Negative follower counters", result.Reason);
    }

    [Fact]
    public void Build_Drops_WhenProfileHasNoFollowers()
    {
        var result = Builder.Build(Input(followers: 0, following: 12));

        // A valid but analytically empty profile is dropped, not dead-lettered: nothing is malformed.
        Assert.Equal(BuildStatus.Drop, result.Status);
        Assert.Equal("Profile has no followers", result.Reason);
        Assert.Null(result.Value);
    }

    [Fact]
    public void Build_ChecksNegativeCountersBeforeTheNoFollowersDrop()
    {
        // followers = 0 and following = -1: the malformed payload must win, so the operator sees it.
        var result = Builder.Build(Input(followers: 0, following: -1));

        Assert.Equal(BuildStatus.DeadLetter, result.Status);
        Assert.Equal("Negative follower counters", result.Reason);
    }

    [Fact]
    public void Build_ReturnsOk_ForAValidProfile()
    {
        var result = Builder.Build(Input(followers: 500, following: 25));

        Assert.Equal(BuildStatus.Ok, result.Status);
        Assert.NotNull(result.Value);
        Assert.Null(result.Reason);
    }

    [Theory]
    [InlineData("@Ada", "ada")]
    [InlineData("  @ADA ", "ada")]
    [InlineData("Ada", "ada")]
    public void Build_NormalizesHandle(string input, string expected)
    {
        var result = Builder.Build(Input(handle: input));

        Assert.Equal(expected, result.Value!.Handle);
    }

    [Theory]
    [InlineData("X", "x")]
    [InlineData(" Bluesky ", "bluesky")]
    public void Build_NormalizesNetwork(string input, string expected)
    {
        var result = Builder.Build(Input(network: input));

        Assert.Equal(expected, result.Value!.Network);
    }

    [Fact]
    public void Build_TrimsDisplayName()
    {
        var result = Builder.Build(Input(displayName: "  Ada Lovelace  "));

        Assert.Equal("Ada Lovelace", result.Value!.DisplayName);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Build_FallsBackToHandle_WhenDisplayNameBlank(string displayName)
    {
        var result = Builder.Build(Input(handle: "@Linus", displayName: displayName));

        // The fallback keeps the handle's original casing; only the handle field is lower-cased.
        Assert.Equal("Linus", result.Value!.DisplayName);
        Assert.Equal("linus", result.Value!.Handle);
    }

    [Theory]
    [InlineData(100, 10, 10.0)]
    [InlineData(5000, 250, 20.0)]
    [InlineData(1, 3, 1.0 / 3.0)]
    public void Build_ComputesFollowerRatio(int followers, int following, double expected)
    {
        var result = Builder.Build(Input(followers: followers, following: following));

        Assert.Equal(expected, result.Value!.FollowerRatio, precision: 10);
    }

    [Fact]
    public void Build_ReturnsZeroRatio_WhenFollowingNobody()
    {
        var result = Builder.Build(Input(followers: 100, following: 0));

        // Guards the division: downstream never has to handle infinity or NaN.
        Assert.Equal(0d, result.Value!.FollowerRatio);
    }

    [Theory]
    [InlineData(1, "micro")]
    [InlineData(9_999, "micro")]
    [InlineData(10_000, "mid")]
    [InlineData(99_999, "mid")]
    [InlineData(100_000, "macro")]
    [InlineData(999_999, "macro")]
    [InlineData(1_000_000, "mega")]
    [InlineData(50_000_000, "mega")]
    public void Build_AssignsAudienceTierAtTheBoundaries(int followers, string expectedTier)
    {
        var result = Builder.Build(Input(followers: followers, following: 1));

        Assert.Equal(expectedTier, result.Value!.AudienceTier);
    }

    [Fact]
    public void Build_PreservesVerifiedFlag()
    {
        Assert.True(Builder.Build(Input(verified: true)).Value!.Verified);
        Assert.False(Builder.Build(Input(verified: false)).Value!.Verified);
    }

    [Fact]
    public void Build_IgnoresDerivedFieldsSuppliedByTheProducer()
    {
        var input = Input(followers: 100, following: 10);
        input.DomainData.FollowerRatio = 999d;
        input.DomainData.AudienceTier = "injected";

        var result = Builder.Build(input);

        Assert.Equal(10d, result.Value!.FollowerRatio);
        Assert.Equal("micro", result.Value!.AudienceTier);
    }

    [Fact]
    public void Build_DoesNotMutateTheIncomingPayload()
    {
        var input = Input(handle: "@Ada", network: "X", followers: 100, following: 10);

        Builder.Build(input);

        Assert.Equal("@Ada", input.DomainData.Handle);
        Assert.Equal("X", input.DomainData.Network);
        Assert.Equal(0d, input.DomainData.FollowerRatio);
        Assert.Equal(string.Empty, input.DomainData.AudienceTier);
    }

    [Fact]
    public void ProfileData_ReportsItsDomainName()
    {
        Assert.Equal("profiles", new ProfileData().DomainName);
        Assert.Equal(ProfileData.Domain, new ProfileData().DomainName);
    }
}
