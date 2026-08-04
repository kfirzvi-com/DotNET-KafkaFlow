using Processor.Core.Messages;

namespace Processor.Domains.Profiles;

/// <summary>
/// Domain payload for social-network <em>profiles</em>: the account behind the posts. Same
/// social-network business area as <c>PostData</c>, different entity — which is exactly the case the
/// generic base is for.
/// </summary>
public class ProfileData : IDomainData
{
    /// <summary>Stable domain name; matches <c>Processor:Domain: "profiles"</c>.</summary>
    public const string Domain = "profiles";

    public string DomainName => Domain;

    /// <summary>Account handle this profile belongs to.</summary>
    public string Handle { get; set; } = string.Empty;

    /// <summary>Network the profile lives on (e.g. <c>x</c>, <c>mastodon</c>, <c>bluesky</c>).</summary>
    public string Network { get; set; } = string.Empty;

    public string DisplayName { get; set; } = string.Empty;

    public int Followers { get; set; }

    public int Following { get; set; }

    public bool Verified { get; set; }

    /// <summary>
    /// Followers divided by following — computed by the domain builder, not trusted from the producer.
    /// Zero when the account follows nobody, so downstream never has to handle a division by zero.
    /// </summary>
    public double FollowerRatio { get; set; }

    /// <summary>
    /// Coarse audience bucket derived from <see cref="Followers"/>. A bounded set of values, so it is
    /// safe to group by downstream.
    /// </summary>
    public string AudienceTier { get; set; } = string.Empty;
}
