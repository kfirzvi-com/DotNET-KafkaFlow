using Processor.Core.Messages;

namespace Processor.Domains.Posts;

/// <summary>
/// Domain payload for social-network <em>posts</em>: what a post carries beyond the shared id,
/// content and timestamp. This is the type that closes Core's generics for this deployment.
/// </summary>
public class PostData : IDomainData
{
    /// <summary>Stable domain name; matches <c>Processor:Domain: "posts"</c>.</summary>
    public const string Domain = "posts";

    public string DomainName => Domain;

    /// <summary>Handle of the account that published the post.</summary>
    public string AuthorHandle { get; set; } = string.Empty;

    /// <summary>Network the post came from (e.g. <c>x</c>, <c>mastodon</c>, <c>bluesky</c>).</summary>
    public string Network { get; set; } = string.Empty;

    public int Likes { get; set; }

    public int Shares { get; set; }

    /// <summary>Hashtags extracted from the post content, normalized by the domain builder.</summary>
    public List<string> Hashtags { get; set; } = new();

    /// <summary>
    /// Engagement score derived from likes and shares — a shares-weighted total, computed by the
    /// domain builder rather than trusted from the producer.
    /// </summary>
    public int EngagementScore { get; set; }
}
