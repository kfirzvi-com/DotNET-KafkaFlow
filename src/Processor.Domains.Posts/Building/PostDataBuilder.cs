using System.Text.RegularExpressions;
using Processor.Core.Building;

namespace Processor.Domains.Posts.Building;

/// <summary>
/// The posts domain's processing: validates the post payload and derives its computed fields. This is
/// the only builder the posts domain has to supply — Core orchestrates it alongside the shared field
/// builders and honours its dead-letter / drop decisions.
/// </summary>
public class PostDataBuilder : IDomainDataBuilder<PostInputMessage, PostData>
{
    /// <summary>Shares are weighted higher than likes: a reshare is a stronger engagement signal.</summary>
    private const int ShareWeight = 3;

    private static readonly Regex HashtagPattern =
        new(@"#(\w{1,64})", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public FieldBuildResult<PostData> Build(PostInputMessage input)
    {
        var source = input.DomainData;

        // An unattributed post cannot be joined downstream, so it is unprocessable rather than
        // uninteresting: dead-letter it for inspection.
        if (string.IsNullOrWhiteSpace(source.AuthorHandle))
        {
            return FieldBuildResult<PostData>.DeadLetter("Missing post author handle");
        }

        if (string.IsNullOrWhiteSpace(source.Network))
        {
            return FieldBuildResult<PostData>.DeadLetter("Missing post network");
        }

        // Negative counters mean the producer is broken; a silent clamp would hide that.
        if (source.Likes < 0 || source.Shares < 0)
        {
            return FieldBuildResult<PostData>.DeadLetter("Negative engagement counters");
        }

        var processed = new PostData
        {
            AuthorHandle = NormalizeHandle(source.AuthorHandle),
            Network = source.Network.Trim().ToLowerInvariant(),
            Likes = source.Likes,
            Shares = source.Shares,
            Hashtags = ExtractHashtags(input.Content),
            EngagementScore = source.Likes + (source.Shares * ShareWeight)
        };

        return FieldBuildResult<PostData>.Ok(processed);
    }

    /// <summary>Handles are compared downstream, so they are stored lower-case without a leading '@'.</summary>
    private static string NormalizeHandle(string handle) =>
        handle.Trim().TrimStart('@').ToLowerInvariant();

    /// <summary>
    /// Hashtags are derived from the raw content, de-duplicated case-insensitively and stored
    /// lower-case without the '#'.
    /// </summary>
    private static List<string> ExtractHashtags(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return new List<string>();
        }

        return HashtagPattern.Matches(content)
            .Select(match => match.Groups[1].Value.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .ToList();
    }
}
