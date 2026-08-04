using Processor.Core.Building;

namespace Processor.Domains.Profiles.Building;

/// <summary>
/// The profiles domain's processing: validates the profile payload and derives its computed fields.
/// Note it makes different routing choices from the posts builder (it <em>drops</em> a profile with no
/// audience instead of dead-lettering it) — the same Core orchestration honours both.
/// </summary>
public class ProfileDataBuilder : IDomainDataBuilder<ProfileInputMessage, ProfileData>
{
    private const int MicroTierMax = 10_000;
    private const int MidTierMax = 100_000;
    private const int MacroTierMax = 1_000_000;

    public FieldBuildResult<ProfileData> Build(ProfileInputMessage input)
    {
        var source = input.DomainData;

        // Without a handle the profile cannot be joined to its posts: unprocessable, so dead-letter.
        if (string.IsNullOrWhiteSpace(source.Handle))
        {
            return FieldBuildResult<ProfileData>.DeadLetter("Missing profile handle");
        }

        if (string.IsNullOrWhiteSpace(source.Network))
        {
            return FieldBuildResult<ProfileData>.DeadLetter("Missing profile network");
        }

        if (source.Followers < 0 || source.Following < 0)
        {
            return FieldBuildResult<ProfileData>.DeadLetter("Negative follower counters");
        }

        // A profile with no audience is valid but carries no analytical value downstream, so it is
        // dropped rather than dead-lettered: nothing is wrong with the message.
        if (source.Followers == 0)
        {
            return FieldBuildResult<ProfileData>.Drop("Profile has no followers");
        }

        var processed = new ProfileData
        {
            Handle = source.Handle.Trim().TrimStart('@').ToLowerInvariant(),
            Network = source.Network.Trim().ToLowerInvariant(),
            DisplayName = string.IsNullOrWhiteSpace(source.DisplayName)
                ? source.Handle.Trim().TrimStart('@')
                : source.DisplayName.Trim(),
            Followers = source.Followers,
            Following = source.Following,
            Verified = source.Verified,
            FollowerRatio = source.Following == 0 ? 0d : (double)source.Followers / source.Following,
            AudienceTier = TierFor(source.Followers)
        };

        return FieldBuildResult<ProfileData>.Ok(processed);
    }

    private static string TierFor(int followers) => followers switch
    {
        < MicroTierMax => "micro",
        < MidTierMax => "mid",
        < MacroTierMax => "macro",
        _ => "mega"
    };
}
