using Processor.Domains.Profiles;
using Processor.MockTests.Support;
using Processor.Tests.Shared;

namespace Processor.MockTests;

/// <summary>
/// Every profiles test case, driven through the same runner as the posts cases. That both domains reuse
/// one runner unchanged is itself the point: the shared pipeline is genuinely domain-agnostic.
/// </summary>
public class ProfilesFlowTests
{
    [Theory]
    [MemberData(nameof(TestCaseLoader.Profiles), MemberType = typeof(TestCaseLoader))]
    public Task RunsTestCase(string fileName, string fileContent) =>
        TestCaseRunner.RunAsync<ProfileInputMessage, ProfileData>(
            new ProfilesDomainModule(), fileName, fileContent);
}
