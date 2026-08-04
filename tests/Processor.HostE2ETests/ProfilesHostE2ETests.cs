using Processor.Domains.Profiles;
using Processor.HostE2ETests.Support;
using Processor.Tests.Shared;

namespace Processor.HostE2ETests;

/// <summary>Every profiles test case, through the real host with real Kafka and Oracle.</summary>
[Collection(InfrastructureCollection.Name)]
public class ProfilesHostE2ETests
{
    private readonly InfrastructureFixture _infrastructure;

    public ProfilesHostE2ETests(InfrastructureFixture infrastructure) => _infrastructure = infrastructure;

    [Theory]
    [MemberData(nameof(TestCaseLoader.Profiles), MemberType = typeof(TestCaseLoader))]
    public Task RunsTestCase(string fileName, string fileContent) =>
        E2ETestCaseRunner.RunAsync<ProfileInputMessage, ProfileData>(
            _infrastructure, ProfileData.Domain, fileName, fileContent);
}
