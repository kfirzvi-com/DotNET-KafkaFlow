using Processor.Domains.Posts;
using Processor.HostE2ETests.Support;
using Processor.Tests.Shared;

namespace Processor.HostE2ETests;

/// <summary>Every posts test case, through the real host with real Kafka and Oracle.</summary>
[Collection(InfrastructureCollection.Name)]
public class PostsHostE2ETests
{
    private readonly InfrastructureFixture _infrastructure;

    public PostsHostE2ETests(InfrastructureFixture infrastructure) => _infrastructure = infrastructure;

    [Theory]
    [MemberData(nameof(TestCaseLoader.Posts), MemberType = typeof(TestCaseLoader))]
    public Task RunsTestCase(string fileName, string fileContent) =>
        E2ETestCaseRunner.RunAsync<PostInputMessage, PostData>(
            _infrastructure, PostData.Domain, fileName, fileContent);
}
