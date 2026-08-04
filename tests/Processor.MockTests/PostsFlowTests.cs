using Processor.Domains.Posts;
using Processor.MockTests.Support;
using Processor.Tests.Shared;

namespace Processor.MockTests;

/// <summary>
/// Every posts test case, driven end-to-end through the assembled app. One xUnit case per JSON file, so
/// a failure names the file that broke.
/// </summary>
public class PostsFlowTests
{
    [Theory]
    [MemberData(nameof(TestCaseLoader.Posts), MemberType = typeof(TestCaseLoader))]
    public Task RunsTestCase(string fileName, string fileContent) =>
        TestCaseRunner.RunAsync<PostInputMessage, PostData>(
            new PostsDomainModule(), fileName, fileContent);
}
