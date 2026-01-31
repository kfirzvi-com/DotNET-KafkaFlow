using Processor.Messages;

namespace Processor.Tests.Helpers;

public static class TestDataLoader
{
    public static IEnumerable<object[]> TestCases
    {
        get
        {
            var testDataDirectory = ResolveTestDataDirectory();
            var testFiles = Directory.GetFiles(testDataDirectory, "test_case_*.json")
                .OrderBy(f => f)
                .ToList();

            foreach (var file in testFiles)
            {
                var fileName = Path.GetFileName(file) ?? string.Empty;
                var fileContent = File.ReadAllText(file);
                yield return new object[] { fileName, fileContent };
            }
        }
    }

    private static string ResolveTestDataDirectory()
    {
        var baseDir = AppContext.BaseDirectory;
        var candidates = new List<string>
        {
            Path.Combine(baseDir, "TestsData"),
            Path.Combine(Directory.GetCurrentDirectory(), "TestsData")
        };

        var dir = new DirectoryInfo(baseDir);
        while (dir != null)
        {
            var projectFile = Path.Combine(dir.FullName, "Processor.Tests.csproj");
            if (File.Exists(projectFile))
            {
                candidates.Add(Path.Combine(dir.FullName, "TestsData"));
                break;
            }

            dir = dir.Parent;
        }

        foreach (var candidate in candidates.Distinct())
        {
            if (Directory.Exists(candidate))
            {
                return candidate;
            }
        }

        return candidates[0];
    }
}

public class TestData
{
    public InputMessage? Input { get; set; }
    public string ExpectedOutcome { get; set; } = "output";
    public ExpectedOutputData? ExpectedOutput { get; set; }
}

public class ExpectedOutputData
{
    public string? Id { get; set; }
    public string? ProcessedContent { get; set; }
}
