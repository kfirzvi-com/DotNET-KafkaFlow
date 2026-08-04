namespace Processor.Tests.Shared;

/// <summary>
/// Discovers the per-domain JSON test cases copied next to the test assembly. Yields only primitives
/// (file name + raw content) because xUnit enumerates a theory into individually runnable cases only
/// when the data is primitive — complex objects collapse into one opaque test.
/// </summary>
public static class TestCaseLoader
{
    /// <summary>Test cases for the posts domain.</summary>
    public static IEnumerable<object[]> Posts => Cases("Posts");

    /// <summary>Test cases for the profiles domain.</summary>
    public static IEnumerable<object[]> Profiles => Cases("Profiles");

    /// <summary>Yields <c>{ fileName, fileContent }</c> for every test case file of a domain.</summary>
    public static IEnumerable<object[]> Cases(string domainDirectory)
    {
        var directory = Path.Combine(ResolveTestDataRoot(), domainDirectory);

        if (!Directory.Exists(directory))
        {
            throw new InvalidOperationException(
                $"Test data directory not found: {directory}. Expected tests/TestData/{domainDirectory} " +
                "to be linked into the test output (see tests/TestData.targets).");
        }

        var files = Directory.GetFiles(directory, "test_case_*.json").OrderBy(f => f, StringComparer.Ordinal);

        var any = false;
        foreach (var file in files)
        {
            any = true;
            yield return new object[] { Path.GetFileName(file), File.ReadAllText(file) };
        }

        if (!any)
        {
            throw new InvalidOperationException($"No test_case_*.json files found in {directory}.");
        }
    }

    private static string ResolveTestDataRoot()
    {
        var candidate = Path.Combine(AppContext.BaseDirectory, "TestData");
        if (Directory.Exists(candidate))
        {
            return candidate;
        }

        // Fall back to the repository copy, so an IDE that runs tests from a different working
        // directory still finds the cases.
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            var repoCopy = Path.Combine(directory.FullName, "tests", "TestData");
            if (Directory.Exists(repoCopy))
            {
                return repoCopy;
            }

            directory = directory.Parent;
        }

        return candidate;
    }
}
