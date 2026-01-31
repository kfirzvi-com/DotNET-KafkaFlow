using System.Reflection;
using Xunit.Sdk;
using System.Text.Json;

namespace Processor.Tests.Helpers;

/// <summary>
/// Load test data from JSON files.
/// Supports loading from a single file or a specific property within a file.
/// </summary>
public class JsonFileDataAttribute : DataAttribute
{
    private readonly string _filePath;
    private readonly string? _propertyName;

    /// <summary>
    /// Load data from a JSON file as the data source for a theory
    /// </summary>
    /// <param name="filePath">The relative path to the JSON file (relative to TestsData folder)</param>
    public JsonFileDataAttribute(string filePath)
        : this(filePath, null) { }

    /// <summary>
    /// Load data from a JSON file as the data source for a theory
    /// </summary>
    /// <param name="filePath">The relative path to the JSON file (relative to TestsData folder)</param>
    /// <param name="propertyName">The name of the property in the JSON file that contains the test data</param>
    public JsonFileDataAttribute(string filePath, string? propertyName)
    {
        _filePath = filePath;
        _propertyName = propertyName;
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        if (testMethod == null)
            throw new ArgumentNullException(nameof(testMethod));

        var testDataDir = ResolveTestDataDirectory();
        var fullPath = Path.Combine(testDataDir, _filePath);

        if (!File.Exists(fullPath))
            throw new ArgumentException($"Could not find file at path: {fullPath}");

        var fileContent = File.ReadAllText(fullPath);
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        if (string.IsNullOrEmpty(_propertyName))
        {
            // Entire file is the test data
            var testData = JsonSerializer.Deserialize<List<object[]>>(fileContent, options);
            return testData ?? new List<object[]>();
        }

        // Load specific property from JSON
        using var doc = System.Text.Json.JsonDocument.Parse(fileContent);
        if (!doc.RootElement.TryGetProperty(_propertyName, out var property))
            throw new ArgumentException($"Property '{_propertyName}' not found in JSON file: {fullPath}");

        var data = JsonSerializer.Deserialize<List<object[]>>(property.GetRawText(), options);
        return data ?? new List<object[]>();
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
                return candidate;
        }

        return candidates[0];
    }
}
