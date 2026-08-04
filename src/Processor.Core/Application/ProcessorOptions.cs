namespace Processor.Core.Application;

/// <summary>
/// Selects which domain this deployment runs. The same image is deployed once per domain; only this
/// value differs between deployments, and it decides every domain-specific type DI resolves.
/// </summary>
public class ProcessorOptions
{
    public const string SectionName = "Processor";

    /// <summary>
    /// Domain name, matched case-insensitively against the registered domain modules
    /// (e.g. <c>posts</c>, <c>profiles</c>). Startup fails with the list of known domains if it does
    /// not match — a typo must never silently start the wrong processor.
    /// </summary>
    public string Domain { get; set; } = string.Empty;
}
