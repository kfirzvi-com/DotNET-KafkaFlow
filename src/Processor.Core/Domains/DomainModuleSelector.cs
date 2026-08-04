using Processor.Core.Application;

namespace Processor.Core.Domains;

/// <summary>
/// Resolves the configured domain name to its module. Kept separate from the host wiring so the
/// selection rules — case-insensitive match, unknown name is fatal — are unit-testable on their own.
/// </summary>
public static class DomainModuleSelector
{
    /// <summary>
    /// Returns the module whose <see cref="IDomainModule.Name"/> matches <paramref name="domain"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The name is blank, unknown, or ambiguous. Failing here is deliberate: a typo must stop startup
    /// rather than silently run the wrong domain against this deployment's topics.
    /// </exception>
    public static IDomainModule Select(IEnumerable<IDomainModule> modules, string? domain)
    {
        var available = modules.ToList();

        if (available.Count == 0)
        {
            throw new InvalidOperationException(
                "No domain modules are registered; the host cannot run any domain.");
        }

        var known = string.Join(", ", available.Select(m => m.Name).OrderBy(n => n));

        if (string.IsNullOrWhiteSpace(domain))
        {
            throw new InvalidOperationException(
                $"'{ProcessorOptions.SectionName}:{nameof(ProcessorOptions.Domain)}' is not configured. " +
                $"Set it to one of: {known}.");
        }

        var matches = available
            .Where(m => string.Equals(m.Name, domain.Trim(), StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (matches.Count == 0)
        {
            throw new InvalidOperationException(
                $"Unknown domain '{domain}'. Known domains: {known}.");
        }

        if (matches.Count > 1)
        {
            throw new InvalidOperationException(
                $"Domain '{domain}' is registered {matches.Count} times; domain names must be unique.");
        }

        return matches[0];
    }
}
