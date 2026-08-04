namespace Processor.Core.Application;

/// <summary>
/// Lifecycle of the data-type settings snapshot loaded from the data store (Oracle in production).
/// </summary>
public class DataTypeSettingsOptions
{
    public const string SectionName = "DataTypeSettings";

    /// <summary>
    /// Which backing store to load from: <c>oracle</c> (default, production) or <c>inmemory</c> for
    /// local runs and tests. Only the store swaps — caching, refresh and fail-fast startup behave
    /// identically either way, so a local run exercises the same code path as production.
    /// </summary>
    public string Store { get; set; } = "oracle";

    /// <summary>
    /// How often the snapshot is reloaded from the store. A failed refresh logs a warning and keeps
    /// the current snapshot — it never clears settings and never crashes the app.
    /// </summary>
    public int RefreshMinutes { get; set; } = 10;

    /// <summary>
    /// How many times the initial load is attempted at startup before the host refuses to start.
    /// Exhausting these attempts is fatal by design: the processor must never consume without
    /// settings, so it exits non-zero and lets the orchestrator restart it.
    /// </summary>
    public int StartupAttempts { get; set; } = 3;

    /// <summary>Delay between startup load attempts.</summary>
    public int StartupRetryDelaySeconds { get; set; } = 5;

    /// <summary>Kafka header carrying the data type id; falls back to the message key when absent.</summary>
    public string HeaderName { get; set; } = "data-type-id";
}
