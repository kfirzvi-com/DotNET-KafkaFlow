namespace Processor.Settings;

public class BenchmarkOptions
{
    public const string SectionName = "Benchmark";

    /// <summary>
    /// Synthetic CPU work per message, in microseconds. When &gt; 0 the handler busy-spins for this
    /// long to emulate a CPU-bound processor (used for worker/buffer tuning). Default 0 = disabled.
    /// </summary>
    public int WorkMicros { get; set; }
}
