using Processor.Core.Building;
using Processor.Core.Messages;

namespace Processor.Core.Tests.Support;

/// <summary>
/// A synthetic domain used only by the Core tests. Core's shared pipeline must work for
/// <em>any</em> domain, so testing it against a fake one proves the generics rather than the posts or
/// profiles rules — and keeps this project's only project reference on Core.
/// </summary>
public class WidgetData : IDomainData
{
    public const string Domain = "widgets";

    public string DomainName => Domain;

    public string Sku { get; set; } = string.Empty;

    public int Quantity { get; set; }
}

public sealed class WidgetInputMessage : InputMessage<WidgetData>
{
}

/// <summary>
/// Domain data builder whose behaviour each test dictates, so Core's orchestration can be driven
/// through every status a domain builder can return.
/// </summary>
public sealed class StubWidgetDataBuilder : IDomainDataBuilder<WidgetInputMessage, WidgetData>
{
    private readonly Func<WidgetInputMessage, FieldBuildResult<WidgetData>> _build;

    private StubWidgetDataBuilder(Func<WidgetInputMessage, FieldBuildResult<WidgetData>> build) => _build = build;

    /// <summary>Number of times the builder ran — lets tests assert short-circuiting.</summary>
    public int Invocations { get; private set; }

    /// <summary>Echoes the incoming payload with the SKU upper-cased, so tests can see it ran.</summary>
    public static StubWidgetDataBuilder Passthrough() => new(input => FieldBuildResult<WidgetData>.Ok(
        new WidgetData
        {
            Sku = input.DomainData.Sku.ToUpperInvariant(),
            Quantity = input.DomainData.Quantity
        }));

    public static StubWidgetDataBuilder DeadLettering(string reason) =>
        new(_ => FieldBuildResult<WidgetData>.DeadLetter(reason));

    public static StubWidgetDataBuilder Dropping(string reason) =>
        new(_ => FieldBuildResult<WidgetData>.Drop(reason));

    /// <summary>Returns Ok with a null value, to prove Core substitutes an empty payload.</summary>
    public static StubWidgetDataBuilder OkWithNullValue() =>
        new(_ => FieldBuildResult<WidgetData>.Ok(null!));

    public FieldBuildResult<WidgetData> Build(WidgetInputMessage input)
    {
        Invocations++;
        return _build(input);
    }
}
