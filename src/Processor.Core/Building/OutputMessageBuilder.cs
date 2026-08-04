using Processor.Core.Building.FieldBuilders;
using Processor.Core.DataTypes;
using Processor.Core.Messages;

namespace Processor.Core.Building;

/// <summary>
/// Orchestrates the shared field builders plus the domain's own data builder, closed over one
/// domain's types. Registered once per deployment as
/// <c>IOutputMessageBuilder&lt;PostInputMessage, PostData&gt;</c> (or the profiles equivalent), so DI
/// injects the builder for whichever domain <c>Processor:Domain</c> selects.
/// <para>
/// The shared builders are injected as <c>IOutputFieldBuilder&lt;IInputMessage, ...&gt;</c> —
/// contravariance means the same instances serve every domain.
/// </para>
/// </summary>
public class OutputMessageBuilder<TInput, TDomainData> : IOutputMessageBuilder<TInput, TDomainData>
    where TInput : InputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    private readonly IOutputFieldBuilder<IInputMessage, string> _idBuilder;
    private readonly IOutputFieldBuilder<IInputMessage, string> _contentBuilder;
    private readonly IOutputFieldBuilder<IInputMessage, DateTime> _processedAtBuilder;
    private readonly IOutputFieldBuilder<IInputMessage, string> _processorNameBuilder;
    private readonly IDomainDataBuilder<TInput, TDomainData> _domainDataBuilder;
    private readonly IDataTypeSettingsRepository _settingsRepository;

    public OutputMessageBuilder(
        OutputIdBuilder idBuilder,
        ProcessedContentBuilder contentBuilder,
        ProcessedAtBuilder processedAtBuilder,
        ProcessorNameBuilder processorNameBuilder,
        IDomainDataBuilder<TInput, TDomainData> domainDataBuilder,
        IDataTypeSettingsRepository settingsRepository)
    {
        _idBuilder = idBuilder;
        _contentBuilder = contentBuilder;
        _processedAtBuilder = processedAtBuilder;
        _processorNameBuilder = processorNameBuilder;
        _domainDataBuilder = domainDataBuilder;
        _settingsRepository = settingsRepository;
    }

    public async Task<BuildOutcome<TDomainData>> Build(
        TInput input, string? dataTypeId, CancellationToken cancellationToken = default)
    {
        // Filter first: only records whose data type is active continue through the pipeline.
        // Reasons are bounded category codes (not the raw id) so they are safe as a metric label.
        if (string.IsNullOrWhiteSpace(dataTypeId))
        {
            return BuildOutcome<TDomainData>.Filtered("missing_data_type_id");
        }

        var setting = await _settingsRepository.FindByIdAsync(dataTypeId, cancellationToken);
        if (setting is null)
        {
            return BuildOutcome<TDomainData>.Filtered("unknown_data_type");
        }

        if (!setting.IsActive)
        {
            return BuildOutcome<TDomainData>.Filtered("inactive_data_type");
        }

        var idResult = _idBuilder.Build(input);
        if (idResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(idResult);
        }

        var contentResult = _contentBuilder.Build(input);
        if (contentResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(contentResult);
        }

        var processedAtResult = _processedAtBuilder.Build(input);
        if (processedAtResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(processedAtResult);
        }

        var processorNameResult = _processorNameBuilder.Build(input);
        if (processorNameResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(processorNameResult);
        }

        // Domain-specific processing runs last, so a domain never pays for building its payload on a
        // message the shared rules would have rejected anyway.
        var domainDataResult = _domainDataBuilder.Build(input);
        if (domainDataResult.Status != BuildStatus.Ok)
        {
            return ToOutcome(domainDataResult);
        }

        var domainData = domainDataResult.Value ?? new TDomainData();

        var message = new OutputMessage<TDomainData>
        {
            Id = idResult.Value ?? string.Empty,
            ProcessedContent = contentResult.Value ?? string.Empty,
            ProcessedAt = processedAtResult.Value,
            ProcessorName = processorNameResult.Value ?? string.Empty,
            Domain = domainData.DomainName,
            DomainData = domainData
        };

        return BuildOutcome<TDomainData>.Ok(message);
    }

    private static BuildOutcome<TDomainData> ToOutcome<T>(FieldBuildResult<T> result)
    {
        var reason = result.Reason ?? "Unknown reason";

        return result.Status switch
        {
            BuildStatus.DeadLetter => BuildOutcome<TDomainData>.DeadLetter(reason),
            BuildStatus.Drop => BuildOutcome<TDomainData>.Drop(reason),
            _ => BuildOutcome<TDomainData>.DeadLetter("Unexpected builder status")
        };
    }
}
