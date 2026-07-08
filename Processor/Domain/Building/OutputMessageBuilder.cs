using Processor.Domain.Building.FieldBuilders;
using Processor.Domain.Messages;
using Processor.Domain.DataTypes;

namespace Processor.Domain.Building;

public class OutputMessageBuilder : IOutputMessageBuilder
{
    private readonly OutputIdBuilder _idBuilder;
    private readonly ProcessedContentBuilder _contentBuilder;
    private readonly ProcessedAtBuilder _processedAtBuilder;
    private readonly ProcessorNameBuilder _processorNameBuilder;
    private readonly IDataTypeSettingsRepository _settingsRepository;

    public OutputMessageBuilder(
        OutputIdBuilder idBuilder,
        ProcessedContentBuilder contentBuilder,
        ProcessedAtBuilder processedAtBuilder,
        ProcessorNameBuilder processorNameBuilder,
        IDataTypeSettingsRepository settingsRepository)
    {
        _idBuilder = idBuilder;
        _contentBuilder = contentBuilder;
        _processedAtBuilder = processedAtBuilder;
        _processorNameBuilder = processorNameBuilder;
        _settingsRepository = settingsRepository;
    }

    public async Task<BuildOutcome> Build(InputMessage input, string? dataTypeId, CancellationToken cancellationToken = default)
    {
        // Filter first: only records whose data type is active continue through the pipeline.
        // Reasons are bounded category codes (not the raw id) so they are safe as a metric label.
        if (string.IsNullOrWhiteSpace(dataTypeId))
        {
            return BuildOutcome.Filtered("missing_data_type_id");
        }

        var setting = await _settingsRepository.FindByIdAsync(dataTypeId, cancellationToken);
        if (setting is null)
        {
            return BuildOutcome.Filtered("unknown_data_type");
        }

        if (!setting.IsActive)
        {
            return BuildOutcome.Filtered("inactive_data_type");
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

        var message = new OutputMessage
        {
            Id = idResult.Value ?? string.Empty,
            ProcessedContent = contentResult.Value ?? string.Empty,
            ProcessedAt = processedAtResult.Value,
            ProcessorName = processorNameResult.Value ?? string.Empty
        };

        return BuildOutcome.Ok(message);
    }

    private static BuildOutcome ToOutcome<T>(FieldBuildResult<T> result)
    {
        var reason = result.Reason ?? "Unknown reason";

        return result.Status switch
        {
            BuildStatus.DeadLetter => BuildOutcome.DeadLetter(reason),
            BuildStatus.Drop => BuildOutcome.Drop(reason),
            _ => BuildOutcome.DeadLetter("Unexpected builder status")
        };
    }
}