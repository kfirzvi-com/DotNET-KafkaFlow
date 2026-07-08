namespace Processor.Domain.DataTypes;

/// <summary>
/// A single data-type setting as stored in Redis. Filtering keys off <see cref="IsActive"/>:
/// records whose data type is not active (or has no setting at all) are dropped.
/// </summary>
public class DataTypeSetting
{
    public string DataTypeId { get; set; } = string.Empty;

    public bool IsActive { get; set; }
}
