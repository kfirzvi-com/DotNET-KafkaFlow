namespace Processor.Core.DataTypes;

/// <summary>
/// A single data-type setting as stored in the data store. Filtering keys off <see cref="IsActive"/>:
/// records whose data type is not active (or has no setting at all) are dropped.
/// </summary>
public class DataTypeSetting
{
    public string DataTypeId { get; set; } = string.Empty;

    public bool IsActive { get; set; }
}
