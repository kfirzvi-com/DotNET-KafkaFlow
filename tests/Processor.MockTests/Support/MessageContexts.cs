using KafkaFlow;
using Moq;

namespace Processor.MockTests.Support;

/// <summary>
/// Builds message contexts the way KafkaFlow delivers them. Uses KafkaFlow's real
/// <see cref="MessageHeaders"/> (its <c>GetString</c> is an extension method and cannot be mocked), so
/// header decoding is genuinely exercised.
/// </summary>
public static class MessageContexts
{
    public const string DataTypeHeaderName = "data-type-id";

    /// <summary>
    /// A context carrying <paramref name="dataTypeId"/> either as the message key (the default) or as
    /// the Kafka header. Null means neither is present.
    /// </summary>
    public static IMessageContext For(object message, string? dataTypeId, bool useHeader = false)
    {
        var headers = new MessageHeaders();
        object? key = null;

        if (dataTypeId is not null)
        {
            if (useHeader)
            {
                headers.Add(DataTypeHeaderName, System.Text.Encoding.UTF8.GetBytes(dataTypeId));
            }
            else
            {
                key = dataTypeId;
            }
        }

        var context = new Mock<IMessageContext>();
        context.Setup(c => c.Message).Returns(new Message(key!, message));
        context.Setup(c => c.Headers).Returns(headers);
        return context.Object;
    }
}
