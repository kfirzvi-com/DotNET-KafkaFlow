using Microsoft.Extensions.Logging;
using Moq;
using Processor.Core.Diagnostics;

namespace Processor.Core.Tests.Diagnostics;

/// <summary>
/// KafkaFlow's log levels must land on the matching ILogger levels — otherwise a broker error shows up
/// as debug noise and nobody sees it.
/// </summary>
public class MicrosoftLogHandlerTests
{
    private static (MicrosoftLogHandler handler, Mock<ILogger<MicrosoftLogHandler>> logger) Create()
    {
        var logger = new Mock<ILogger<MicrosoftLogHandler>>();
        logger.Setup(l => l.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        return (new MicrosoftLogHandler(logger.Object), logger);
    }

    /// <summary>
    /// Verifies one log call at <paramref name="level"/>. The exception argument needs its own matcher
    /// per case — a conditional inside the expression tree is data to Moq, not a matcher.
    /// </summary>
    private static void VerifyLogged(Mock<ILogger<MicrosoftLogHandler>> logger, LogLevel level)
    {
        logger.Verify(
            l => l.Log(
                level,
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    /// <summary>Verifies one log call at <paramref name="level"/> that carries an exception.</summary>
    private static void VerifyLoggedWithException(
        Mock<ILogger<MicrosoftLogHandler>> logger, LogLevel level)
    {
        logger.Verify(
            l => l.Log(
                level,
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsNotNull<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void Error_LogsAtError_WithTheException()
    {
        var (handler, logger) = Create();

        handler.Error("broker unreachable", new InvalidOperationException("boom"), new { topic = "t" });

        VerifyLoggedWithException(logger, LogLevel.Error);
    }

    [Fact]
    public void Warning_LogsAtWarning()
    {
        var (handler, logger) = Create();

        handler.Warning("slow commit", new { topic = "t" });

        VerifyLogged(logger, LogLevel.Warning);
    }

    [Fact]
    public void Warning_WithException_LogsAtWarning_WithTheException()
    {
        var (handler, logger) = Create();

        handler.Warning("retrying", new TimeoutException("late"), new { topic = "t" });

        VerifyLoggedWithException(logger, LogLevel.Warning);
    }

    [Fact]
    public void Info_LogsAtInformation()
    {
        var (handler, logger) = Create();

        handler.Info("partitions assigned", new { count = 3 });

        VerifyLogged(logger, LogLevel.Information);
    }

    [Fact]
    public void Verbose_LogsAtDebug()
    {
        var (handler, logger) = Create();

        handler.Verbose("fetch loop tick", new { offset = 12 });

        VerifyLogged(logger, LogLevel.Debug);
    }
}
