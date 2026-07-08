using Processor.Diagnostics;
using Xunit;

namespace Processor.Tests.Diagnostics;

public class KafkaConsumerStatisticsTests
{
    private const string Sample = """
    {
      "rxmsgs": 1000,
      "txmsgs": 5,
      "cgrp": { "state": "up", "rebalance_cnt": 3, "assignment_size": 4 },
      "topics": {
        "input-topic": { "partitions": {
          "-1": { "consumer_lag": -1, "fetchq_cnt": 0 },
          "0":  { "consumer_lag": 100, "fetchq_cnt": 2 },
          "1":  { "consumer_lag": 50,  "fetchq_cnt": 1 }
        }}
      },
      "brokers": {
        "b1": { "rtt": { "avg": 2000 } },
        "b2": { "rtt": { "avg": 4000 } },
        "bootstrap": { "rtt": { "avg": 0 } }
      }
    }
    """;

    [Fact]
    public void Handle_ParsesConsumerStatistics()
    {
        var stats = new KafkaConsumerStatistics();

        stats.Handle(Sample);

        Assert.Equal(1000, stats.RxMessages);
        Assert.Equal(5, stats.TxMessages);
        Assert.Equal(3, stats.Rebalances);
        Assert.Equal(4, stats.AssignmentPartitions);
        Assert.Equal(150, stats.ConsumerLag);      // 100 + 50; the -1 (internal) partition is skipped
        Assert.Equal(3, stats.FetchqMessages);     // 2 + 1
        Assert.Equal(3.0, stats.BrokerRttMsAvg);   // (2ms + 4ms) / 2; bootstrap (0) ignored
        Assert.Equal(4.0, stats.BrokerRttMsMax);
    }

    [Fact]
    public void Handle_DoesNotThrow_OnMalformedJson()
    {
        var stats = new KafkaConsumerStatistics();
        stats.Handle("not json");
        stats.Handle("{}");
        Assert.Equal(0, stats.ConsumerLag);
    }
}
