using Processor.Host;

var builder = WebApplication.CreateBuilder(args);

// Wires Core plus the single domain named by Processor:Domain. Throws on an unknown domain.
var domain = builder.AddProcessor();

var app = builder.Build();

app.Logger.LogInformation("Starting KafkaFlow processor for domain '{Domain}'", domain.Name);

app.MapProcessorEndpoints(domain);

await app.RunAsync();
