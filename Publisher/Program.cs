using System;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using dotenv.net;

DotEnv.Load();

string connectionString = Environment.GetEnvironmentVariable("SERVICE_BUS_CONN_STRING");
string topicName = Environment.GetEnvironmentVariable("TOPIC_NAME");
if (string.IsNullOrWhiteSpace(topicName))
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine("❌ TOPIC_NAME environment variable is not set.");
    Console.ResetColor();
    return;
}

await using var client = new ServiceBusClient(connectionString);
ServiceBusSender sender = client.CreateSender(topicName);

for (int i = 1; i <= 10; i++)
{
    string userId = $"user-{i:D3}"; // e.g., user-001, user-002, ..., user-010

    var payload = new
    {
        resourceId = $"{userId}",
        resourceType = "test-2",
        action = "test",
        timestamp = DateTime.UtcNow
    };

    string jsonBody = JsonSerializer.Serialize(payload);

    var message = new ServiceBusMessage(jsonBody)
    {
        ContentType = "application/json",
        SessionId = userId,
        CorrelationId = Guid.NewGuid().ToString()
    };

    message.ApplicationProperties.Add("operation", "avsInsights");
    message.ApplicationProperties.Add("operationResource", "test");
    message.ApplicationProperties.Add("initiatedBy", "geneva-action");

    await sender.SendMessageAsync(message);
    Console.WriteLine($"📤 Sent message for {userId} with CorrelationId: {message.CorrelationId}");
}

Console.WriteLine("✅ All 10 messages sent.");
