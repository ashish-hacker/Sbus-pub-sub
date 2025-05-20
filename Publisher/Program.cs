using System.Text.Json;
using Azure.Messaging.ServiceBus;
using dotenv.net;

DotEnv.Load();

string connectionString = Environment.GetEnvironmentVariable("SERVICE_BUS_CONN_STRING");
string topicName = Environment.GetEnvironmentVariable("TOPIC_NAME");

await using var client = new ServiceBusClient(connectionString);
ServiceBusSender sender = client.CreateSender(topicName);

// Create a sample payload object
var payload = new
{
    resourceId = "abc123456",
    resourceType = "test-1",
    action = "test",
    timestamp = DateTime.UtcNow
};

// Serialize to JSON string
string jsonBody = JsonSerializer.Serialize(payload);

// Create the message with the JSON body
var message = new ServiceBusMessage(jsonBody)
{
    ContentType = "application/json"
};

// Optional: Add custom properties
message.ApplicationProperties.Add("operation", "avsInsights");
message.ApplicationProperties.Add("operationResource", "test");
message.ApplicationProperties.Add("initiatedBy", "geneva-action");

await sender.SendMessageAsync(message);
Console.WriteLine($"✅ Publisher: Sent to topic: {topicName}");
