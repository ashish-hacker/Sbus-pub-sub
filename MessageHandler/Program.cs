using Azure;
using System;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.ResourceManager;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager.ServiceBus;
using dotenv.net;

DotEnv.Load();

// Env variables
string connectionString = Environment.GetEnvironmentVariable("SERVICE_BUS_CONN_STRING");
string topicName = Environment.GetEnvironmentVariable("WORK_TOPIC_NAME");
string subscriptionName = Environment.GetEnvironmentVariable("SUBSCRIPTION_NAME");
string eventTopic = Environment.GetEnvironmentVariable("EVENT_TOPIC_NAME");

string subscriptionId = Environment.GetEnvironmentVariable("SUBSCRIPTION_ID");
string resourceGroupName = Environment.GetEnvironmentVariable("RESOURCE_GROUP_NAME");
string namespaceName = Environment.GetEnvironmentVariable("SERVICE_BUS_NAMESPACE");

// Authenticate to Azure ARM
var credential = new DefaultAzureCredential();
ArmClient armClient = new ArmClient(credential);

// Locate Service Bus namespace
var subscription = await armClient.GetSubscriptions().GetAsync(subscriptionId);
var resourceGroup = await subscription.Value.GetResourceGroups().GetAsync(resourceGroupName);
var namespaceResource = await resourceGroup.Value.GetServiceBusNamespaceAsync(namespaceName);
var topic = await namespaceResource.Value.GetServiceBusTopicAsync(topicName);

// Create subscription if not exists
var subscriptions = topic.Value.GetServiceBusSubscriptions();
bool exists = await subscriptions.ExistsAsync(subscriptionName);
if (!exists)
{
    Console.WriteLine($"🔧 Creating subscription '{subscriptionName}'...");
    await subscriptions.CreateOrUpdateAsync(
        WaitUntil.Completed,
        subscriptionName,
        new ServiceBusSubscriptionData());
}
else
{
    Console.WriteLine($"✅ Subscription '{subscriptionName}' already exists.");
}

// Setup SQL filter: operation = 'avsInsights'
var adminClient = new ServiceBusAdministrationClient(connectionString);

// Remove $Default rule (match all)
if (await adminClient.RuleExistsAsync(topicName, subscriptionName, "$Default"))
{
    await adminClient.DeleteRuleAsync(topicName, subscriptionName, "$Default");
    Console.WriteLine("🧹 Removed default rule.");
}

// Add custom rule if not present
string filterName = "OnlyAvsInsights";
if (!await adminClient.RuleExistsAsync(topicName, subscriptionName, filterName))
{
    var rule = new CreateRuleOptions
    {
        Name = filterName,
        Filter = new SqlRuleFilter("operation = 'avsInsights'")
    };

    await adminClient.CreateRuleAsync(topicName, subscriptionName, rule);
    Console.WriteLine("✅ Added SQL filter: operation = 'avsInsights'");
}
else
{
    Console.WriteLine("🔎 Filter 'OnlyAvsInsights' already exists.");
}

// Start listening
await using var client = new ServiceBusClient(connectionString);
ServiceBusProcessor processor = client.CreateProcessor(topicName, subscriptionName, new ServiceBusProcessorOptions());

processor.ProcessMessageAsync += async args =>
{
    var message = args.Message;
    string body = message.Body.ToString();

    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine("📥 [MessageHandler] Received filtered message:");
    Console.ResetColor();

    Console.WriteLine("─────────────────────────────");
    Console.WriteLine($"📝 Body: {body}");

    foreach (var prop in message.ApplicationProperties)
    {
        Console.WriteLine($"🔖 {prop.Key}: {prop.Value}");
    }
    Console.WriteLine("─────────────────────────────");

    // Process and respond to event topic
    var responseBody = $"{{\"status\":\"processed\",\"original\":{body}}}";
    var responseMessage = new ServiceBusMessage(responseBody)
    {
        ContentType = "application/json"
    };

    responseMessage.ApplicationProperties["operation"] = "avsInsights";
    responseMessage.ApplicationProperties["operationResource"] = "MessageHandler";

    var sender = client.CreateSender(eventTopic);
    await sender.SendMessageAsync(responseMessage);

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine("✅ Sent response to sddc-event-topic with operation = avsInsights");
    Console.ResetColor();

    await args.CompleteMessageAsync(message);
};

processor.ProcessErrorAsync += args =>
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"❌ Error in MessageHandler: {args.Exception.Message}");
    Console.ResetColor();
    return Task.CompletedTask;
};

Console.WriteLine("🟡 MessageHandler listening for operation = 'avsInsights'. Press any key to exit.");
await processor.StartProcessingAsync();
Console.ReadKey();
await processor.StopProcessingAsync();
