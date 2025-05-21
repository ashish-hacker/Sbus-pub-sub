using Azure;
using System;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.ResourceManager;
using Azure.ResourceManager.ServiceBus;
using Azure.ResourceManager.Resources;
using dotenv.net;

DotEnv.Load();

string connectionString = Environment.GetEnvironmentVariable("SERVICE_BUS_CONN_STRING");
string topicName = Environment.GetEnvironmentVariable("TOPIC_NAME");
string subscriptionName = Environment.GetEnvironmentVariable("SUBSCRIPTION_NAME");

string subscriptionId = Environment.GetEnvironmentVariable("SUBSCRIPTION_ID");
string resourceGroupName = Environment.GetEnvironmentVariable("RESOURCE_GROUP_NAME");
string namespaceName = Environment.GetEnvironmentVariable("SERVICE_BUS_NAMESPACE");

// Authenticate to Azure
var credential = new DefaultAzureCredential();
ArmClient armClient = new ArmClient(credential);

// Locate the Service Bus Namespace resource
var subscription = await armClient.GetSubscriptions().GetAsync(subscriptionId);
var resourceGroup = await subscription.Value.GetResourceGroups().GetAsync(resourceGroupName);
var namespaceResource = await resourceGroup.Value.GetServiceBusNamespaceAsync(namespaceName);

// Get topic
ServiceBusTopicResource topic = await namespaceResource.Value.GetServiceBusTopicAsync(topicName);

// Check if subscription exists
var subscriptions = topic.GetServiceBusSubscriptions();
bool exists = await subscriptions.ExistsAsync(subscriptionName);

if (!exists)
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"🔧 Creating subscription '{subscriptionName}'...");
    Console.ResetColor();

    var subscriptionData = new ServiceBusSubscriptionData
    {
        RequiresSession = true
    };

    await subscriptions.CreateOrUpdateAsync(
        WaitUntil.Completed,
        subscriptionName,
        subscriptionData);
}
else
{
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"✅ Subscription '{subscriptionName}' already exists.");
    Console.ResetColor();
}

// Setup filter: operation = 'avsInsights'
var adminClient = new ServiceBusAdministrationClient(connectionString);

// Remove the default rule if it exists
if (await adminClient.RuleExistsAsync(topicName, subscriptionName, "$Default"))
{
    await adminClient.DeleteRuleAsync(topicName, subscriptionName, "$Default");
    Console.WriteLine("🧹 Removed default rule ($Default).");
}

// Add the custom filter rule if not already added
const string filterName = "AvsInsightsFilter";
if (!await adminClient.RuleExistsAsync(topicName, subscriptionName, filterName))
{
    var rule = new CreateRuleOptions
    {
        Name = filterName,
        Filter = new SqlRuleFilter("operation = 'avsInsights'")
    };

    await adminClient.CreateRuleAsync(topicName, subscriptionName, rule);
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine("✅ Added SQL filter: operation = 'avsInsights'");
    Console.ResetColor();
}
else
{
    Console.WriteLine("🔎 Filter 'AvsInsightsFilter' already exists.");
}

// Start listening to messages
await using var client = new ServiceBusClient(connectionString);
ServiceBusSessionProcessor processor = client.CreateSessionProcessor(
    topicName, subscriptionName,
    new ServiceBusSessionProcessorOptions
    {
        SessionIds = {"user-008", "user-009"},
        MaxConcurrentSessions = 10,
        AutoCompleteMessages = false
    });

processor.ProcessMessageAsync += async args =>
{
    string sessionId = args.Message.SessionId;
    string body = args.Message.Body.ToString();

    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine("📨 New message received:");
    Console.ResetColor();

    Console.WriteLine("─────────────────────────────────────");
    Console.WriteLine($"📦 Body: {body}");

    foreach (var prop in args.Message.ApplicationProperties)
    {
        Console.WriteLine($"🔖 {prop.Key}: {prop.Value}");
    }

    Console.WriteLine("─────────────────────────────────────");

    Console.WriteLine($"📥 Response for {sessionId}: {body}");
    await args.CompleteMessageAsync(args.Message);
};

processor.ProcessErrorAsync += args =>
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"❌ Error: {args.Exception.Message}");
    Console.ResetColor();
    return Task.CompletedTask;
};

Console.WriteLine("🟢 Subscriber is listening. Press any key to exit...");
await processor.StartProcessingAsync();
Console.ReadKey();
await processor.StopProcessingAsync();
