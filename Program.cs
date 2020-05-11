using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;

namespace EventHubsReceiver
{
    //HostName=DemoIoTHubApp.azure-devices.net;DeviceId=MyDotnetDevice;SharedAccessKey=7gc2j30NMmj98WmMiRL5tPKw5D38REkCrpN16IHa+1k=
    class Program
    {
        private const string ehubNamespaceConnectionString = "Endpoint=sb://eventhubdemo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=6KamFRhaTkuktoSxD2/DNwTlSg2d0JhHyUcSs+JUeik=";
        private const string eventHubName = "eventhubtest";
        private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=storageactesting;AccountKey=gJ+9DQC0+bx5w783jt/YxcuZXDV2rTt7h22b31qbHEZGEEwNfylmuJCiIe2qPddzLpcuH2+2JKmI/6v0+qUvmg==;EndpointSuffix=core.windows.net";
        private const string blobContainerName = "blobforeventhubtest";
        static async Task Main()
        {
            
            // Read from the default consumer group: $Default
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a blob container client that the event processor will use 
            BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            // Create an event processor client to process events in the event hub
            EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, ehubNamespaceConnectionString, eventHubName);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            Console.WriteLine("Receiving data");
            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 10 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(10));

            // Stop the processing
            await processor.StopProcessingAsync();
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
