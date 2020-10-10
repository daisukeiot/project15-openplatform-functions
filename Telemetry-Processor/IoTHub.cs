using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using System;
using Microsoft.Azure.Devices;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.Azure.Devices.Shared;

namespace Solution_Accelerator
{
    public static class IoTHubEvent
    {
        private static HttpClient client = new HttpClient();
        private static String IoTHubConnectionString = Environment.GetEnvironmentVariable("IoTHubCS");
        private static String PatliteDeviceId = Environment.GetEnvironmentVariable("PatliteDeviceId");
        private static RegistryManager registryManager = null;

        [FunctionName("IoTHubEvent")]
        public static void Run([IoTHubTrigger("messages/events", Connection = "IoTHubEventHub")] EventData message,
                               ILogger log)
        {

            if (registryManager == null)
            {
                registryManager = RegistryManager.CreateFromConnectionString(IoTHubConnectionString);
            }

            if (message.Properties.ContainsKey("MessageType"))
            {
                if (message.Properties["MessageType"].Equals("Patlite"))
                {
                    var patliteMsg = Encoding.UTF8.GetString(message.Body.Array);
                    log.LogInformation($"Patlite Message Received: {patliteMsg}");

                    UpdatePatliteTwin(patliteMsg, log).Wait();
                }
            }
        }

        public static async Task UpdatePatliteTwin(String message, ILogger log)
        {
            var twin = await registryManager.GetTwinAsync(PatliteDeviceId);

            var jsonMessage = message.Replace("\"", "'");
            jsonMessage = jsonMessage.Replace("\\", "");
            jsonMessage = jsonMessage.Replace("'{", "{");
            jsonMessage = jsonMessage.Replace("}'", "}");

            PATLITE_DATA patlite = JsonConvert.DeserializeObject<PATLITE_DATA>(jsonMessage);

            var desired = new TwinCollection();

            desired["led_red"] = patlite.person ? 1 : 0;
            desired["led_blue"] = patlite.elephant ? 1 : 0;
            desired["led_yellow"] = patlite.giraffe ? 1 : 0;
            desired["led_white"] = patlite.zebra ? 1 : 0;

            if (desired.Count > 0)
            {
                try
                {
                    twin.Properties.Desired = desired;
                    await registryManager.UpdateTwinAsync(PatliteDeviceId, twin, twin.ETag);
                }
                catch (Exception e)
                {
                }
            }
        }

        public class PATLITE_DATA
        {
            public bool elephant { get; set; }
            public bool zebra { get; set; }
            public bool giraffe { get; set; }
            public bool person { get; set; }
        }

    }
}