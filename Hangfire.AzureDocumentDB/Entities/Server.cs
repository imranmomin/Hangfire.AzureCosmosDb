using System;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Server : DocumentEntity
    {
        [JsonProperty("server_id")]
        public string ServerId { get; set; }

        [JsonProperty("workers")]
        public int Workers { get; set; }

        [JsonProperty("queues")]
        public string[] Queues { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        [JsonProperty("last_heartbeat")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime LastHeartbeat { get; set; }
    }
}