using System;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal abstract class DocumentEntity
    {
        [JsonProperty("id")]
        public string Id { get; set; } = Guid.NewGuid().ToString();

        [JsonProperty("_self")]
        public string SelfLink { get; set; }

        [JsonProperty("expire_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime? ExpireOn { get; set; }

        [JsonProperty("type")]
        public abstract DocumentTypes DocumentType { get; set; }
    }

    internal enum DocumentTypes
    {
        Server,
        Job,
        Queue,
        Counter,
        List,
        Hash,
        Set,
        State,
        Lock
    }
}
