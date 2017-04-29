using System;
using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Job : DocumentEntity
    {
        [JsonProperty("data")]
        public InvocationData InvocationData { get; set; }

        [JsonProperty("arguments")]
        public string Arguments { get; set; }

        [JsonProperty("state_id")]
        public string StateId { get; set; }

        [JsonProperty("state_name")]
        public string StateName { get; set; }

        [JsonProperty("parameters")]
        public Parameter[] Parameters { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        public override DocumentTypes DocumentType { get; set; } = DocumentTypes.Job;
    }
}
