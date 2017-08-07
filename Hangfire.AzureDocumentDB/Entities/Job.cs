using System;

using Newtonsoft.Json;
using Hangfire.Storage;
using Microsoft.Azure.Documents;

namespace Hangfire.Azure.Documents
{
    internal class Job : DocumentBase
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

        public override DocumentTypes DocumentType => DocumentTypes.Job;
    }
}
