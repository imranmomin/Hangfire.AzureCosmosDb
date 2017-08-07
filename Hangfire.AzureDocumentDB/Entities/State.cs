using System;
using System.Collections.Generic;

using Newtonsoft.Json;
using Microsoft.Azure.Documents;

namespace Hangfire.Azure.Documents
{
    internal class State : DocumentBase
    {
        [JsonProperty("job_id")]
        public string JobId { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("reason")]
        public string Reason { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        [JsonProperty("data")]
        public Dictionary<string, string> Data { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.State;
    }
}