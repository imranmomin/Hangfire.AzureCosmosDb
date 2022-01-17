using System;

using Hangfire.Storage;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents
{
    public class Job : DocumentBase
    {
        [JsonProperty("data")]
        public InvocationData InvocationData { get; set; } = null!;

        [JsonProperty("arguments")]
        public string Arguments { get; set; } = null!;

        [JsonProperty("state_id")]
        public string StateId { get; set; } = null!;

        [JsonProperty("state_name")]
        public string StateName { get; set; } = null!;

        [JsonProperty("parameters")]
        public Parameter[] Parameters { get; set; } = null!;

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.Job;
    }
}
