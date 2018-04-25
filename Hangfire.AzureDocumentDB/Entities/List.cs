using System;
using Newtonsoft.Json;
using Microsoft.Azure.Documents;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents
{
    internal class List : DocumentBase
    {
        [JsonProperty("key")]
        public string Key { get; set; }

        [JsonProperty("value")]
        public string Value { get; set; }

        [JsonProperty("created_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime CreatedOn { get; set; }

        public override DocumentTypes DocumentType => DocumentTypes.List;
    }
}
