using System;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents
{
    internal abstract class DocumentBase
    {
        [JsonProperty("id")]
        public string Id { get; set; } = Guid.NewGuid().ToString();

        [JsonProperty("_self")]
        public string SelfLink { get; set; }

        [JsonProperty("expire_on")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime? ExpireOn { get; set; }

        [JsonProperty("type")]
        public abstract DocumentTypes DocumentType { get; }
    }

    internal enum DocumentTypes
    {
        Server = 1,
        Job = 2,
        Queue = 3,
        Counter = 4,
        List = 5,
        Hash = 6,
        Set = 7,
        State = 8,
        Lock = 9
    }

    internal class ProcedureResponse
    {
        [JsonProperty("affected")]
        public int Affected { get; set; }

        [JsonProperty("continuation")]
        public bool Continuation { get; set; }
    }

    internal class Data<T>
    {
        [JsonProperty("items")]
        public T[] Items { get; set; }
    }
}
