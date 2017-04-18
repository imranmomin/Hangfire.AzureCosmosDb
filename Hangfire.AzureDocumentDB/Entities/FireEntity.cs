using System;
using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal abstract class FireEntity : IExpireEntity
    {
        [JsonProperty("id")]
        public string Id { get; set; } = Guid.NewGuid().ToString();

        [JsonProperty("_self")]
        public string SelfLink { get; set; }

        public DateTime? ExpireOn { get; set; }
    }
}
