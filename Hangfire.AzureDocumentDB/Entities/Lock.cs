using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Lock : DocumentEntity
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        public override DocumentTypes DocumentType { get; set; } = DocumentTypes.Lock;
    }
}
