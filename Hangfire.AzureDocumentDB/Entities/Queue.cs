using Newtonsoft.Json;

namespace Hangfire.AzureDocumentDB.Entities
{
    class Queue : DocumentEntity
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("job_id")]
        public string JobId { get; set; }

        public override DocumentTypes DocumentType { get; set; } = DocumentTypes.Queue;
    }
}
