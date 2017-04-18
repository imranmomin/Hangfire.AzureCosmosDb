namespace Hangfire.AzureDocumentDB.Entities
{
    class Queue : FireEntity
    {
        public string Name { get; set; }
        public string JobId { get; set; }
    }
}
