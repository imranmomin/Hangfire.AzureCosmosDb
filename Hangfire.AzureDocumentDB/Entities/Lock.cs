namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Lock : FireEntity
    {
        public string Resource { get; set; }
    }
}
