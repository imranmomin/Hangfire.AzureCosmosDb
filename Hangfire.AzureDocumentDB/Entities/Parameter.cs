namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Parameter : FireEntity
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }
}