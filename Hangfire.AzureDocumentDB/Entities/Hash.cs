namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Hash : FireEntity
    {
        public string Key { get; set; }
        public string Field { get; set; }
        public string Value { get; set; }
    }
}