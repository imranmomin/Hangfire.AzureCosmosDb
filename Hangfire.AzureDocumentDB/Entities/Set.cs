namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Set : FireEntity
    {
        public string Key { get; set; }
        public string Value { get; set; }
        public double Score { get; set; }
    }
}