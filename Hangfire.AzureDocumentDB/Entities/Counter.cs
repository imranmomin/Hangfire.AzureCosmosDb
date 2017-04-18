namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Counter : FireEntity
    {
        public string Key { get; set; }
        public int Value { get; set; }
        public CounterTypes Type { get; set; }
    }

    internal enum CounterTypes
    {
        Raw,
        Aggregrate
    }
}
