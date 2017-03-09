using System;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Server : FireEntity
    {
        public string ServerId { get; set; }
        public int Workers { get; set; }
        public string[] Queues { get; set; }
        public DateTime CreatedOn { get; set; }
        public DateTime LastHeartbeat { get; set; }
    }
}