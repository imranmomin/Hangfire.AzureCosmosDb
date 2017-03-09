using System;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class FireEntity : IExpireEntity
    {
        public DateTime? ExpireOn { get; set; }
    }
}
