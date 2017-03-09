using System;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal interface IExpireEntity
    {
        DateTime? ExpireOn { get; set; }
    }
}
