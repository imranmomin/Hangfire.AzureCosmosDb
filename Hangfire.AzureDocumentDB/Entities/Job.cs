using System;
using Hangfire.Storage;

namespace Hangfire.AzureDocumentDB.Entities
{
    internal class Job : FireEntity
    {
        public InvocationData InvocationData { get; set; }
        public string Arguments { get; set; }
        public string StateId { get; set; }
        public string StateName { get; set; }
        public Parameter[] Parameters { get; set; }
        public DateTime CreatedOn { get; set; }
    }
}
