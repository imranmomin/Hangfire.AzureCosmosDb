using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.Azure.Helper
{
    public static class QueryHelper
    {
        public static List<T> ToQueryResult<T>(this IQueryable<T> source)
        {
            IDocumentQuery<T> query = source.AsDocumentQuery();
            List<T> results = new List<T>();

            while (query.HasMoreResults)
            {
                Task<FeedResponse<T>> task = Task.Run(async () => await query.ExecuteNextWithRetriesAsync());
                task.Wait();
                results.AddRange(task.Result);
            }

            return results;
        }
    }
}