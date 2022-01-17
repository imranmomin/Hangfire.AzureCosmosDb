using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace Hangfire.Azure.Helper
{
    public static class QueryHelper
    {
        public static IEnumerable<T> ToQueryResult<T>(this FeedIterator<T> iterator)
        {
            while (iterator.HasMoreResults)
            {
                Task<FeedResponse<T>> task = Task.Run(async () => await iterator.ReadNextAsync());
                task.Wait();
                foreach (T item in task.Result)
                {
                    yield return item;
                }
            }
        }

        public static IEnumerable<T> ToQueryResult<T>(this IQueryable<T> queryable)
        {
            FeedIterator<T> iterator = queryable.ToFeedIterator();
            return iterator.ToQueryResult();
        }
    }
}