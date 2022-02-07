using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace Hangfire.Azure.Helper;

internal static class QueryHelper
{
	internal static IEnumerable<T> ToQueryResult<T>(this FeedIterator<T> iterator)
	{
		while (iterator.HasMoreResults)
		{
			Task<FeedResponse<T>> task = iterator.ReadNextAsync();
			task.Wait();
			foreach (T item in task.Result)
			{
				yield return item;
			}
		}
	}

	internal static IEnumerable<T> ToQueryResult<T>(this IQueryable<T> queryable)
	{
		FeedIterator<T> iterator = queryable.ToFeedIterator();
		return iterator.ToQueryResult();
	}
}