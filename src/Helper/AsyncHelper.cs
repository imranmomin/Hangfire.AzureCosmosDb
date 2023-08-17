using System.Threading.Tasks;

namespace Hangfire.Azure.Helper;

internal static class AsyncHelper
{
    public static void ExecuteSynchronously(this Task task)
    {
        Task tempTask = Task.Run(async () => await task);
        tempTask.Wait();
    }

    public static T ExecuteSynchronously<T>(this Task<T> task)
    {
        T result = default!;
        Task tempTask = Task.Run(async () => result = await task);
        tempTask.Wait();
        return result!;
    }
}