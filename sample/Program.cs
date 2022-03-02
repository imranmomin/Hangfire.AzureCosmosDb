using Hangfire;
using Microsoft.Azure.Cosmos;
using Hangfire.Azure;
using Hangfire.AzureCosmosDB.Sample;
using LogLevel = Hangfire.Logging.LogLevel;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddScoped<ToDoService>();
builder.Services.AddControllers();
builder.Services.AddHangfireServer(x => { x.WorkerCount = 25; });
builder.Services.AddHangfireServer(x =>
{
	x.ServerName = "Server2";
	x.WorkerCount = 25;
});
builder.Services.AddHangfireServer(x =>
{
	x.ServerName = "Server3";
	x.WorkerCount = 25;
});

CosmosClientOptions cosmoClientOptions = new()
{
	ApplicationName = "hangfire",
	RequestTimeout = TimeSpan.FromSeconds(60),
	ConnectionMode = ConnectionMode.Direct,
	MaxRetryAttemptsOnRateLimitedRequests = 1,
	MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30)
};

// use cosmos emulator or free cosmos plan from azure
string url = "https://localhost:8081";
string secretkey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
string database = "hangfire";
string collection = "hangfire-test";

builder.Services.AddHangfire(o =>
{
	o.UseAzureCosmosDbStorage(url, secretkey, database, collection, cosmoClientOptions);
	o.UseColouredConsoleLogProvider(LogLevel.Trace);
});
JobStorage.Current = CosmosDbStorage.Create(url, secretkey, database, collection, cosmoClientOptions);

WebApplication app = builder.Build();
app.UseStaticFiles();
app.UseHangfireDashboard();
app.UseRouting();
app.UseEndpoints(x => { x.MapControllers(); });

using (IServiceScope scope = app.Services.CreateScope()) {
	ToDoService service = scope.ServiceProvider.GetRequiredService<ToDoService>();
	RecurringJob.AddOrUpdate("TO_DO_TASK_JOB", () => service.DoTask(), Cron.Minutely());
	RecurringJob.AddOrUpdate("TO_DO_ANOTHER_TASK_JOB", () => service.DoAnotherTask(), Cron.Minutely(), TimeZoneInfo.Local);
}

app.Run();




 