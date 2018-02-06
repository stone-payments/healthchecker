using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using RabbitMQ.Abstraction;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace HealthChecker
{
    public class HealthCheckerInstance
    {
        private readonly List<RabbitMQClient> _rabbitMQTargets;

        private readonly List<Tuple<SqlConnection, Dictionary<string, object>>> _sqlServerTargets;

        private readonly List<Uri> _serviceTargets;

        private readonly HttpClient _httpClient;

        private const string SQLHealthTableName = "Healthcheck";

        private const string SQLHealthClientIdentifierColumn = "ClientIdentifier";

        private const string QueuingExchangeName = "health";

        private const string QueuingRoutingKey = "check";

        private readonly string _queuingQueueName = $"health.{GetClientIdentifier()}";

        public HealthCheckerInstance()
        {
            _rabbitMQTargets = new List<RabbitMQClient>();
            _sqlServerTargets = new List<Tuple<SqlConnection, Dictionary<string, object>>>();
            _serviceTargets = new List<Uri>();
            _httpClient = new HttpClient();
        }

        public async Task<HealthCheckerInstance> AddAsync<T>(T targetData, bool required = true, Dictionary<string, object> additionalData = null)
        {
            switch (targetData)
            {
                case RabbitMQClient client:
                {
                    if (required)
                    {
                        await SetupQueueInfrastructureAsync(client);
                    }

                    _rabbitMQTargets.Add(client);
                    break;
                }
                case SqlConnection connection:
                {
                    if (required)
                    {
                        AssertSQLServerInfrastructure(connection, additionalData).Wait();
                    }

                    _sqlServerTargets.Add(new Tuple<SqlConnection, Dictionary<string, object>>(connection, additionalData ?? new Dictionary<string, object>()));
                    break;
                }
                case Uri uri:
                {
                    if (required)
                    {
                        AssertService(uri).Wait();
                    }

                    _serviceTargets.Add(uri);
                    break;
                }
            }

            return this;
        }

        public Task<HealthCheckerInstance> AddAsync(HealthCheckTargetType targetType, string targetData, bool required = true, Dictionary<string, object> additionalData = null)
        {
            switch (targetType)
            {
                case HealthCheckTargetType.RabbitMQ:
                {
                    return AddAsync(new RabbitMQClient(targetData), required, additionalData);
                }
                case HealthCheckTargetType.SQLServer:
                {
                    return AddAsync(new SqlConnection(targetData), required, additionalData);
                }
                case HealthCheckTargetType.Service:
                {
                    return AddAsync(new Uri(targetData), required, additionalData);
                }
                default:
                {
                    throw new Exception("Unsupported target type");
                }
            }
        }

        public async Task<IEnumerable<HealthCheckResult>> Check()
        {
            var tasks = new List<Task<IEnumerable<HealthCheckResult>>>
            {
                CheckRabbitMQ(),
                CheckSQLServer(),
                CheckServices()
            };

            return (await Task.WhenAll(tasks)).SelectMany(t => t);
        }

        private static string GetClientIdentifier()
        {
            return System.Reflection.Assembly.GetEntryAssembly().GetName().Name;
        }

        private async Task SetupQueueInfrastructureAsync(IQueueClient client)
        {
            await client.ExchangeDeclareAsync(QueuingExchangeName);

            await client.QueueDeclareAsync(_queuingQueueName);

            await client.QueueBindAsync(_queuingQueueName, QueuingExchangeName, QueuingRoutingKey);
        }

        private async Task<IEnumerable<HealthCheckResult>> CheckRabbitMQ()
        {
            var healthCheckResults = new ConcurrentBag<HealthCheckResult>();

            await _rabbitMQTargets.ForEachAsync(async t =>
            {
                var stopwatch = Stopwatch.StartNew();
                string errorInfo = null;

                try
                {
                    await t.PublishAsync(QueuingExchangeName, QueuingRoutingKey, DateTimeOffset.UtcNow);
                }
                catch (Exception e)
                {
                    errorInfo = e.Message;
                }
                finally
                {
                    stopwatch.Stop();
                }

                healthCheckResults.Add(new HealthCheckResult
                {
                    HealthCheckTarget = HealthCheckTargetType.RabbitMQ,
                    ErrorInfo = errorInfo,
                    IsHealthy = errorInfo == null,
                    ResponseTime = stopwatch.Elapsed,
                    HealthCheckTargetIdentifier = t.ToString(),
                });
            });

            return healthCheckResults;
        }

        private static async Task AssertSQLServerInfrastructure(SqlConnection connection, IReadOnlyDictionary<string, object> additionalData = null)
        {
            using (var cmd = new SqlCommand())
            {
                cmd.Connection = connection;
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = additionalData!= null && additionalData.ContainsKey("query") ? (string)additionalData["query"] : $"SELECT 1 FROM {SQLHealthTableName} WHERE 1 = 0";

                await connection.OpenAsync();
                await cmd.ExecuteNonQueryAsync();
                connection.Close();
            }
        }

        private async Task<IEnumerable<HealthCheckResult>> CheckSQLServer()
        {
            return await Task.WhenAll(_sqlServerTargets.Select(s => Task.Run(async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                string errorInfo = null;

                try
                {
                    using (var cmd = new SqlCommand())
                    {
                        cmd.Connection = s.Item1;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = s.Item2 != null && s.Item2.ContainsKey("query") ? (string)s.Item2["query"] : $@"INSERT INTO {SQLHealthTableName}({SQLHealthClientIdentifierColumn}) VALUES(@clientIdentifier)";

                        cmd.Parameters.AddWithValue("@clientIdentifier", GetClientIdentifier());

                        await s.Item1.OpenAsync();
                        await cmd.ExecuteNonQueryAsync();
                        s.Item1.Close();
                    }
                }
                catch (Exception e)
                {
                    errorInfo = e.Message;
                }
                finally
                {
                    stopwatch.Stop();
                }

                return new HealthCheckResult
                {
                    HealthCheckTarget = HealthCheckTargetType.SQLServer,
                    ErrorInfo = errorInfo,
                    IsHealthy = errorInfo == null,
                    ResponseTime = stopwatch.Elapsed,
                    HealthCheckTargetIdentifier = $"{s.Item1.Database}@{s.Item1.DataSource}",
                };
            })));
        }

        private async Task<IEnumerable<HealthCheckResult>> CheckServices()
        {
            return await Task.WhenAll(_serviceTargets.Select(CheckService));
        }

        private async Task<HealthCheckResult> CheckService(Uri uri)
        {
            var stopwatch = Stopwatch.StartNew();
            string errorInfo = null;

            try
            {
                (await _httpClient.GetAsync(uri)).EnsureSuccessStatusCode();
            }
            catch (Exception e)
            {
                errorInfo = e.Message;
            }
            finally
            {
                stopwatch.Stop();
            }

            return new HealthCheckResult
            {
                HealthCheckTarget = HealthCheckTargetType.Service,
                ErrorInfo = errorInfo,
                IsHealthy = errorInfo == null,
                ResponseTime = stopwatch.Elapsed,
                HealthCheckTargetIdentifier = uri.ToString(),
            };
        }

        private async Task AssertService(Uri uri)
        {
            var health = await CheckService(uri);

            if (!health.IsHealthy)
            {
                throw new Exception(health.ErrorInfo);
            }
        }
    }
}