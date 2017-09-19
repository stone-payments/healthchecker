using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using RabbitMQ.Abstraction.Messaging;
using RabbitMQ.Abstraction.Messaging.Interfaces;

namespace HealthChecker
{
    public class HealthCheckerInstance
    {
        private readonly List<RabbitMQClient> _rabbitMQTargets;

        private readonly List<SqlConnection> _sqlServerTargets;

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
            _sqlServerTargets = new List<SqlConnection>();
            _serviceTargets = new List<Uri>();
            _httpClient = new HttpClient();
        }

        public HealthCheckerInstance Add<T>(T targetData, bool required = true)
        {
            switch (targetData)
            {
                case RabbitMQClient client:
                {
                    if (required)
                    {
                        SetupQueueInfrastructure(client);
                    }

                    _rabbitMQTargets.Add(client);
                    break;
                }
                case SqlConnection connection:
                {
                    if (required)
                    {
                        AssertSQLServerInfrastructure(connection).Wait();
                    }

                    _sqlServerTargets.Add(connection);
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

        public HealthCheckerInstance Add(HealthCheckTargetType targetType, string targetData, bool required = true)
        {
            switch (targetType)
            {
                case HealthCheckTargetType.RabbitMQ:
                {
                    return Add(new RabbitMQClient(targetData), required);
                }
                case HealthCheckTargetType.SQLServer:
                {
                    return Add(new SqlConnection(targetData), required);
                }
                case HealthCheckTargetType.Service:
                {
                    return Add(new Uri(targetData), required);
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

        private void SetupQueueInfrastructure(IQueueClient client)
        {
            client.ExchangeDeclare(QueuingExchangeName);

            client.QueueDeclare(_queuingQueueName);

            client.QueueBind(_queuingQueueName, QueuingExchangeName, QueuingRoutingKey);
        }

        private async Task<IEnumerable<HealthCheckResult>> CheckRabbitMQ()
        {
            return await Task.WhenAll(_rabbitMQTargets.Select(c => Task.Run(() =>
            {
                var stopwatch = Stopwatch.StartNew();
                string errorInfo = null;

                try
                {
                    c.Publish(QueuingExchangeName, QueuingRoutingKey, DateTimeOffset.UtcNow);
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
                    HealthCheckTarget = HealthCheckTargetType.RabbitMQ,
                    ErrorInfo = errorInfo,
                    IsHealthy = errorInfo == null,
                    ResponseTime = stopwatch.Elapsed,
                    HealthCheckTargetIdentifier = c.ToString(),
                };
            })));
        }

        private static async Task AssertSQLServerInfrastructure(SqlConnection connection)
        {
            using (var cmd = new SqlCommand())
            {
                cmd.Connection = connection;
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = $"SELECT 1 FROM {SQLHealthTableName} WHERE 1 = 0";

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
                        cmd.Connection = s;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $@"INSERT INTO {SQLHealthTableName}({SQLHealthClientIdentifierColumn}) VALUES(@clientIdentifier)";

                        cmd.Parameters.AddWithValue("@clientIdentifier", GetClientIdentifier());

                        await s.OpenAsync();
                        await cmd.ExecuteNonQueryAsync();
                        s.Close();
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
                    HealthCheckTargetIdentifier = $"{s.Database}@{s.DataSource}",
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