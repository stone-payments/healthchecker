using System;

namespace HealthChecker
{
    public class HealthCheckResult
    {
        public HealthCheckTargetType HealthCheckTarget { get; set; }

        public TimeSpan ResponseTime { get; set; }

        public string HealthCheckTargetIdentifier { get; set; }

        public string ErrorInfo { get; set; }

        public bool IsHealthy { get; set; }
    }
}