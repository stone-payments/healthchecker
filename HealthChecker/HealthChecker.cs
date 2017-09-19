namespace HealthChecker
{
    public static class HealthChecker
    {
        private static HealthCheckerInstance _healthCheckerInstance;

        public static HealthCheckerInstance Get()
        {
            return _healthCheckerInstance ?? (_healthCheckerInstance = new HealthCheckerInstance());
        }
    }
}
