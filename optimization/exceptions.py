class NoFeasibleConfigurationError(RuntimeError):
    """Raised when a finite search space contains no feasible configuration."""


class RequestClassMetricsUnavailableError(RuntimeError):
    """Raised when configured SLOs lack end-to-end request-class measurements."""
