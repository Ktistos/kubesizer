class NoFeasibleConfigurationError(RuntimeError):
    """Raised when a finite search space contains no feasible configuration."""


class RequestClassMetricsUnavailableError(RuntimeError):
    """Raised when configured SLOs lack end-to-end request-class measurements."""


class StatisticalEvaluationError(RuntimeError):
    """Raised when calibrated simulation cannot produce a valid decision."""

    def __init__(self, message, evaluation):
        super().__init__(message)
        self.evaluation = evaluation
