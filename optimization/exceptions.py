class NoFeasibleConfigurationError(RuntimeError):
    """Raised when a search returns no certified feasible configuration."""


class StatisticalEvaluationError(RuntimeError):
    """Raised when calibrated simulation cannot produce a valid decision."""

    def __init__(self, message, evaluation):
        super().__init__(message)
        self.evaluation = evaluation
