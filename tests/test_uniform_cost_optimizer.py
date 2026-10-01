from types import SimpleNamespace

from optimization import GraphSearchProblem, UniformCostReplicaOptimizer


class SearchProblem(GraphSearchProblem):
    def __init__(self, graph, evaluations, *, state_ordering=None):
        self.graph = graph
        self.evaluations = evaluations
        self.state_ordering = state_ordering or {}
        self.evaluation_order = []

    def initial_state(self):
        return "start"

    def neighbors(self, state):
        return tuple(neighbor for neighbor, _ in self.graph[state])

    def step_cost(self, current, neighbor):
        return dict(self.graph[current])[neighbor]

    def search_tie_breaker(self, state):
        return self.state_ordering.get(state, 0.0)

    def evaluate(self, configuration):
        self.evaluation_order.append(configuration)
        return self.evaluations[configuration]

    def evaluate_and_check(self, configuration):
        evaluation = self.evaluate(configuration)
        return evaluation, evaluation.feasible

    def is_feasible(self, configuration):
        return self.evaluations[configuration].feasible


def evaluation(*, feasible=False, tie_breaker=0.0, classification=None):
    return SimpleNamespace(
        feasible=feasible,
        tie_breaker=tie_breaker,
        classification=classification,
    )


def test_uniform_cost_finds_the_lowest_actual_cost_path():
    problem = SearchProblem(
        {
            "start": (("a", 5), ("b", 1)),
            "b": (("a", 1),),
            "a": (("goal", 10),),
            "goal": (),
        },
        {
            "start": evaluation(),
            "a": evaluation(),
            "b": evaluation(),
            "goal": evaluation(feasible=True),
        },
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.configuration == "goal"
    assert result.cost == 12
    assert result.path == ("start", "b", "a", "goal")
    assert result.minimality_proven
    assert result.unresolved_lower_cost_states == ()


def test_same_cost_ordering_changes_speed_not_minimum_cost_or_goal_ranking():
    problem = SearchProblem(
        {
            "start": (("high-congestion", 1), ("low-congestion", 1)),
            "high-congestion": (),
            "low-congestion": (),
        },
        {
            "start": evaluation(),
            "high-congestion": evaluation(feasible=True, tie_breaker=0.2),
            "low-congestion": evaluation(feasible=True, tie_breaker=0.5),
        },
        state_ordering={"high-congestion": 0.9, "low-congestion": 0.1},
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert problem.evaluation_order == [
        "start",
        "low-congestion",
        "high-congestion",
    ]
    assert result.configuration == "high-congestion"
    assert result.cost == 1
    assert result.metrics.tie_breaker == 0.2


def test_lower_cost_inconclusive_state_prevents_a_proven_minimum_claim():
    problem = SearchProblem(
        {
            "start": (("uncertain", 1), ("bridge", 1)),
            "uncertain": (),
            "bridge": (("goal", 1),),
            "goal": (),
        },
        {
            "start": evaluation(classification="INFEASIBLE"),
            "uncertain": evaluation(classification="INCONCLUSIVE"),
            "bridge": evaluation(classification="INFEASIBLE"),
            "goal": evaluation(
                feasible=True,
                classification="FEASIBLE",
            ),
        },
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.configuration == "goal"
    assert result.cost == 2
    assert not result.minimality_proven
    assert result.unresolved_lower_cost_states == ("uncertain",)


def test_definitively_infeasible_lower_states_preserve_minimality_proof():
    problem = SearchProblem(
        {
            "start": (("infeasible", 1),),
            "infeasible": (("goal", 1),),
            "goal": (),
        },
        {
            "start": evaluation(classification="INFEASIBLE"),
            "infeasible": evaluation(classification="INFEASIBLE"),
            "goal": evaluation(
                feasible=True,
                classification="FEASIBLE",
            ),
        },
    )

    result = UniformCostReplicaOptimizer().optimize(problem)

    assert result.minimality_proven
    assert result.unresolved_lower_cost_states == ()
