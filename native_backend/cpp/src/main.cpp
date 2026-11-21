#include <jansson.h>
#include "kubesizer/custom_distribution.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <dlfcn.h>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <optional>
#include <queue>
#include <random>
#include <stdexcept>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

constexpr int kProtocolVersion = 18;
constexpr int kUrgentPriority = 0;
constexpr int kNormalPriority = 1;
constexpr int kDeadlinePriority = 2;

enum class EventKind {
    ExternalArrival,
    Admit,
    ServiceCompletion,
    CpuBandwidthBoundary,
    BlockingCompletion,
    ServiceResponse,
    Grant,
    Resume,
    ConcurrencyGrant,
    CallReturn,
    StageReady,
    StageContinuationReturn,
    FinalExit,
    RequestTimeout, AdmissionTimeout, RateWake, InstanceReady, InstanceIdle,
    RetryReady, AttemptDeadline
};
enum class RunMode { FixedHorizon, MeasurementDrain };
enum class FlowKind { Routing, Workflow, Calls, Stages };
enum class CallExecution { Parallel, Sequential, Choice };
enum class CallCompletion { All, Quorum, Background };

struct CallPolicy {
    CallExecution execution{CallExecution::Parallel};
    CallCompletion completion{CallCompletion::All};
    std::int64_t required{};
};
enum class ExecutionModel { FixedSlots, ProcessorSharing, Delay };
enum class ReplicaQueueModel { Pooled, IndependentRandom, IndependentRoundRobin, IndependentKeyHash };
enum class DistributionKind {
    Exponential,
    Deterministic,
    Gamma,
    Lognormal,
    Weibull,
    Empirical,
    ArrivalTrace,
    Custom
};

struct Distribution {
    DistributionKind kind{};
    double mean{};
    double scv{};
    double shape{};
    double block_duration{};
    bool bayesian_bootstrap{};
    std::vector<double> samples;
    std::vector<std::vector<double>> sample_clusters;
    std::shared_ptr<void> custom_library_handle;
    kubesizer_custom_sample_fn custom_sampler{};
    std::vector<double> custom_parameters;
};

struct CacheSpec {
    bool fixed_ratio{};
    std::uint64_t capacity{};
    double ttl{};
    int observed_replicas{};
    std::vector<double> hit_ratios;
    std::vector<std::string> keys;
    std::vector<std::uint64_t> sizes;
    std::vector<std::vector<int>> accesses;
    std::vector<bool> cyclic;
};

struct CacheState {
    struct Entry { std::list<int>::iterator position; double expires; };
    std::list<int> lru;
    std::unordered_map<int, Entry> entries;
    std::priority_queue<std::pair<double,int>,std::vector<std::pair<double,int>>,std::greater<std::pair<double,int>>> expiry;
    std::uint64_t bytes{};
    CacheState() = default;
    CacheState(const CacheState&) = delete;
    CacheState& operator=(const CacheState&) = delete;
    CacheState(CacheState&&) noexcept = default;
    CacheState& operator=(CacheState&&) noexcept = default;
};

struct Service {
    std::string name;
    int replicas{};
    int capacity{};
    int concurrency_capacity{};
    ExecutionModel execution_model{ExecutionModel::FixedSlots};
    ReplicaQueueModel replica_queue_model{ReplicaQueueModel::Pooled};
    std::vector<Distribution> service_times;
    std::vector<std::optional<Distribution>> blocking_times;
    std::optional<Distribution> post_release_delay;
    double cpu_quota{};
    double cpu_period{};
    int cpu_parallelism{};
    int pending_capacity{-1};
    double pending_timeout{};
    double token_rate{};
    int token_burst{};
    bool token_reject{};
    double failure_probability{};
    std::optional<Distribution> startup_delay;
    double idle_timeout{};
    bool initially_warm{true};
    std::optional<CacheSpec> cache;
};

struct Route {
    int target{-1};
    double probability{};
};

struct WorkflowVisit {
    int service{-1};
    double mean_visits{};
};

struct Stage {
    std::string name;
    int resource{-1};
    std::optional<Distribution> service_time;
    std::optional<Distribution> blocking_time;
    std::optional<Distribution> delay;
    double work_units{1.0};
    bool hold_resource{};
    std::vector<WorkflowVisit> calls;
    CallPolicy policy;
    int next{-1};
    int retry_max{};
    std::optional<Distribution> retry_backoff;
    double attempt_timeout{};
    bool cache_operation{};
    int cache_action{}; // lookup=0, put=1, invalidate=2
    int cache_hit{-1};
    int cache_miss{-1};
    bool cache_fill{true};
};

struct RequestClass {
    std::string name;
    double concurrency_weight{};
    FlowKind flow_kind{FlowKind::Routing};
    std::vector<std::vector<Route>> routes;
    std::vector<WorkflowVisit> workflow;
    std::vector<std::vector<WorkflowVisit>> calls;
    CallExecution call_execution{CallExecution::Parallel};
    std::vector<CallPolicy> call_policies;
    std::vector<Stage> stages;
    std::vector<int> stage_entries;
    std::optional<Distribution> latency_overhead;
    std::vector<Distribution> latency_components;
    double end_to_end_timeout{};
    bool all_work_completion{};
};

struct ExternalStream {
    int request_class{};
    int entry_service{};
    Distribution interarrival;
};

struct InputEpoch {
    std::vector<double> arrival_rate_multipliers;
    std::vector<double> service_time_multipliers;
};

struct TraceSamplerState {
    std::size_t next_index{};
    std::size_t remaining{};
};

struct Workload {
    std::string name;
    std::vector<Service> services;
    std::vector<RequestClass> request_classes;
    std::vector<ExternalStream> external_streams;
    std::vector<InputEpoch> input_epochs;
    std::size_t input_block_length{};
    std::optional<Distribution> same_node_one_way_latency;
    std::optional<Distribution> cross_node_one_way_latency;
    double cross_node_probability{};
};

struct Request {
    std::int64_t id{};
    int request_class{};
    double external_entry_time{};
    std::size_t ownership_index{};
    bool latency_eligible{};
    bool measurement_tagged{};
    std::size_t workflow_index{};
    std::int64_t workflow_visits_remaining{};
    std::int64_t generated_visits{};
    bool outcome_recorded{};
    bool server_completed{};
    bool deadline_processed{};
    std::int64_t live_visits{};
    bool failed{};
    bool deferred_response{};
    bool work_failed{};
    std::vector<int> cache_keys;
};

struct Visit {
    Request* request{};
    int service{-1};
    int replica{-1};
    Visit* parent{};
    std::size_t ownership_index{};
    std::int64_t pending_children{};
    double arrival_time{};
    std::vector<int> sequential_children;
    std::size_t next_sequential_child{};
    double return_network_delay{};
    double remaining_work{};
    std::int64_t returned_children{};
    bool completion_started{};
    bool response_completed{};
    bool response_sample_recorded{};
    int stage{-1};
    bool stage_phase_done{};
    bool continuation_pending{};
    bool is_continuation{};
    bool resource_released{};
    bool admitted{};
    bool failed{};
    std::int64_t failed_children{};
    std::uint64_t serial{};
    std::uint64_t wait_generation{};
    std::uint64_t retry_generation{};
    std::uint64_t child_attempt{};
    int attempts_started{};
    bool attempt_active{};
    bool cache_missed{};
    std::uint64_t cache_incarnation{};
};

struct Event {
    double time{};
    int priority{};
    std::uint64_t sequence{};
    EventKind kind{};
    Request* request{};
    Visit* visit{};
    int index{};
    int queue{-1};
    std::uint64_t generation{};
    std::size_t visit_slot{};
    std::uint64_t visit_serial{};
    bool failed{};
};

struct ProcessorSharingEntry {
    double finish_tag{};
    std::int64_t request_id{};
    Visit* visit{};
};

struct LaterProcessorSharingEntry {
    bool operator()(
        const ProcessorSharingEntry& left,
        const ProcessorSharingEntry& right
    ) const {
        if (left.finish_tag != right.finish_tag) {
            return left.finish_tag > right.finish_tag;
        }
        return left.request_id > right.request_id;
    }
};

struct LaterEvent {
    bool operator()(const Event& left, const Event& right) const {
        if (left.time != right.time) return left.time > right.time;
        if (left.priority != right.priority) return left.priority > right.priority;
        return left.sequence > right.sequence;
    }
};

struct Point {
    double time{};
    int value{};
};

void record_change(std::vector<Point>& history, double time, int value) {
    Point& previous = history.back();
    if (time < previous.time) throw std::runtime_error("state history moved backwards");
    if (time == previous.time) {
        if (value == previous.value) return;
        previous.value = value;
        if (history.size() > 1 && history[history.size() - 2].value == value) {
            history.pop_back();
        }
        return;
    }
    if (value != previous.value) history.push_back({time, value});
}

class TimeWeightedBuckets {
  public:
    TimeWeightedBuckets(double width, double horizon)
        : width_(width), horizon_(horizon) {
        if (!std::isfinite(width_) || width_ <= 0.0) {
            throw std::runtime_error("bucket_width must be finite and positive");
        }
        if (!std::isfinite(horizon_) || horizon_ <= 0.0) {
            throw std::runtime_error("bucket horizon must be finite and positive");
        }
        const double ratio = horizon_ / width_;
        const double nearest = std::round(ratio);
        const bool aligned = std::abs(ratio - nearest) <=
            1e-12 * std::max(1.0, std::abs(ratio));
        const std::size_t count = static_cast<std::size_t>(
            aligned ? nearest : std::floor(ratio)
        );
        if (count == 0) throw std::runtime_error("bucket_width exceeds horizon");
        bucket_horizon_ = static_cast<double>(count) * width_;
        areas_.assign(count, 0.0);
    }

    void transition(double timestamp, int new_value) {
        if (timestamp < last_time_) {
            throw std::runtime_error("bucket state moved backwards");
        }
        const double interval_end = std::min(timestamp, bucket_horizon_);
        double current = std::min(last_time_, bucket_horizon_);
        while (current < interval_end) {
            std::size_t index = std::min(
                static_cast<std::size_t>(current / width_),
                areas_.size() - 1
            );
            double bucket_end = static_cast<double>(index + 1) * width_;
            // A decimal width such as 0.09 is not exactly representable.
            // At an exact mathematical boundary current / width can therefore
            // round just below the next integer.  That selects the preceding
            // bucket whose computed end equals current, so the loop makes no
            // progress.  Move to the next bucket explicitly in that case.
            if (bucket_end <= current && index + 1 < areas_.size()) {
                ++index;
                bucket_end = static_cast<double>(index + 1) * width_;
            }
            const double segment_end = std::min(interval_end, bucket_end);
            if (segment_end <= current) {
                throw std::runtime_error("bucket integration failed to advance");
            }
            areas_[index] += static_cast<double>(value_) * (segment_end - current);
            current = segment_end;
        }
        last_time_ = timestamp;
        value_ = new_value;
    }

    void finish() { transition(horizon_, value_); }

    std::vector<double> averages() const {
        std::vector<double> result = areas_;
        for (double& value : result) value /= width_;
        return result;
    }

  private:
    double width_{};
    double horizon_{};
    double bucket_horizon_{};
    double last_time_{};
    int value_{};
    std::vector<double> areas_;
};

struct BucketOutput {
    std::vector<double> active;
    std::vector<std::vector<double>> queues;
    std::vector<std::vector<double>> busy;
};

class StateRecorder {
  public:
    StateRecorder(
        const std::vector<int>& capacities,
        bool capture_history,
        double bucket_width,
        double bucket_horizon
    ) : capacities_(capacities), capture_history_(capture_history),
        queues_(capacities.size()), busy_(capacities.size()),
        queue_areas_(capacities.size()), busy_areas_(capacities.size()),
        last_queue_updates_(capacities.size()), last_busy_updates_(capacities.size()) {
        if (capture_history_) {
            active_history_.push_back({0.0, 0});
            queue_history_.resize(capacities.size());
            busy_history_.resize(capacities.size());
            for (std::size_t i = 0; i < capacities.size(); ++i) {
                queue_history_[i].push_back({0.0, 0});
                busy_history_[i].push_back({0.0, 0});
            }
        }
        if (bucket_width > 0.0) {
            active_buckets_.emplace(bucket_width, bucket_horizon);
            for (std::size_t i = 0; i < capacities.size(); ++i) {
                queue_buckets_.emplace_back(bucket_width, bucket_horizon);
                busy_buckets_.emplace_back(bucket_width, bucket_horizon);
            }
        }
    }

    void external_arrival(double now) {
        ++active_;
        if (active_buckets_) active_buckets_->transition(now, active_);
        if (capture_history_) record_change(active_history_, now, active_);
    }

    void final_exit(double now) {
        if (active_ <= 0) throw std::runtime_error("active_requests cannot become negative");
        --active_;
        if (active_buckets_) active_buckets_->transition(now, active_);
        if (capture_history_) record_change(active_history_, now, active_);
    }

    void service_state(int service, double now, int queued, int occupied) {
        if (queued < 0 || occupied < 0 || occupied > capacities_[service]) {
            throw std::runtime_error("invalid queue or busy state");
        }
        update_area(service, now);
        if (active_buckets_) {
            queue_buckets_[service].transition(now, queued);
            busy_buckets_[service].transition(now, occupied);
        }
        queues_[service] = queued;
        busy_[service] = occupied;
        if (capture_history_) {
            record_change(queue_history_[service], now, queued);
            record_change(busy_history_[service], now, occupied);
        }
    }

    void finish(double now) {
        for (std::size_t service = 0; service < capacities_.size(); ++service) {
            update_area(static_cast<int>(service), now);
        }
        if (active_buckets_) {
            active_buckets_->finish();
            for (std::size_t service = 0; service < capacities_.size(); ++service) {
                queue_buckets_[service].finish();
                busy_buckets_[service].finish();
            }
        }
    }

    BucketOutput buckets() const {
        if (!active_buckets_) throw std::runtime_error("buckets were not enabled");
        BucketOutput output;
        output.active = active_buckets_->averages();
        for (const auto& series : queue_buckets_) output.queues.push_back(series.averages());
        for (const auto& series : busy_buckets_) output.busy.push_back(series.averages());
        return output;
    }

    int active() const { return active_; }
    const std::vector<int>& queues() const { return queues_; }
    const std::vector<int>& busy() const { return busy_; }
    const std::vector<double>& queue_areas() const { return queue_areas_; }
    const std::vector<double>& busy_areas() const { return busy_areas_; }
    const std::vector<Point>& active_history() const { return active_history_; }
    const std::vector<std::vector<Point>>& queue_history() const { return queue_history_; }
    const std::vector<std::vector<Point>>& busy_history() const { return busy_history_; }

  private:
    void update_area(int service, double now) {
        if (now < last_queue_updates_[service] || now < last_busy_updates_[service]) {
            throw std::runtime_error("service state time moved backwards");
        }
        queue_areas_[service] += queues_[service] * (now - last_queue_updates_[service]);
        busy_areas_[service] += busy_[service] * (now - last_busy_updates_[service]);
        last_queue_updates_[service] = now;
        last_busy_updates_[service] = now;
    }

    std::vector<int> capacities_;
    bool capture_history_{};
    int active_{};
    std::vector<int> queues_;
    std::vector<int> busy_;
    std::vector<double> queue_areas_;
    std::vector<double> busy_areas_;
    std::vector<double> last_queue_updates_;
    std::vector<double> last_busy_updates_;
    std::vector<Point> active_history_;
    std::vector<std::vector<Point>> queue_history_;
    std::vector<std::vector<Point>> busy_history_;
    std::optional<TimeWeightedBuckets> active_buckets_;
    std::vector<TimeWeightedBuckets> queue_buckets_;
    std::vector<TimeWeightedBuckets> busy_buckets_;
};

struct KernelSummary {
    std::int64_t processed_event_count{};
    std::int64_t external_arrival_count{};
    std::int64_t completed_request_count{};
    double wall_time_seconds{};
    double stop_time{};
};

double percentile(std::vector<double> values, double requested_percentile) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const double position = (static_cast<double>(values.size()) - 1.0)
        * requested_percentile / 100.0;
    const std::size_t lower = static_cast<std::size_t>(position);
    const std::size_t upper = std::min(lower + 1, values.size() - 1);
    const double weight = position - static_cast<double>(lower);
    return values[lower] * (1.0 - weight) + values[upper] * weight;
}

class Kernel {
  public:
    Kernel(
        const Workload& workload,
        std::int64_t seed,
        RunMode mode,
        double horizon,
        double warmup_duration,
        double measurement_duration,
        bool capture_history,
        bool collect_service_samples,
        bool collect_request_class_samples,
        double bucket_width = 0.0,
        double maximum_drain_time = 0.0
    ) : workload_(workload), seed_(seed), mode_(mode), horizon_(horizon),
        warmup_duration_(warmup_duration),
        measurement_end_(warmup_duration + measurement_duration),
        maximum_drain_time_(maximum_drain_time),
        collect_service_samples_(collect_service_samples),
        collect_request_class_samples_(collect_request_class_samples),
        random_(static_cast<std::uint64_t>(seed)), waiting_(workload.services.size()),
        admission_waiting_(workload.services.size()),
        busy_(workload.services.size()),
        busy_by_queue_(workload.services.size()),
        active_concurrency_(workload.services.size()),
        active_concurrency_by_queue_(workload.services.size()),
        next_round_robin_replica_(workload.services.size()),
        processor_sharing_active_(workload.services.size()),
        processor_sharing_last_update_(workload.services.size()),
        processor_sharing_virtual_work_(workload.services.size()),
        processor_sharing_generation_(workload.services.size()),
        cpu_budget_(workload.services.size()),
        cpu_next_period_(workload.services.size()),
        startup_waiting_(workload.services.size()),
        instance_state_(workload.services.size()),
        instance_generation_(workload.services.size()),
        tokens_(workload.services.size()),
        token_updated_(workload.services.size()),
        token_wake_scheduled_(workload.services.size()),
        caches_(workload.services.size()), cache_access_positions_(workload.services.size()),
        cache_incarnations_(workload.services.size()),
        trace_states_(workload.external_streams.size()),
        recorder_(capacities(workload), capture_history, bucket_width, horizon),
        service_response_times_(workload.services.size()),
        request_class_response_times_(workload.request_classes.size()),
        request_class_arrival_times_(workload.request_classes.size()),
        request_class_failure_flags_(workload.request_classes.size()),
        measured_request_counts_(workload.request_classes.size()) {
        for (std::size_t service = 0; service < workload.services.size(); ++service) {
            const std::size_t queues = queue_count(static_cast<int>(service));
            waiting_[service].resize(queues);
            admission_waiting_[service].resize(queues);
            busy_by_queue_[service].assign(queues, 0);
            active_concurrency_by_queue_[service].assign(queues, 0);
            processor_sharing_active_[service].resize(queues);
            processor_sharing_last_update_[service].assign(queues, 0.0);
            processor_sharing_virtual_work_[service].assign(queues, 0.0);
            processor_sharing_generation_[service].assign(queues, 0);
            // Service demand is normalized by quota/period, so one period
            // replenishes 'period' normalized CPU-seconds per replica.
            cpu_budget_[service].assign(queues, workload.services[service].cpu_period);
            cpu_next_period_[service].assign(queues, workload.services[service].cpu_period);
            startup_waiting_[service].resize(queues);
            const auto& spec = workload.services[service];
            instance_state_[service].assign(queues, spec.initially_warm ? 2 : 0);
            instance_generation_[service].assign(queues, 0);
            tokens_[service].assign(queues, spec.token_burst);
            token_updated_[service].assign(queues, 0.0);
            token_wake_scheduled_[service].assign(queues, false);
            caches_[service].resize(queues);
            cache_incarnations_[service].assign(queues, 0);
            cache_access_positions_[service].assign(workload.request_classes.size(), 0);
        }
        initialize_input_uncertainty();
    }

    KernelSummary run() {
        const auto start = std::chrono::steady_clock::now();
        for (std::size_t s = 0; s < workload_.services.size(); ++s) {
            if (!workload_.services[s].startup_delay.has_value()) continue;
            for (std::size_t q = 0; q < instance_state_[s].size(); ++q)
                maybe_idle_instance(static_cast<int>(s), static_cast<int>(q), 0.0);
        }
        for (std::size_t stream = 0; stream < workload_.external_streams.size(); ++stream) {
            schedule_next_external(static_cast<int>(stream), 0.0);
        }

        double stop_time = mode_ == RunMode::FixedHorizon ? horizon_ : measurement_end_;
        while (!future_events_.empty()) {
            const Event event = future_events_.top();
            if (mode_ == RunMode::FixedHorizon && event.time >= horizon_) break;
            if (
                mode_ == RunMode::MeasurementDrain
                && tagged_requests_in_flight_ > 0
                && event.time > measurement_end_ + maximum_drain_time_
            ) {
                throw std::runtime_error("eligible requests exceeded maximum drain time");
            }
            if (
                mode_ == RunMode::MeasurementDrain
                && event.time >= measurement_end_
                && tagged_requests_in_flight_ == 0
            ) {
                stop_time = measurement_end_;
                break;
            }
            future_events_.pop();
            dispatch(event);
            if (
                mode_ == RunMode::MeasurementDrain
                && event.time >= measurement_end_
                && tagged_requests_in_flight_ == 0
            ) {
                stop_time = event.time;
                break;
            }
        }
        recorder_.finish(stop_time);
        const double wall = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - start
        ).count();
        return {
            processed_event_count_, external_arrival_count_,
            completed_request_count_, wall, stop_time
        };
    }

    BucketOutput buckets() const { return recorder_.buckets(); }
    const StateRecorder& recorder() const { return recorder_; }
    const std::vector<std::vector<double>>& service_response_times() const {
        return service_response_times_;
    }
    const std::vector<std::vector<double>>& request_class_response_times() const {
        return request_class_response_times_;
    }
    const std::vector<std::vector<double>>& request_class_arrival_times() const {
        return request_class_arrival_times_;
    }
    std::vector<std::vector<double>> take_request_class_response_times() {
        return std::move(request_class_response_times_);
    }
    std::vector<std::vector<double>> take_request_class_arrival_times() {
        return std::move(request_class_arrival_times_);
    }
    std::vector<std::vector<int>> take_request_class_failure_flags() {
        return std::move(request_class_failure_flags_);
    }
    const std::vector<std::int64_t>& measured_request_counts() const {
        return measured_request_counts_;
    }
    std::int64_t tagged_requests_in_flight() const {
        return tagged_requests_in_flight_;
    }
    std::int64_t seed() const { return seed_; }
    int sampled_input_block_start() const { return sampled_input_block_start_; }
    const std::vector<double>& input_arrival_rate_multipliers() const {
        return input_arrival_rate_multipliers_;
    }
    const std::vector<double>& input_service_time_multipliers() const {
        return input_service_time_multipliers_;
    }

  private:
    void initialize_input_uncertainty() {
        input_arrival_rate_multipliers_.assign(
            workload_.request_classes.size(), 1.0
        );
        input_service_time_multipliers_.assign(
            workload_.services.size(), 1.0
        );
        if (workload_.input_epochs.empty()) return;
        std::fill(
            input_arrival_rate_multipliers_.begin(),
            input_arrival_rate_multipliers_.end(),
            0.0
        );
        std::fill(
            input_service_time_multipliers_.begin(),
            input_service_time_multipliers_.end(),
            0.0
        );
        std::uniform_int_distribution<std::size_t> choose(
            0, workload_.input_epochs.size() - 1
        );
        const std::size_t start = choose(random_);
        sampled_input_block_start_ = static_cast<int>(start);
        const double divisor = static_cast<double>(workload_.input_block_length);
        for (std::size_t offset = 0; offset < workload_.input_block_length; ++offset) {
            const InputEpoch& epoch = workload_.input_epochs[
                (start + offset) % workload_.input_epochs.size()
            ];
            for (std::size_t index = 0;
                 index < input_arrival_rate_multipliers_.size(); ++index) {
                input_arrival_rate_multipliers_[index] +=
                    epoch.arrival_rate_multipliers[index] / divisor;
            }
            for (std::size_t index = 0;
                 index < input_service_time_multipliers_.size(); ++index) {
                input_service_time_multipliers_[index] +=
                    epoch.service_time_multipliers[index] / divisor;
            }
        }
    }

    static std::vector<int> capacities(const Workload& workload) {
        std::vector<int> result;
        for (const Service& service : workload.services) result.push_back(service.capacity);
        return result;
    }

    void schedule(
        double time,
        int priority,
        EventKind kind,
        Request* request,
        int index,
        Visit* visit = nullptr,
        std::uint64_t generation = 0,
        int queue = -1,
        bool failed = false
    ) {
        future_events_.push({
            time,
            priority,
            sequence_++,
            kind,
            request,
            visit,
            index,
            queue,
            generation,
            visit == nullptr ? 0 : visit->ownership_index,
            visit == nullptr ? 0 : visit->serial,
            failed
        });
    }

    double exponential(double mean) {
        std::uniform_real_distribution<double> uniform(0.0, 1.0);
        const double draw = uniform(random_);
        return -std::log(1.0 - draw) * mean;
    }

    static double custom_uniform(void* state) {
        auto* random = static_cast<std::mt19937_64*>(state);
        std::uniform_real_distribution<double> uniform(0.0, 1.0);
        return uniform(*random);
    }

    double sample(
        const Distribution& distribution,
        TraceSamplerState* trace_state = nullptr
    ) {
        double result = 0.0;
        switch (distribution.kind) {
            case DistributionKind::Exponential:
                result = exponential(distribution.mean);
                break;
            case DistributionKind::Deterministic:
                result = distribution.mean;
                break;
            case DistributionKind::Gamma: {
                const double shape = 1.0 / distribution.scv;
                std::gamma_distribution<double> gamma(
                    shape,
                    distribution.mean / shape
                );
                result = gamma(random_);
                break;
            }
            case DistributionKind::Lognormal: {
                if (distribution.scv == 0.0) {
                    result = distribution.mean;
                    break;
                }
                const double sigma_squared = std::log1p(distribution.scv);
                std::lognormal_distribution<double> lognormal(
                    std::log(distribution.mean) - 0.5 * sigma_squared,
                    std::sqrt(sigma_squared)
                );
                result = lognormal(random_);
                break;
            }
            case DistributionKind::Weibull: {
                const double scale = distribution.mean
                    / std::tgamma(1.0 + 1.0 / distribution.shape);
                std::weibull_distribution<double> weibull(
                    distribution.shape,
                    scale
                );
                result = weibull(random_);
                break;
            }
            case DistributionKind::Empirical: {
                if (!distribution.sample_clusters.empty()) {
                    auto found = cluster_empirical_samples_.find(&distribution);
                    if (found == cluster_empirical_samples_.end()) {
                        std::uniform_int_distribution<std::size_t> choose_cluster(
                            0,
                            distribution.sample_clusters.size() - 1
                        );
                        std::vector<double> resampled;
                        for (std::size_t index = 0;
                             index < distribution.sample_clusters.size();
                             ++index) {
                            const auto& cluster = distribution.sample_clusters[
                                choose_cluster(random_)
                            ];
                            resampled.insert(
                                resampled.end(),
                                cluster.begin(),
                                cluster.end()
                            );
                        }
                        found = cluster_empirical_samples_.emplace(
                            &distribution,
                            std::move(resampled)
                        ).first;
                    }
                    std::uniform_int_distribution<std::size_t> choose(
                        0,
                        found->second.size() - 1
                    );
                    result = found->second[choose(random_)];
                } else if (distribution.bayesian_bootstrap) {
                    auto found = bayesian_empirical_samplers_.find(&distribution);
                    if (found == bayesian_empirical_samplers_.end()) {
                        std::exponential_distribution<double> weight(1.0);
                        std::vector<double> weights;
                        weights.reserve(distribution.samples.size());
                        for (std::size_t index = 0;
                             index < distribution.samples.size();
                             ++index) {
                            weights.push_back(weight(random_));
                        }
                        found = bayesian_empirical_samplers_.emplace(
                            &distribution,
                            std::discrete_distribution<std::size_t>(
                                weights.begin(),
                                weights.end()
                            )
                        ).first;
                    }
                    result = distribution.samples[found->second(random_)];
                } else {
                    std::uniform_int_distribution<std::size_t> choose(
                        0,
                        distribution.samples.size() - 1
                    );
                    result = distribution.samples[choose(random_)];
                }
                break;
            }
            case DistributionKind::ArrivalTrace: {
                if (trace_state == nullptr) {
                    throw std::runtime_error(
                        "arrival_trace requires per-stream sampler state"
                    );
                }
                if (trace_state->remaining == 0) {
                    std::uniform_int_distribution<std::size_t> choose(
                        0,
                        distribution.samples.size() - 1
                    );
                    trace_state->next_index = choose(random_);
                    // A fixed number of gaps per circular block keeps every
                    // source gap equally likely. Stopping a block by elapsed
                    // time instead overweights long gaps and changes the load.
                    // block_duration specifies the target expected duration.
                    trace_state->remaining = static_cast<std::size_t>(std::min(
                        static_cast<double>(distribution.samples.size()),
                        std::max(1.0, std::ceil(
                            distribution.block_duration / distribution.mean
                        ))
                    ));
                }
                result = distribution.samples[trace_state->next_index];
                trace_state->next_index =
                    (trace_state->next_index + 1) % distribution.samples.size();
                --trace_state->remaining;
                break;
            }
            case DistributionKind::Custom:
                if (distribution.custom_sampler == nullptr) {
                    throw std::runtime_error("custom distribution has no sampler");
                }
                result = distribution.mean * distribution.custom_sampler(
                    custom_uniform,
                    &random_,
                    distribution.custom_parameters.data(),
                    distribution.custom_parameters.size()
                );
                break;
        }
        if (!std::isfinite(result)
            || result < 0.0
            || (result == 0.0
                && distribution.kind != DistributionKind::ArrivalTrace)) {
            throw std::runtime_error(
                "a distribution generated an invalid sample"
            );
        }
        return result;
    }

    void schedule_next_external(int stream, double now) {
        const int request_class = workload_.external_streams[stream].request_class;
        schedule(
            now + sample(
                workload_.external_streams[stream].interarrival,
                &trace_states_[stream]
            ) / input_arrival_rate_multipliers_[request_class],
            kNormalPriority,
            EventKind::ExternalArrival,
            nullptr,
            stream
        );
    }

    std::int64_t sample_workflow_visits(double mean_visits) {
        const double integral = std::floor(mean_visits);
        std::int64_t count = static_cast<std::int64_t>(integral);
        const double fractional = mean_visits - integral;
        if (fractional > 0.0) {
            std::bernoulli_distribution extra(fractional);
            if (extra(random_)) ++count;
        }
        return count;
    }

    int start_workflow(Request* request) {
        const auto& workflow =
            workload_.request_classes[request->request_class].workflow;
        if (workflow.empty()) return -1;
        request->workflow_index = 0;
        request->workflow_visits_remaining = sample_workflow_visits(
            workflow.front().mean_visits
        );
        if (request->workflow_visits_remaining <= 0) {
            throw std::runtime_error(
                "the first workflow stage generated zero visits"
            );
        }
        return workflow.front().service;
    }

    int advance_workflow(Request* request) {
        const auto& workflow =
            workload_.request_classes[request->request_class].workflow;
        if (workflow.empty()) return -2;
        if (request->workflow_visits_remaining <= 0
            || request->workflow_index >= workflow.size()) {
            throw std::runtime_error("invalid request workflow state");
        }
        --request->workflow_visits_remaining;
        if (request->workflow_visits_remaining > 0) {
            return workflow[request->workflow_index].service;
        }
        while (++request->workflow_index < workflow.size()) {
            request->workflow_visits_remaining = sample_workflow_visits(
                workflow[request->workflow_index].mean_visits
            );
            if (request->workflow_visits_remaining > 0) {
                return workflow[request->workflow_index].service;
            }
        }
        return -1;
    }

    int choose_route(Request* request, int service) {
        const int workflow_target = advance_workflow(request);
        if (workflow_target != -2) return workflow_target;
        const auto& routes = workload_.request_classes[request->request_class].routes[service];
        if (routes.empty()) {
            throw std::runtime_error("a reached service has no route or exit probability");
        }
        std::uniform_real_distribution<double> uniform(0.0, 1.0);
        const double draw = uniform(random_);
        double cumulative = 0.0;
        for (const Route& route : routes) {
            cumulative += route.probability;
            if (draw < cumulative) return route.target;
        }
        return routes.back().target;
    }

    void dispatch(const Event& event) {
        switch (event.kind) {
            case EventKind::ExternalArrival: external_arrival(event.index, event.time); break;
            case EventKind::Admit: admit(event.visit, event.time); break;
            case EventKind::ServiceCompletion:
                service_completion(event); break;
            case EventKind::BlockingCompletion:
                blocking_completion(event); break;
            case EventKind::CpuBandwidthBoundary:
                cpu_bandwidth_boundary(event); break;
            case EventKind::ServiceResponse:
                if (workload_.request_classes[event.visit->request->request_class].flow_kind
                    == FlowKind::Calls) {
                    complete_call_visit(event.visit, event.time, true);
                } else {
                    complete_routed_visit(event.visit, event.time, true);
                }
                break;
            case EventKind::Grant:
                grant(event.index, event.queue, event.time);
                break;
            case EventKind::Resume: resume(event.visit, event.time); break;
            case EventKind::ConcurrencyGrant:
                concurrency_grant(event.index, event.queue, event.time);
                break;
            case EventKind::CallReturn:
                complete_child_return(event.visit, event.time, event.failed, event.generation); break;
            case EventKind::StageReady:
                complete_service_visit(event.visit, event.time); break;
            case EventKind::StageContinuationReturn:
                if (!event.visit->continuation_pending) {
                    throw std::runtime_error("unexpected staged continuation return");
                }
                event.visit->continuation_pending = false;
                event.visit->failed = event.visit->failed || event.failed;
                complete_stage_response(event.visit, event.time);
                break;
            case EventKind::FinalExit: final_exit(event.request, event.time); break;
            case EventKind::RequestTimeout:
                request_timeout(event.request, event.time); break;
            case EventKind::AdmissionTimeout: admission_timeout(event); break;
            case EventKind::RateWake:
                token_wake_scheduled_[event.index][event.queue] = false;
                concurrency_grant(event.index, event.queue, event.time); break;
            case EventKind::InstanceReady: instance_ready(event); break;
            case EventKind::InstanceIdle: instance_idle(event); break;
            case EventKind::RetryReady: retry_ready(event); break;
            case EventKind::AttemptDeadline: attempt_deadline(event); break;
        }
    }

    void external_arrival(int stream_id, double now) {
        const ExternalStream& stream = workload_.external_streams[stream_id];
        const bool tagged = mode_ == RunMode::MeasurementDrain
            && now >= warmup_duration_ && now < measurement_end_;
        const bool eligible = mode_ == RunMode::FixedHorizon
            ? now >= warmup_duration_
            : tagged;
        std::size_t ownership_index;
        if (free_request_slots_.empty()) {
            ownership_index = requests_.size();
            requests_.push_back(nullptr);
        } else {
            ownership_index = free_request_slots_.back();
            free_request_slots_.pop_back();
            if (requests_[ownership_index]) {
                throw std::runtime_error("request ownership slot was not empty");
            }
        }
        requests_[ownership_index] = std::make_unique<Request>();
        Request* request = requests_[ownership_index].get();
        request->id = next_request_id_++;
        request->request_class = stream.request_class;
        request->external_entry_time = now;
        request->ownership_index = ownership_index;
        request->latency_eligible = eligible;
        request->measurement_tagged = tagged;
        request->cache_keys.assign(workload_.services.size(), -1);
        for (std::size_t s = 0; s < workload_.services.size(); ++s) {
            const auto& cache = workload_.services[s].cache;
            if (!cache.has_value() || cache->fixed_ratio || cache->accesses[stream.request_class].empty()) continue;
            const auto& keys = cache->accesses[stream.request_class];
            const std::size_t position = cache->cyclic[stream.request_class]
                ? cache_access_positions_[s][stream.request_class]++ % keys.size()
                : std::uniform_int_distribution<std::size_t>(0, keys.size()-1)(random_);
            request->cache_keys[s] = keys[position];
        }
        const double timeout = workload_.request_classes[
            request->request_class
        ].end_to_end_timeout;
        if (timeout > 0.0) {
            schedule(
                now + timeout,
                kDeadlinePriority,
                EventKind::RequestTimeout,
                request,
                -1
            );
        }
        ++processed_event_count_;
        ++external_arrival_count_;
        if (tagged) {
            ++tagged_requests_in_flight_;
            ++measured_request_counts_[stream.request_class];
        }
        recorder_.external_arrival(now);
        const auto& workflow = workload_.request_classes[
            request->request_class
        ].workflow;
        const int entry_service = workflow.empty()
            ? stream.entry_service
            : start_workflow(request);
        if (entry_service != stream.entry_service) {
            throw std::runtime_error(
                "workflow entry service does not match the external stream"
            );
        }
        const RequestClass& request_class = workload_.request_classes[request->request_class];
        Visit* root = request_class.flow_kind == FlowKind::Stages
            ? create_stage_visit(request, request_class.stage_entries[stream.entry_service], nullptr, now)
            : create_visit(request, entry_service, nullptr, now);
        schedule(
            now,
            kUrgentPriority,
            EventKind::Admit,
            nullptr,
            entry_service,
            root
        );
        schedule_next_external(stream_id, now);
    }

    Visit* create_visit(
        Request* request,
        int service,
        Visit* parent,
        double now
    ) {
        if (++request->generated_visits > 10'000) {
            throw std::runtime_error(
                "request exceeded the 10000-visit nontermination guard"
            );
        }
        std::size_t ownership_index;
        if (free_visit_slots_.empty()) {
            ownership_index = visits_.size();
            visits_.push_back(nullptr);
        } else {
            ownership_index = free_visit_slots_.back();
            free_visit_slots_.pop_back();
            if (visits_[ownership_index]) {
                throw std::runtime_error("visit ownership slot was not empty");
            }
        }
        const int replica = service < 0 ? -1 : choose_replica(service, request);
        visits_[ownership_index] = std::make_unique<Visit>(Visit{
            request,
            service,
            replica,
            parent,
            ownership_index,
            0,
            now,
            {},
            0,
            0.0,
            0.0
        });
        ++request->live_visits;
        visits_[ownership_index]->serial = ++next_visit_serial_;
        return visits_[ownership_index].get();
    }

    bool has_independent_queues(int service) const {
        return workload_.services[service].replica_queue_model
            != ReplicaQueueModel::Pooled;
    }

    std::size_t queue_count(int service) const {
        return has_independent_queues(service)
            ? static_cast<std::size_t>(workload_.services[service].replicas)
            : 1U;
    }

    int queue_index(const Visit* visit) const {
        return has_independent_queues(visit->service) ? visit->replica : 0;
    }

    int choose_replica(int service, const Request* request) {
        const Service& specification = workload_.services[service];
        if (specification.replica_queue_model == ReplicaQueueModel::IndependentKeyHash) {
            const int key = request->cache_keys[service];
            if (key < 0 || !specification.cache.has_value()) throw std::runtime_error("cache key profile is missing");
            std::uint64_t hash = 14695981039346656037ULL;
            for (unsigned char c : specification.cache->keys[key]) { hash ^= c; hash *= 1099511628211ULL; }
            return static_cast<int>(hash % static_cast<std::uint64_t>(specification.replicas));
        }
        if (specification.replica_queue_model == ReplicaQueueModel::Pooled) {
            return -1;
        }
        if (
            specification.replica_queue_model
                == ReplicaQueueModel::IndependentRoundRobin
        ) {
            const int replica = next_round_robin_replica_[service];
            next_round_robin_replica_[service] =
                (replica + 1) % specification.replicas;
            return replica;
        }
        std::uniform_int_distribution<int> choose(0, specification.replicas - 1);
        return choose(random_);
    }

    double network_delay() {
        if (!workload_.same_node_one_way_latency.has_value()) return 0.0;
        std::bernoulli_distribution cross_node(
            workload_.cross_node_probability
        );
        const bool cross = cross_node(random_);
        return sample(
            cross
                ? workload_.cross_node_one_way_latency.value()
                : workload_.same_node_one_way_latency.value()
        );
    }

    const Distribution& service_time(Request* request, int service) const {
        const auto& distributions = workload_.services[service].service_times;
        if (request->request_class < 0
            || static_cast<std::size_t>(request->request_class)
                >= distributions.size()) {
            throw std::runtime_error("missing request-class service distribution");
        }
        return distributions[request->request_class];
    }

    double sample_service_time(Visit* visit) {
        if (visit->stage >= 0) {
            const Stage& stage = visit_stage(visit);
            return sample(stage.service_time.value()) * stage.work_units
                * input_service_time_multipliers_[visit->service];
        }
        return sample(service_time(visit->request, visit->service))
            * input_service_time_multipliers_[visit->service];
    }

    std::optional<double> sample_blocking_time(Visit* visit) {
        if (visit->stage >= 0) {
            const auto& distribution = visit_stage(visit).blocking_time;
            return distribution.has_value() ? std::optional<double>(sample(distribution.value())) : std::nullopt;
        }
        const auto& distributions = workload_.services[
            visit->service
        ].blocking_times;
        const int request_class = visit->request->request_class;
        if (
            request_class < 0
            || static_cast<std::size_t>(request_class) >= distributions.size()
        ) {
            throw std::runtime_error(
                "missing request-class blocking-time distribution"
            );
        }
        if (!distributions[request_class].has_value()) return std::nullopt;
        return sample(distributions[request_class].value());
    }

    int total_waiting(int service) const {
        std::size_t total = 0;
        for (const auto& queue : admission_waiting_[service]) total += queue.size();
        for (const auto& queue : waiting_[service]) total += queue.size();
        for (const auto& queue : startup_waiting_[service]) total += queue.size();
        return static_cast<int>(total);
    }

    int processing_capacity(int service, int queue) const {
        (void) queue;
        const Service& specification = workload_.services[service];
        return has_independent_queues(service)
            ? specification.capacity / specification.replicas
            : specification.capacity;
    }

    int concurrency_capacity(int service, int queue) const {
        (void) queue;
        const Service& specification = workload_.services[service];
        if (specification.concurrency_capacity == 0) return 0;
        return has_independent_queues(service)
            ? specification.concurrency_capacity / specification.replicas
            : specification.concurrency_capacity;
    }

    void record_service_state(int service, double now) {
        int backlog = total_waiting(service);
        if (
            workload_.services[service].execution_model
                == ExecutionModel::ProcessorSharing
        ) {
            // Every admitted visit receives CPU progress. For the fundamental
            // N=Q+B state identity, visits beyond the replicas are reported as
            // processor-sharing backlog even though they are not FIFO-blocked.
            for (std::size_t queue = 0;
                 queue < processor_sharing_active_[service].size(); ++queue) {
                backlog += std::max(
                    0,
                    static_cast<int>(
                        processor_sharing_active_[service][queue].size()
                    ) - busy_by_queue_[service][queue]
                );
            }
        }
        recorder_.service_state(service, now, backlog, busy_[service]);
    }

    void queue_for_processing(Visit* visit, double now) {
        const int service = visit->service;
        const int instance = queue_index(visit);
        const auto& spec = workload_.services[service];
        if (spec.startup_delay.has_value()) {
            if (instance_state_[service][instance] != 2) {
                startup_waiting_[service][instance].push_back(visit);
                if (instance_state_[service][instance] == 0) {
                    instance_state_[service][instance] = 1;
                    schedule(now + sample(spec.startup_delay.value()), kNormalPriority,
                        EventKind::InstanceReady, nullptr, service, nullptr,
                        ++instance_generation_[service][instance], instance);
                }
                record_service_state(service, now);
                return;
            }
            ++instance_generation_[service][instance];
        }
        if (spec.execution_model == ExecutionModel::Delay) {
            schedule(now + sample_service_time(visit), kNormalPriority,
                EventKind::ServiceCompletion, nullptr, service, visit);
            return;
        }
        if (
            workload_.services[service].execution_model
                == ExecutionModel::ProcessorSharing
        ) {
            start_processor_sharing(visit, now);
            return;
        }
        const int queue = queue_index(visit);
        const int capacity = processing_capacity(service, queue);
        if (busy_by_queue_[service][queue] < capacity
            && waiting_[service][queue].empty()) {
            ++busy_by_queue_[service][queue];
            ++busy_[service];
            if (busy_[service] > workload_.services[service].capacity) {
                throw std::runtime_error("busy processing slots exceed capacity");
            }
            record_service_state(service, now);
            schedule(
                now + sample_service_time(visit),
                kNormalPriority,
                EventKind::ServiceCompletion,
                nullptr,
                service,
                visit
            );
        } else {
            waiting_[service][queue].push_back(visit);
            record_service_state(service, now);
        }
    }

    double processor_sharing_rate(int service, int queue) const {
        const std::size_t active = processor_sharing_active_[service][queue].size();
        if (active == 0) return 0.0;
        const Service& specification = workload_.services[service];
        if (specification.cpu_period > 0.0) {
            if (cpu_budget_[service][queue] <= 1e-12) return 0.0;
            return std::min(
                1.0,
                static_cast<double>(specification.cpu_parallelism)
                    / static_cast<double>(active)
            ) * specification.cpu_period / specification.cpu_quota;
        }
        const double processors = static_cast<double>(
            processing_capacity(service, queue)
        );
        return std::min(1.0, processors / static_cast<double>(active));
    }

    void update_processor_sharing_work(int service, int queue, double now) {
        const double previous = processor_sharing_last_update_[service][queue];
        if (now < previous) {
            throw std::runtime_error("processor-sharing time moved backwards");
        }
        const double elapsed = now - previous;
        const Service& specification = workload_.services[service];
        if (specification.cpu_period > 0.0) {
            const std::size_t active = processor_sharing_active_[service][queue].size();
            const double per_job_rate = active == 0 ? 0.0 : std::min(
                1.0, static_cast<double>(specification.cpu_parallelism) / active
            ) * specification.cpu_period / specification.cpu_quota;
            auto consume = [&](double duration) {
                if (active == 0 || duration <= 0.0) return;
                const double progress = std::min(
                    per_job_rate * duration,
                    cpu_budget_[service][queue] / static_cast<double>(active)
                );
                processor_sharing_virtual_work_[service][queue] += progress;
                cpu_budget_[service][queue] = std::max(
                    0.0, cpu_budget_[service][queue] - progress * active
                );
            };
            double& boundary = cpu_next_period_[service][queue];
            consume(std::min(now, boundary) - previous);
            if (now >= boundary) {
                // An idle queue may skip many periods; active queues schedule
                // each boundary, so they cannot silently lose CPU progress.
                const double periods = std::floor((now - boundary) / specification.cpu_period);
                boundary += (periods + 1.0) * specification.cpu_period;
                if (boundary <= now) boundary += specification.cpu_period;
                const double start = boundary - specification.cpu_period;
                cpu_budget_[service][queue] = specification.cpu_period;
                consume(now - start);
            }
            processor_sharing_last_update_[service][queue] = now;
            return;
        }
        if (
            elapsed > 0.0
            && !processor_sharing_active_[service][queue].empty()
        ) {
            processor_sharing_virtual_work_[service][queue] +=
                processor_sharing_rate(service, queue) * elapsed;
        }
        processor_sharing_last_update_[service][queue] = now;
    }

    void schedule_processor_sharing_completion(
        int service,
        int queue,
        double now
    ) {
        const std::uint64_t generation =
            ++processor_sharing_generation_[service][queue];
        auto& active = processor_sharing_active_[service][queue];
        if (active.empty()) return;
        const ProcessorSharingEntry& next = active.top();
        const double rate = processor_sharing_rate(service, queue);
        const Service& specification = workload_.services[service];
        if (specification.cpu_period > 0.0) {
            if (next.finish_tag - processor_sharing_virtual_work_[service][queue] <= 1e-12) {
                // A completion at exact budget exhaustion must not wait for
                // another period because subtraction left a rounding remainder.
                schedule(now, kNormalPriority, EventKind::ServiceCompletion,
                    nullptr, service, next.visit, generation, queue);
                return;
            }
            double boundary_delay = cpu_next_period_[service][queue] - now;
            if (!(boundary_delay > 0.0)) {
                throw std::runtime_error("CPU quota period did not advance");
            }
            if (rate > 0.0) {
                boundary_delay = std::min(
                    boundary_delay,
                    cpu_budget_[service][queue] / (rate * active.size())
                );
                const double completion_delay = std::max(
                    0.0, next.finish_tag - processor_sharing_virtual_work_[service][queue]
                ) / rate;
                if (completion_delay <= boundary_delay) {
                    schedule(now + completion_delay, kNormalPriority,
                        EventKind::ServiceCompletion, nullptr, service,
                        next.visit, generation, queue);
                    return;
                }
            }
            schedule(now + boundary_delay, kNormalPriority,
                EventKind::CpuBandwidthBoundary, nullptr, service,
                nullptr, generation, queue);
            return;
        }
        if (rate <= 0.0) {
            throw std::runtime_error("processor-sharing service has no capacity");
        }
        schedule(
            now + std::max(
                0.0,
                next.finish_tag
                    - processor_sharing_virtual_work_[service][queue]
            ) / rate,
            kNormalPriority,
            EventKind::ServiceCompletion,
            nullptr,
            service,
            next.visit,
            generation,
            queue
        );
    }

    void start_processor_sharing(Visit* visit, double now) {
        const int service = visit->service;
        const int queue = queue_index(visit);
        update_processor_sharing_work(service, queue, now);
        visit->remaining_work = processor_sharing_virtual_work_[service][queue]
            + sample_service_time(visit);
        processor_sharing_active_[service][queue].push({
            visit->remaining_work,
            visit->request->id,
            visit
        });
        const int previous_busy = busy_by_queue_[service][queue];
        busy_by_queue_[service][queue] = std::min(
            static_cast<int>(processor_sharing_active_[service][queue].size()),
            processing_capacity(service, queue)
        );
        busy_[service] += busy_by_queue_[service][queue] - previous_busy;
        record_service_state(service, now);
        schedule_processor_sharing_completion(service, queue, now);
    }

    void cpu_bandwidth_boundary(const Event& event) {
        if (event.generation != processor_sharing_generation_[event.index][event.queue]) return;
        const bool refill = event.time >= cpu_next_period_[event.index][event.queue];
        update_processor_sharing_work(event.index, event.queue, event.time);
        // Exhaustion may occur less than one representable clock tick away.
        // Clear the rounding remainder rather than rescheduling at the same time.
        if (!refill) cpu_budget_[event.index][event.queue] = 0.0;
        schedule_processor_sharing_completion(event.index, event.queue, event.time);
    }

    void admit(Visit* visit, double now) {
        if (visit->stage >= 0 && visit->service < 0) {
            const auto& delay = visit_stage(visit).delay;
            schedule(now + (delay.has_value() ? sample(delay.value()) : 0.0),
                     kNormalPriority, EventKind::StageReady, nullptr, -1, visit);
            return;
        }
        const int service = visit->service;
        const int queue = queue_index(visit);
        const int limit = concurrency_capacity(service, queue);
        if (
            (limit == 0 || active_concurrency_by_queue_[service][queue] < limit)
            && admission_waiting_[service][queue].empty()
            && acquire_token(service, queue, now)
        ) {
            ++active_concurrency_by_queue_[service][queue];
            ++active_concurrency_[service];
            visit->admitted = true;
            if (
                limit > 0
                && active_concurrency_by_queue_[service][queue] > limit
            ) {
                throw std::runtime_error("active service concurrency exceeds capacity");
            }
            queue_for_processing(visit, now);
        } else {
            const auto& spec = workload_.services[service];
            if ((spec.token_reject && !token_available(service, queue, now))
                || (spec.pending_capacity >= 0
                    && admission_waiting_[service][queue].size() >= static_cast<std::size_t>(spec.pending_capacity)
                        * (has_independent_queues(service) ? 1u : static_cast<std::size_t>(spec.replicas)))) {
                fail_visit(visit, now);
                return;
            }
            admission_waiting_[service][queue].push_back(visit);
            if (spec.pending_timeout > 0.0) {
                schedule(now + spec.pending_timeout, kDeadlinePriority,
                    EventKind::AdmissionTimeout, nullptr, service, visit,
                    ++visit->wait_generation, queue);
            }
            record_service_state(service, now);
            schedule_token_wake(service, queue, now);
        }
    }

    bool token_available(int service, int queue, double now) {
        const auto& spec = workload_.services[service];
        if (spec.token_rate == 0.0) return true;
        tokens_[service][queue] = std::min(static_cast<double>(spec.token_burst),
            tokens_[service][queue] + spec.token_rate * (now - token_updated_[service][queue]));
        token_updated_[service][queue] = now;
        return tokens_[service][queue] >= 1.0 - 1e-12;
    }

    bool acquire_token(int service, int queue, double now) {
        if (!token_available(service, queue, now)) return false;
        if (workload_.services[service].token_rate > 0.0)
            tokens_[service][queue] = std::max(0.0, tokens_[service][queue] - 1.0);
        return true;
    }

    void schedule_token_wake(int service, int queue, double now) {
        const auto& spec = workload_.services[service];
        if (spec.token_rate == 0.0 || spec.token_reject
            || admission_waiting_[service][queue].empty()
            || token_wake_scheduled_[service][queue]
            || token_available(service, queue, now)) return;
        const double when = std::max(std::nextafter(now, INFINITY),
            now + (1.0 - tokens_[service][queue]) / spec.token_rate);
        token_wake_scheduled_[service][queue] = true;
        schedule(when, kNormalPriority, EventKind::RateWake,
            nullptr, service, nullptr, 0, queue);
    }

    void admission_timeout(const Event& event) {
        if (event.visit_slot >= visits_.size()) return;
        Visit* visit = visits_[event.visit_slot].get();
        if (visit == nullptr || visit->serial != event.visit_serial
            || visit->wait_generation != event.generation || visit->admitted) return;
        auto& queue = admission_waiting_[event.index][event.queue];
        auto position = std::find(queue.begin(), queue.end(), visit);
        if (position == queue.end()) return;
        queue.erase(position);
        ++visit->wait_generation;
        record_service_state(event.index, event.time);
        fail_visit(visit, event.time);
    }

    void instance_ready(const Event& event) {
        if (event.generation != instance_generation_[event.index][event.queue]) return;
        instance_state_[event.index][event.queue] = 2;
        auto& queue = startup_waiting_[event.index][event.queue];
        while (!queue.empty()) {
            Visit* visit = queue.front(); queue.pop_front();
            queue_for_processing(visit, event.time);
        }
        maybe_idle_instance(event.index, event.queue, event.time);
    }

    void maybe_idle_instance(int service, int queue, double now) {
        const auto& spec = workload_.services[service];
        if (!spec.startup_delay.has_value() || instance_state_[service][queue] != 2
            || active_concurrency_by_queue_[service][queue] != 0
            || !admission_waiting_[service][queue].empty()) return;
        schedule(now + spec.idle_timeout, kNormalPriority, EventKind::InstanceIdle,
            nullptr, service, nullptr, ++instance_generation_[service][queue], queue);
    }

    void instance_idle(const Event& event) {
        if (event.generation == instance_generation_[event.index][event.queue]
            && active_concurrency_by_queue_[event.index][event.queue] == 0
            && admission_waiting_[event.index][event.queue].empty()) {
            instance_state_[event.index][event.queue] = 0;
            if (workload_.services[event.index].cache.has_value())
                caches_[event.index][event.queue] = CacheState{};
            ++cache_incarnations_[event.index][event.queue];
        }
    }

    void fail_visit(Visit* visit, double now) {
        visit->failed = true;
        if (visit->stage >= 0) {
            visit->stage_phase_done = true;
            release_stage_resource(visit, now);
            complete_stage_response(visit, now);
        } else if (workload_.request_classes[visit->request->request_class].flow_kind == FlowKind::Calls) {
            complete_call_visit(visit, now);
        } else {
            Request* request = visit->request;
            request->failed = true;
            release_concurrency_and_destroy(visit, now);
            finish_or_delay(request, now);
        }
    }

    void finish_or_delay(Request* request, double now) {
        const auto& request_class = workload_.request_classes[
            request->request_class
        ];
        if (request_class.all_work_completion && request->live_visits > 0) {
            request->deferred_response = true;
            return;
        }
        if (request_class.all_work_completion) request->failed = request->failed || request->work_failed;
        double overhead = 0.0;
        for (const Distribution& component : request_class.latency_components) {
            overhead += sample(component);
        }
        if (request_class.latency_overhead.has_value()) {
            overhead += sample(request_class.latency_overhead.value());
        }
        if (overhead > 0.0) {
            schedule(
                now + overhead,
                kNormalPriority,
                EventKind::FinalExit,
                request,
                -1
            );
        } else {
            final_exit(request, now);
        }
    }

    void release_processing(Visit* visit, double now) {
        const int service = visit->service;
        const int queue = queue_index(visit);
        --busy_by_queue_[service][queue];
        if (busy_by_queue_[service][queue] < 0) {
            throw std::runtime_error("replica busy slots became negative");
        }
        --busy_[service];
        if (busy_[service] < 0) {
            throw std::runtime_error("busy processing slots became negative");
        }
        record_service_state(service, now);
        schedule(
            now,
            kNormalPriority,
            EventKind::Grant,
            nullptr,
            service,
            nullptr,
            0,
            queue
        );
    }

    void release_concurrency(Visit* visit, double now) {
        if (visit->resource_released) return;
        visit->resource_released = true;
        if (visit->service < 0) return;
        if (!visit->admitted) return;
        visit->admitted = false;
        const int service = visit->service;
        const int queue = queue_index(visit);
        --active_concurrency_by_queue_[service][queue];
        if (active_concurrency_by_queue_[service][queue] < 0) {
            throw std::runtime_error("replica active concurrency became negative");
        }
        --active_concurrency_[service];
        if (active_concurrency_[service] < 0) {
            throw std::runtime_error("active service concurrency became negative");
        }
        record_service_state(service, now);
        schedule(
            now,
            kNormalPriority,
            EventKind::ConcurrencyGrant,
            nullptr,
            service,
            nullptr,
            0,
            queue
        );
        maybe_idle_instance(service, queue, now);
    }

    void record_visit_response(Visit* visit, double now) {
        if (visit->service >= 0 && collect_service_samples_ && !visit->response_sample_recorded) {
            service_response_times_[visit->service].push_back(now - visit->arrival_time);
        }
        visit->response_sample_recorded = true;
    }

    void destroy_visit(Visit* visit, double now) {
        record_visit_response(visit, now);
        const std::size_t ownership_index = visit->ownership_index;
        Request* request = visit->request;
        visits_[ownership_index].reset();
        free_visit_slots_.push_back(ownership_index);
        if (--request->live_visits < 0) {
            throw std::runtime_error("request live visit count became negative");
        }
        maybe_release_request(request, now);
    }

    void release_concurrency_and_destroy(Visit* visit, double now) {
        release_concurrency(visit, now);
        destroy_visit(visit, now);
    }

    bool defer_service_response(Visit* visit, double now) {
        const auto& distribution = workload_.services[visit->service].post_release_delay;
        if (!distribution.has_value()) return false;
        const double delay = sample(distribution.value());
        if (delay == 0.0) return false;
        // Keep the Visit and Request alive until the response is delivered.
        // A caller retains its own slot; this service no longer retains one.
        release_concurrency(visit, now);
        schedule(now + delay, kNormalPriority, EventKind::ServiceResponse,
                 nullptr, visit->service, visit);
        return true;
    }

    const Stage& visit_stage(const Visit* visit) const {
        const auto& stages = workload_.request_classes[visit->request->request_class].stages;
        if (visit->stage < 0 || static_cast<std::size_t>(visit->stage) >= stages.size()) {
            throw std::runtime_error("visit references an invalid execution stage");
        }
        return stages[visit->stage];
    }

    Visit* create_stage_visit(Request* request, int stage, Visit* parent, double now) {
        const auto& stages = workload_.request_classes[request->request_class].stages;
        if (stage < 0 || static_cast<std::size_t>(stage) >= stages.size()) {
            throw std::runtime_error("invalid execution stage index");
        }
        Visit* result = create_visit(request, stages[stage].resource, parent, now);
        result->stage = stage;
        return result;
    }

    void schedule_stage_child(Visit* parent, int stage, double now, bool continuation = false,
                              std::uint64_t attempt = 0) {
        const double transit = continuation ? 0.0 : network_delay();
        Visit* child = create_stage_visit(parent->request, stage, parent, now + transit);
        child->is_continuation = continuation;
        child->child_attempt = attempt;
        child->return_network_delay = transit;
        if (continuation) parent->continuation_pending = true;
        else ++parent->pending_children;
        schedule(now + transit, kUrgentPriority, EventKind::Admit,
                 nullptr, child->service, child);
    }

    void release_stage_resource(Visit* visit, double now) {
        if (visit->resource_released) return;
        // Resource response is queue residence/holding, not the entire workflow.
        record_visit_response(visit, now);
        release_concurrency(visit, now);
    }

    void complete_stage_response(Visit* visit, double now) {
        if (visit->response_completed || visit->continuation_pending) {
            throw std::runtime_error("invalid staged response boundary");
        }
        visit->completion_started = true;
        visit->response_completed = true;
        Visit* parent = visit->parent;
        Request* request = visit->request;
        const bool continuation = visit->is_continuation;
        const bool failed = visit->failed;
        const auto attempt = visit->child_attempt;
        const double return_time = now + visit->return_network_delay;
        if (visit->pending_children == 0) destroy_visit(visit, now);
        if (parent == nullptr) {
            request->failed = request->failed || failed;
            finish_or_delay(request, now);
        } else {
            // A return event keeps the parent alive through its own pending
            // child/continuation counter, including late quorum responses.
            schedule(return_time, kUrgentPriority,
                     continuation ? EventKind::StageContinuationReturn : EventKind::CallReturn,
                     nullptr, parent->service, parent, attempt, -1, failed);
        }
    }

    void complete_stage_phase(Visit* visit, double now) {
        if (visit->stage_phase_done) return;
        visit->stage_phase_done = true;
        if (visit->cache_missed && !visit->failed && visit_stage(visit).cache_fill)
            cache_insert(visit, now);
        release_stage_resource(visit, now);
        if (visit->failed) {
            complete_stage_response(visit, now);
            return;
        }
        const int next = visit_stage(visit).next;
        if (next >= 0) schedule_stage_child(visit, next, now, true);
        else complete_stage_response(visit, now);
    }

    void cache_remove(int service, int queue, int key) {
        auto& state = caches_[service][queue];
        const auto found = state.entries.find(key);
        if (found == state.entries.end()) return;
        state.bytes -= workload_.services[service].cache->sizes[key];
        state.lru.erase(found->second.position);
        state.entries.erase(found);
    }

    void expire_cache(int service, int queue, double now) {
        auto& state = caches_[service][queue];
        while (!state.expiry.empty() && state.expiry.top().first <= now) {
            const int key = state.expiry.top().second;
            state.expiry.pop();
            const auto entry = state.entries.find(key);
            if (entry != state.entries.end() && entry->second.expires <= now)
                cache_remove(service, queue, key);
        }
    }

    void cache_insert(Visit* visit, double now) {
        const int service = visit->service, queue = queue_index(visit);
        const auto& spec = workload_.services[service].cache.value();
        if (spec.fixed_ratio) return;
        if (visit->cache_incarnation != cache_incarnations_[service][queue]) return;
        const int key = visit->request->cache_keys[service];
        auto& state = caches_[service][queue];
        expire_cache(service, queue, now);
        cache_remove(service, queue, key);
        if (spec.sizes[key] > spec.capacity) return;
        while (state.bytes + spec.sizes[key] > spec.capacity)
            cache_remove(service, queue, state.lru.back());
        state.lru.push_front(key);
        const double expiry = spec.ttl > 0.0 ? now + spec.ttl : INFINITY;
        state.entries.emplace(key, CacheState::Entry{state.lru.begin(), expiry});
        state.bytes += spec.sizes[key];
        if (spec.ttl > 0.0) state.expiry.push({expiry, key});
        if (state.expiry.size() > std::max<std::size_t>(1024, 4*state.entries.size())) {
            decltype(state.expiry) fresh;
            for (const auto& entry : state.entries) fresh.push({entry.second.expires,entry.first});
            state.expiry = std::move(fresh);
        }
    }

    void start_cache_operation(Visit* visit, double now) {
        const Stage& stage = visit_stage(visit);
        const int service = visit->service, queue = queue_index(visit);
        const auto& spec = workload_.services[service].cache.value();
        const int key = visit->request->cache_keys[service];
        visit->cache_incarnation = cache_incarnations_[service][queue];
        if (stage.cache_action == 1) {
            cache_insert(visit, now); complete_stage_phase(visit, now); return;
        }
        if (stage.cache_action == 2) {
            cache_remove(service, queue, key); complete_stage_phase(visit, now); return;
        }
        bool hit;
        if (spec.fixed_ratio) {
            const double ratio = spec.hit_ratios[visit->request->request_class];
            if (ratio < 0.0) throw std::runtime_error("cache hit ratio is missing for request class");
            hit = std::bernoulli_distribution(ratio)(random_);
        } else {
            expire_cache(service, queue, now);
            auto& state = caches_[service][queue];
            const auto entry = state.entries.find(key);
            hit = entry != state.entries.end();
            if (hit) state.lru.splice(state.lru.begin(), state.lru, entry->second.position);
        }
        visit->cache_missed = !hit;
        const int child = hit ? stage.cache_hit : stage.cache_miss;
        if (child >= 0) schedule_stage_child(visit, child, now);
        else complete_stage_phase(visit, now);
    }

    Visit* live_visit(const Event& event) {
        if (event.visit_slot >= visits_.size()) return nullptr;
        Visit* visit = visits_[event.visit_slot].get();
        return visit != nullptr && visit->serial == event.visit_serial ? visit : nullptr;
    }

    void start_retry_attempt(Visit* parent, double now) {
        const Stage& stage = visit_stage(parent);
        ++parent->attempts_started;
        parent->attempt_active = true;
        ++parent->retry_generation;
        schedule_stage_child(parent, stage.calls.front().service, now, false, parent->retry_generation);
        if (stage.attempt_timeout > 0.0)
            schedule(now + stage.attempt_timeout, kDeadlinePriority, EventKind::AttemptDeadline,
                nullptr, -1, parent, parent->retry_generation);
    }

    void retry_or_finish(Visit* parent, double now) {
        const Stage& stage = visit_stage(parent);
        if (parent->attempts_started >= stage.retry_max) {
            parent->failed = true;
            complete_stage_phase(parent, now);
        } else {
            const double delay = stage.retry_backoff.has_value() ? sample(stage.retry_backoff.value()) : 0.0;
            schedule(now + delay, kNormalPriority, EventKind::RetryReady,
                nullptr, -1, parent, parent->retry_generation);
        }
    }

    void retry_result(Visit* parent, double now, bool failed, std::uint64_t attempt) {
        if (parent->stage_phase_done) {
            if (parent->response_completed && parent->pending_children == 0
                && !parent->continuation_pending) destroy_visit(parent, now);
            return;
        }
        if (attempt != parent->retry_generation || !parent->attempt_active) return;
        parent->attempt_active = false;
        if (failed) retry_or_finish(parent, now);
        else complete_stage_phase(parent, now);
    }

    void retry_ready(const Event& event) {
        Visit* parent = live_visit(event);
        if (parent != nullptr && !parent->stage_phase_done && !parent->attempt_active
            && parent->retry_generation == event.generation) start_retry_attempt(parent, event.time);
    }

    void attempt_deadline(const Event& event) {
        Visit* parent = live_visit(event);
        if (parent == nullptr || parent->stage_phase_done || !parent->attempt_active
            || parent->retry_generation != event.generation) return;
        // Keep the old child in pending_children: timeout does not cancel its work.
        parent->attempt_active = false;
        retry_or_finish(parent, event.time);
    }

    void start_stage_children(Visit* visit, double now) {
        const Stage& stage = visit_stage(visit);
        if (stage.cache_operation) {
            if (!stage.hold_resource) release_stage_resource(visit, now);
            start_cache_operation(visit, now);
            return;
        }
        if (stage.retry_max > 0) {
            start_retry_attempt(visit, now);
            return;
        }
        if (!stage.hold_resource || stage.policy.completion == CallCompletion::Background) {
            release_stage_resource(visit, now);
        }
        if (stage.policy.execution == CallExecution::Choice && !stage.calls.empty()) {
            std::vector<double> weights;
            for (const auto& child : stage.calls) weights.push_back(child.mean_visits);
            std::discrete_distribution<std::size_t> choose(weights.begin(),weights.end());
            schedule_stage_child(visit,stage.calls[choose(random_)].service,now);
            return;
        }
        for (const auto& child : stage.calls) {
            const auto count = sample_workflow_visits(child.mean_visits);
            for (std::int64_t i = 0; i < count; ++i) {
                if (stage.policy.execution == CallExecution::Sequential) {
                    visit->sequential_children.push_back(child.service);
                } else schedule_stage_child(visit,child.service,now);
            }
        }
        if (stage.policy.execution == CallExecution::Sequential
            && start_next_sequential_child(visit,now)) return;
        if (visit->pending_children == 0 || stage.policy.completion == CallCompletion::Background) {
            complete_stage_phase(visit,now);
        }
    }

    void schedule_child(Visit* parent, int child_service, double now) {
        Visit* child_visit = create_visit(
            parent->request,
            child_service,
            parent,
            now
        );
        ++parent->pending_children;
        const double outbound_delay = network_delay();
        child_visit->return_network_delay = outbound_delay;
        const double arrival_time = now + outbound_delay;
        child_visit->arrival_time = arrival_time;
        schedule(
            arrival_time,
            kUrgentPriority,
            EventKind::Admit,
            nullptr,
            child_service,
            child_visit
        );
    }

    bool start_next_sequential_child(Visit* parent, double now) {
        if (parent->next_sequential_child >= parent->sequential_children.size()) {
            return false;
        }
        const int child_service = parent->sequential_children[
            parent->next_sequential_child++
        ];
        if (parent->stage >= 0) schedule_stage_child(parent, child_service, now);
        else schedule_child(parent, child_service, now);
        return true;
    }

    void complete_child_return(Visit* parent, double now, bool failed = false,
                               std::uint64_t attempt = 0) {
        if (parent == nullptr || parent->pending_children <= 0) {
            throw std::runtime_error("call-graph child count became negative");
        }
        --parent->pending_children;
        ++parent->returned_children;
        parent->failed_children += failed ? 1 : 0;
        if (parent->stage >= 0 && visit_stage(parent).retry_max > 0) {
            retry_result(parent, now, failed, attempt);
            return;
        }
        if (failed && (parent->completion_started || parent->stage_phase_done))
            parent->request->work_failed = true;
        if (parent->stage >= 0 && parent->stage_phase_done) {
            if (parent->response_completed && parent->pending_children == 0
                && !parent->continuation_pending) destroy_visit(parent,now);
            return;
        }
        // A quorum/background response can precede the remaining work. Keep
        // the parent alive until every child return event has been consumed.
        if (parent->completion_started) {
            if (parent->response_completed && parent->pending_children == 0) {
                destroy_visit(parent, now);
            }
            return;
        }
        const RequestClass& request_class = workload_.request_classes[
            parent->request->request_class
        ];
        const CallPolicy& policy = parent->stage >= 0
            ? visit_stage(parent).policy : request_class.call_policies[parent->service];
        if (failed && policy.completion != CallCompletion::Quorum) parent->failed = true;
        if (
            policy.execution == CallExecution::Sequential
            && !parent->failed
            && start_next_sequential_child(parent, now)
        ) {
            return;
        }
        if (parent->pending_children == 0
            || (policy.completion == CallCompletion::Quorum
                && parent->returned_children - parent->failed_children >= policy.required)) {
            if (policy.completion == CallCompletion::Quorum
                && parent->returned_children - parent->failed_children < policy.required)
                parent->failed = true;
            if (parent->stage >= 0) complete_stage_phase(parent, now);
            else complete_call_visit(parent, now);
        }
    }

    void complete_call_visit(Visit* visit, double now, bool released = false) {
        if (!released) {
            if (visit->completion_started) return;
            visit->completion_started = true;
            if (defer_service_response(visit, now)) return;
        }
        visit->response_completed = true;
        record_visit_response(visit, now);
        Request* request = visit->request;
        Visit* parent = visit->parent;
        const double return_network_delay = visit->return_network_delay;
        const bool failed = visit->failed;
        if (!released) release_concurrency(visit, now);
        if (visit->pending_children == 0) destroy_visit(visit, now);
        if (parent != nullptr) {
            const double return_time = now + return_network_delay;
            if (return_time == now) {
                complete_child_return(parent, now, failed);
            } else {
                schedule(
                    return_time,
                    kUrgentPriority,
                    EventKind::CallReturn,
                    nullptr,
                    parent->service,
                    parent, 0, -1, failed
                );
            }
        } else {
            request->failed = request->failed || failed;
            finish_or_delay(request, now);
        }
    }

    bool release_processor_sharing(
        Visit* visit,
        double now,
        std::uint64_t generation,
        int queue
    ) {
        const int service = visit->service;
        if (generation != processor_sharing_generation_[service][queue]) {
            return false;
        }
        update_processor_sharing_work(service, queue, now);
        auto& active = processor_sharing_active_[service][queue];
        if (active.empty() || active.top().visit != visit) return false;
        if (
            visit->remaining_work
                - processor_sharing_virtual_work_[service][queue]
                > 1e-9
        ) {
            throw std::runtime_error(
                "processor-sharing completion fired before work was exhausted"
            );
        }
        active.pop();
        const int previous_busy = busy_by_queue_[service][queue];
        busy_by_queue_[service][queue] = std::min(
            static_cast<int>(active.size()),
            processing_capacity(service, queue)
        );
        busy_[service] += busy_by_queue_[service][queue] - previous_busy;
        record_service_state(service, now);
        schedule_processor_sharing_completion(service, queue, now);
        return true;
    }

    void complete_service_visit(Visit* visit, double now) {
        const int service = visit->service;
        ++processed_event_count_;
        ++completed_visit_count_;
        if (visit->stage >= 0) {
            start_stage_children(visit,now);
            return;
        }
        Request* request = visit->request;
        const RequestClass& request_class = workload_.request_classes[
            request->request_class
        ];
        if (request_class.flow_kind == FlowKind::Calls) {
            const CallPolicy& policy = request_class.call_policies[service];
            const auto& children = request_class.calls[service];
            if (policy.execution == CallExecution::Choice && !children.empty()) {
                std::vector<double> probabilities;
                for (const WorkflowVisit& child : children) {
                    probabilities.push_back(child.mean_visits);
                }
                std::discrete_distribution<std::size_t> choose(
                    probabilities.begin(), probabilities.end()
                );
                schedule_child(visit, children[choose(random_)].service, now);
                return;
            }
            for (const WorkflowVisit& child : children) {
                const std::int64_t count = sample_workflow_visits(
                    child.mean_visits
                );
                for (std::int64_t occurrence = 0; occurrence < count; ++occurrence) {
                    if (policy.execution != CallExecution::Sequential) {
                        schedule_child(visit, child.service, now);
                    } else {
                        visit->sequential_children.push_back(child.service);
                    }
                }
            }
            if (
                policy.execution == CallExecution::Sequential
                && start_next_sequential_child(visit, now)
            ) {
                return;
            }
            if (visit->pending_children == 0
                || policy.completion == CallCompletion::Background) {
                complete_call_visit(visit, now);
            }
            return;
        }

        if (defer_service_response(visit, now)) return;
        complete_routed_visit(visit, now);
    }

    void complete_routed_visit(Visit* visit, double now, bool released = false) {
        Request* request = visit->request;
        const int target = choose_route(request, visit->service);
        if (target >= 0) {
            Visit* next = create_visit(request, target, nullptr, now);
            const double arrival_time = now + network_delay();
            next->arrival_time = arrival_time;
            schedule(
                arrival_time,
                kUrgentPriority,
                EventKind::Admit,
                nullptr,
                target,
                next
            );
        }
        if (released) destroy_visit(visit, now);
        else release_concurrency_and_destroy(visit, now);
        if (target < 0) {
            finish_or_delay(request, now);
        }
    }

    void service_completion(const Event& event) {
        const double now = event.time;
        const int service = event.index;
        if (
            workload_.services[service].execution_model
                == ExecutionModel::ProcessorSharing
        ) {
            // Superseded processor-sharing completions may refer to a visit
            // which has already completed and been destroyed. Check the
            // service generation before dereferencing that pointer.
            if (
                event.queue < 0
                || event.generation
                    != processor_sharing_generation_[service][event.queue]
            ) return;
        }
        Visit* visit = event.visit;
        if (visit == nullptr || visit->service != service) {
            throw std::runtime_error("service completion refers to an invalid visit");
        }
        if (
            workload_.services[service].execution_model
                == ExecutionModel::ProcessorSharing
        ) {
            if (!release_processor_sharing(
                visit,
                now,
                event.generation,
                event.queue
            )) return;
        } else if (workload_.services[service].execution_model != ExecutionModel::Delay) {
            release_processing(visit, now);
        }
        const double probability = workload_.services[service].failure_probability;
        if (probability > 0.0 && std::bernoulli_distribution(probability)(random_)) {
            fail_visit(visit, now);
            return;
        }
        const std::optional<double> blocking_time = sample_blocking_time(visit);
        if (blocking_time.has_value()) {
            schedule(
                now + blocking_time.value(),
                kNormalPriority,
                EventKind::BlockingCompletion,
                nullptr,
                service,
                visit
            );
            return;
        }
        complete_service_visit(visit, now);
    }

    void blocking_completion(const Event& event) {
        Visit* visit = event.visit;
        if (visit == nullptr || visit->service != event.index) {
            throw std::runtime_error(
                "blocking completion refers to an invalid visit"
            );
        }
        complete_service_visit(visit, event.time);
    }

    void grant(int service, int queue, double now) {
        const int capacity = processing_capacity(service, queue);
        if (!waiting_[service][queue].empty()
            && busy_by_queue_[service][queue] < capacity) {
            Visit* visit = waiting_[service][queue].front();
            waiting_[service][queue].pop_front();
            ++busy_by_queue_[service][queue];
            ++busy_[service];
            if (busy_[service] > workload_.services[service].capacity) {
                throw std::runtime_error("busy processing slots exceed capacity");
            }
            schedule(
                now,
                kNormalPriority,
                EventKind::Resume,
                nullptr,
                service,
                visit
            );
        }
    }

    void concurrency_grant(int service, int queue, double now) {
        const int limit = concurrency_capacity(service, queue);
        while (
            !admission_waiting_[service][queue].empty()
            && (
                limit == 0
                || active_concurrency_by_queue_[service][queue] < limit
            )
        ) {
            if (!acquire_token(service, queue, now)) {
                if (workload_.services[service].token_reject) {
                    Visit* rejected = admission_waiting_[service][queue].front();
                    admission_waiting_[service][queue].pop_front();
                    ++rejected->wait_generation;
                    fail_visit(rejected, now);
                    continue;
                }
                schedule_token_wake(service, queue, now);
                return;
            }
            Visit* visit = admission_waiting_[service][queue].front();
            admission_waiting_[service][queue].pop_front();
            ++visit->wait_generation;
            visit->admitted = true;
            ++active_concurrency_by_queue_[service][queue];
            ++active_concurrency_[service];
            if (
                limit > 0
                && active_concurrency_by_queue_[service][queue] > limit
            ) {
                throw std::runtime_error("active service concurrency exceeds capacity");
            }
            record_service_state(service, now);
            queue_for_processing(visit, now);
        }
    }

    void resume(Visit* visit, double now) {
        const int service = visit->service;
        if (
            workload_.services[service].execution_model
                == ExecutionModel::ProcessorSharing
        ) {
            throw std::runtime_error(
                "processor-sharing visits must not enter the fixed-slot queue"
            );
        }
        record_service_state(service, now);
        schedule(
            now + sample_service_time(visit),
            kNormalPriority,
            EventKind::ServiceCompletion,
            nullptr,
            service,
            visit
        );
    }

    void record_request_outcome(Request* request, double now, bool failed) {
        if (request->outcome_recorded) return;
        request->outcome_recorded = true;
        const double latency = now - request->external_entry_time;
        if (request->latency_eligible && collect_request_class_samples_) {
            request_class_arrival_times_[request->request_class].push_back(
                request->external_entry_time
            );
            request_class_response_times_[request->request_class].push_back(latency);
            request_class_failure_flags_[request->request_class].push_back(
                failed ? 1 : 0
            );
        }
        if (request->measurement_tagged) {
            if (tagged_requests_in_flight_ <= 0) {
                throw std::runtime_error("tagged request count became negative");
            }
            --tagged_requests_in_flight_;
        }
    }

    void release_request(Request* request) {
        const std::size_t ownership_index = request->ownership_index;
        requests_[ownership_index].reset();
        free_request_slots_.push_back(ownership_index);
    }

    void maybe_release_request(Request* request, double now) {
        if (request->deferred_response && request->live_visits == 0) {
            request->deferred_response = false;
            finish_or_delay(request, now);
            return;
        }
        const double timeout = workload_.request_classes[
            request->request_class
        ].end_to_end_timeout;
        if (request->server_completed && request->live_visits == 0
            && (timeout == 0.0 || request->deadline_processed)) {
            release_request(request);
        }
    }

    void request_timeout(Request* request, double now) {
        ++processed_event_count_;
        request->deadline_processed = true;
        if (!request->server_completed) {
            record_request_outcome(request, now, true);
            return;
        }
        maybe_release_request(request, now);
    }

    void final_exit(Request* request, double now) {
        ++completed_request_count_;
        request->server_completed = true;
        record_request_outcome(request, now, request->failed);
        recorder_.final_exit(now);
        maybe_release_request(request, now);
    }

    const Workload& workload_;
    std::int64_t seed_{};
    RunMode mode_{};
    double horizon_{};
    double warmup_duration_{};
    double measurement_end_{};
    double maximum_drain_time_{};
    bool collect_service_samples_{};
    bool collect_request_class_samples_{};
    std::mt19937_64 random_;
    std::unordered_map<
        const Distribution*,
        std::discrete_distribution<std::size_t>
    > bayesian_empirical_samplers_;
    std::unordered_map<const Distribution*, std::vector<double>>
        cluster_empirical_samples_;
    std::priority_queue<Event, std::vector<Event>, LaterEvent> future_events_;
    std::uint64_t sequence_{};
    std::vector<std::vector<std::deque<Visit*>>> waiting_;
    std::vector<std::vector<std::deque<Visit*>>> admission_waiting_;
    std::vector<int> busy_;
    std::vector<std::vector<int>> busy_by_queue_;
    std::vector<int> active_concurrency_;
    std::vector<std::vector<int>> active_concurrency_by_queue_;
    std::vector<int> next_round_robin_replica_;
    using ProcessorSharingQueue = std::priority_queue<
        ProcessorSharingEntry,
        std::vector<ProcessorSharingEntry>,
        LaterProcessorSharingEntry
    >;
    std::vector<std::vector<ProcessorSharingQueue>> processor_sharing_active_;
    std::vector<std::vector<double>> processor_sharing_last_update_;
    std::vector<std::vector<double>> processor_sharing_virtual_work_;
    std::vector<std::vector<std::uint64_t>> processor_sharing_generation_;
    std::vector<std::vector<double>> cpu_budget_;
    std::vector<std::vector<double>> cpu_next_period_;
    std::vector<std::vector<std::deque<Visit*>>> startup_waiting_;
    std::vector<std::vector<int>> instance_state_;
    std::vector<std::vector<std::uint64_t>> instance_generation_;
    std::vector<std::vector<double>> tokens_;
    std::vector<std::vector<double>> token_updated_;
    std::vector<std::vector<bool>> token_wake_scheduled_;
    std::vector<std::vector<CacheState>> caches_;
    std::vector<std::vector<std::size_t>> cache_access_positions_;
    std::vector<std::vector<std::uint64_t>> cache_incarnations_;
    std::vector<TraceSamplerState> trace_states_;
    StateRecorder recorder_;
    std::vector<std::unique_ptr<Request>> requests_;
    std::vector<std::size_t> free_request_slots_;
    std::vector<std::unique_ptr<Visit>> visits_;
    std::vector<std::size_t> free_visit_slots_;
    std::vector<std::vector<double>> service_response_times_;
    std::vector<std::vector<double>> request_class_response_times_;
    std::vector<std::vector<double>> request_class_arrival_times_;
    std::vector<std::vector<int>> request_class_failure_flags_;
    std::vector<std::int64_t> measured_request_counts_;
    int sampled_input_block_start_{-1};
    std::vector<double> input_arrival_rate_multipliers_;
    std::vector<double> input_service_time_multipliers_;
    std::int64_t processed_event_count_{};
    std::int64_t external_arrival_count_{};
    std::int64_t completed_request_count_{};
    std::int64_t completed_visit_count_{};
    std::int64_t tagged_requests_in_flight_{};
    std::int64_t next_request_id_{};
    std::uint64_t next_visit_serial_{};
};

struct JsonOwner {
    json_t* value{};
    explicit JsonOwner(json_t* owned) : value(owned) {}
    ~JsonOwner() { if (value) json_decref(value); }
    JsonOwner(const JsonOwner&) = delete;
    JsonOwner& operator=(const JsonOwner&) = delete;
};

json_t* required(json_t* object, const char* key) {
    if (!json_is_object(object)) throw std::runtime_error("expected JSON object");
    json_t* value = json_object_get(object, key);
    if (!value) throw std::runtime_error(std::string("missing JSON field: ") + key);
    return value;
}

std::string json_text(json_t* object, const char* key) {
    const char* value = json_string_value(required(object, key));
    if (!value) throw std::runtime_error(std::string(key) + " must be a string");
    return value;
}

double json_number(json_t* object, const char* key) {
    json_t* value = required(object, key);
    if (!json_is_number(value)) throw std::runtime_error(std::string(key) + " must be numeric");
    const double result = json_number_value(value);
    if (!std::isfinite(result)) throw std::runtime_error(std::string(key) + " must be finite");
    return result;
}

std::int64_t required_integer(json_t* object, const char* key) {
    json_t* value = required(object, key);
    if (!json_is_integer(value)) throw std::runtime_error(std::string(key) + " must be an integer");
    return static_cast<std::int64_t>(json_integer_value(value));
}

bool required_boolean(json_t* object, const char* key) {
    json_t* value = required(object, key);
    if (!json_is_boolean(value)) throw std::runtime_error(std::string(key) + " must be boolean");
    return json_is_true(value);
}

template <typename Function>
Function load_custom_symbol(void* library, const std::string& symbol) {
    dlerror();
    void* raw_symbol = dlsym(library, symbol.c_str());
    const char* error = dlerror();
    if (error != nullptr || raw_symbol == nullptr) {
        throw std::runtime_error(
            "cannot load custom distribution symbol '" + symbol + "': "
            + (error == nullptr ? "symbol not found" : error)
        );
    }
    return reinterpret_cast<Function>(raw_symbol);
}

Distribution parse_distribution(json_t* raw, bool allow_arrival_trace) {
    Distribution result;
    const std::string name = json_text(raw, "distribution");
    result.mean = json_number(raw, "mean");
    result.scv = json_number(raw, "scv");
    if (result.mean <= 0.0 || result.scv < 0.0) {
        throw std::runtime_error("distribution mean and SCV are invalid");
    }

    if (name == "exponential") {
        result.kind = DistributionKind::Exponential;
    } else if (name == "deterministic") {
        result.kind = DistributionKind::Deterministic;
    } else if (name == "gamma") {
        if (result.scv <= 0.0) {
            throw std::runtime_error("gamma distribution SCV must be positive");
        }
        result.kind = DistributionKind::Gamma;
    } else if (name == "lognormal") {
        result.kind = DistributionKind::Lognormal;
    } else if (name == "weibull") {
        result.kind = DistributionKind::Weibull;
        result.shape = json_number(raw, "shape");
        if (result.shape <= 0.0) {
            throw std::runtime_error("weibull shape must be positive");
        }
    } else if (name == "empirical" || name == "empirical_interarrival") {
        result.kind = DistributionKind::Empirical;
        const std::string sampling = json_text(raw, "sampling");
        if (sampling == "bayesian_bootstrap") {
            result.bayesian_bootstrap = true;
        } else if (sampling == "cluster_bootstrap") {
            json_t* raw_clusters = required(raw, "clusters");
            if (!json_is_array(raw_clusters)
                || json_array_size(raw_clusters) < 2) {
                throw std::runtime_error(
                    "cluster bootstrap requires at least two clusters"
                );
            }
            std::size_t cluster_index;
            json_t* raw_cluster;
            json_array_foreach(
                raw_clusters,
                cluster_index,
                raw_cluster
            ) {
                if (!json_is_array(raw_cluster)
                    || json_array_size(raw_cluster) == 0) {
                    throw std::runtime_error(
                        "cluster bootstrap clusters must be nonempty arrays"
                    );
                }
                std::vector<double> cluster;
                std::size_t sample_index;
                json_t* raw_sample;
                json_array_foreach(
                    raw_cluster,
                    sample_index,
                    raw_sample
                ) {
                    if (!json_is_number(raw_sample)) {
                        throw std::runtime_error(
                            "cluster bootstrap samples must be numeric"
                        );
                    }
                    const double sample = json_number_value(raw_sample);
                    if (!std::isfinite(sample) || sample <= 0.0) {
                        throw std::runtime_error(
                            "cluster bootstrap samples must be finite and positive"
                        );
                    }
                    cluster.push_back(sample);
                }
                result.sample_clusters.push_back(std::move(cluster));
            }
        } else if (sampling != "iid_with_replacement") {
            throw std::runtime_error(
                "empirical sampling must be iid_with_replacement or "
                "bayesian_bootstrap or cluster_bootstrap"
            );
        }
    } else if (name == "arrival_trace" && allow_arrival_trace) {
        result.kind = DistributionKind::ArrivalTrace;
        if (json_text(raw, "sampling") != "block_bootstrap") {
            throw std::runtime_error(
                "arrival_trace sampling must be block_bootstrap"
            );
        }
        result.block_duration = json_number(raw, "block_duration");
        if (result.block_duration <= 0.0) {
            throw std::runtime_error("arrival_trace block_duration must be positive");
        }
    } else if (name == "custom") {
        const std::int64_t abi_version = required_integer(raw, "abi_version");
        if (abi_version != KUBESIZER_CUSTOM_DISTRIBUTION_ABI_VERSION) {
            throw std::runtime_error("unsupported custom distribution ABI version");
        }
        const std::string library_path = json_text(raw, "library");
        const std::string symbol = json_text(raw, "symbol");
        static_cast<void>(json_text(raw, "library_sha256"));
        json_t* raw_parameters = required(raw, "parameters");
        if (!json_is_array(raw_parameters)) {
            throw std::runtime_error("custom distribution parameters must be an array");
        }
        std::size_t parameter_index;
        json_t* raw_parameter;
        json_array_foreach(raw_parameters, parameter_index, raw_parameter) {
            if (!json_is_number(raw_parameter)) {
                throw std::runtime_error(
                    "custom distribution parameters must be numeric"
                );
            }
            const double parameter = json_number_value(raw_parameter);
            if (!std::isfinite(parameter)) {
                throw std::runtime_error(
                    "custom distribution parameters must be finite"
                );
            }
            result.custom_parameters.push_back(parameter);
        }
        void* raw_library = dlopen(library_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (raw_library == nullptr) {
            const char* error = dlerror();
            throw std::runtime_error(
                "cannot load custom distribution library '" + library_path
                + "': " + (error == nullptr ? "unknown error" : error)
            );
        }
        result.custom_library_handle = std::shared_ptr<void>(
            raw_library,
            [](void* library) {
                if (library != nullptr) dlclose(library);
            }
        );
        const auto plugin_abi = load_custom_symbol<
            kubesizer_custom_abi_version_fn
        >(
            raw_library,
            "kubesizer_custom_distribution_abi_version"
        );
        if (plugin_abi() != KUBESIZER_CUSTOM_DISTRIBUTION_ABI_VERSION) {
            throw std::runtime_error("custom distribution plugin ABI mismatch");
        }
        result.custom_sampler = load_custom_symbol<kubesizer_custom_sample_fn>(
            raw_library,
            symbol
        );
        result.kind = DistributionKind::Custom;
    } else {
        throw std::runtime_error("unsupported distribution: " + name);
    }

    if (result.kind == DistributionKind::Empirical
        || result.kind == DistributionKind::ArrivalTrace) {
        json_t* raw_samples = required(raw, "samples");
        if (!json_is_array(raw_samples) || json_array_size(raw_samples) == 0) {
            throw std::runtime_error("empirical distribution samples are empty");
        }
        std::size_t sample_index;
        json_t* raw_sample;
        json_array_foreach(raw_samples, sample_index, raw_sample) {
            if (!json_is_number(raw_sample)) {
                throw std::runtime_error("distribution samples must be numeric");
            }
            const double sample = json_number_value(raw_sample);
            if (!std::isfinite(sample)) {
                throw std::runtime_error("distribution samples must be finite");
            }
            result.samples.push_back(sample);
        }
        if (result.kind == DistributionKind::ArrivalTrace) {
            if (result.samples.size() < 2) {
                throw std::runtime_error(
                    "arrival_trace requires at least two timestamps"
                );
            }
            std::vector<double> intervals;
            intervals.reserve(result.samples.size() - 1);
            bool has_positive_interval = false;
            for (std::size_t i = 1; i < result.samples.size(); ++i) {
                const double interval = result.samples[i] - result.samples[i - 1];
                if (interval < 0.0) {
                    throw std::runtime_error(
                        "arrival_trace timestamps must be nondecreasing"
                    );
                }
                has_positive_interval = has_positive_interval || interval > 0.0;
                intervals.push_back(interval);
            }
            if (!has_positive_interval) {
                throw std::runtime_error(
                    "arrival_trace must span a positive amount of time"
                );
            }
            result.samples = std::move(intervals);
        } else if (std::any_of(
            result.samples.begin(),
            result.samples.end(),
            [](double value) { return value <= 0.0; }
        )) {
            throw std::runtime_error("empirical samples must be positive");
        }
    }
    return result;
}

std::vector<std::int64_t> parse_seeds(json_t* request) {
    json_t* raw = required(request, "seeds");
    if (!json_is_array(raw) || json_array_size(raw) == 0) {
        throw std::runtime_error("seeds must be a non-empty array");
    }
    std::vector<std::int64_t> seeds;
    std::size_t index;
    json_t* value;
    json_array_foreach(raw, index, value) {
        if (!json_is_integer(value) || json_integer_value(value) < 0) {
            throw std::runtime_error("seeds must contain non-negative integers");
        }
        seeds.push_back(static_cast<std::int64_t>(json_integer_value(value)));
    }
    std::vector<std::int64_t> unique = seeds;
    std::sort(unique.begin(), unique.end());
    if (std::adjacent_find(unique.begin(), unique.end()) != unique.end()) {
        throw std::runtime_error("seeds must be unique");
    }
    return seeds;
}

void parse_stage_workflow(json_t* raw, RequestClass& result, const std::vector<Service>& services, std::size_t class_id) {
    const std::size_t resource_count = services.size();
    json_t* nodes = required(raw,"stages");
    json_t* entries = required(raw,"entries");
    if (!json_is_array(nodes) || json_array_size(nodes) == 0 || json_array_size(nodes) > 256
        || !json_is_array(entries) || json_array_size(entries) != resource_count) {
        throw std::runtime_error("invalid staged workflow nodes or entry mapping");
    }
    const auto count = static_cast<std::int64_t>(json_array_size(nodes));
    std::size_t index;
    json_t* item;
    std::unordered_map<std::string,bool> names;
    json_array_foreach(nodes,index,item) {
        Stage stage;
        stage.name = json_text(item,"name");
        if (stage.name.empty() || names[stage.name]) throw std::runtime_error("stage names must be unique and nonempty");
        names[stage.name] = true;
        const auto resource = required_integer(item,"resource");
        const auto next = required_integer(item,"next");
        if (resource < -1 || resource >= static_cast<std::int64_t>(resource_count)
            || next < -1 || next >= count) throw std::runtime_error("invalid staged resource/continuation index");
        stage.resource = static_cast<int>(resource);
        stage.next = static_cast<int>(next);
        stage.work_units = json_number(item,"work_units");
        if (stage.work_units <= 0.0) throw std::runtime_error("stage work_units must be positive");
        json_t* hold = required(item,"hold_resource");
        if (!json_is_boolean(hold)) throw std::runtime_error("hold_resource must be boolean");
        stage.hold_resource = json_is_true(hold);
        json_t* work = required(item,"service_time");
        json_t* blocking = required(item,"blocking_time");
        json_t* delay = required(item,"delay");
        if (!json_is_null(work)) stage.service_time = parse_distribution(work,false);
        if (!json_is_null(blocking)) stage.blocking_time = parse_distribution(blocking,false);
        if (!json_is_null(delay)) stage.delay = parse_distribution(delay,false);
        if ((stage.resource >= 0 && (!stage.service_time.has_value() || stage.delay.has_value()))
            || (stage.resource < 0 && (stage.service_time.has_value() || stage.blocking_time.has_value()
                || stage.hold_resource || stage.work_units != 1.0))) {
            throw std::runtime_error("control stages cannot own resource work; resource stages need service_time");
        }
        if (stage.service_time.has_value()
            && (!std::isfinite(stage.service_time->mean*stage.work_units)
                || stage.service_time->mean*stage.work_units <= 0.0)) {
            throw std::runtime_error("scaled stage work is not finite");
        }
        json_t* calls = required(item,"calls");
        if (!json_is_array(calls)) throw std::runtime_error("stage calls must be an array");
        std::vector<bool> seen(static_cast<std::size_t>(count),false);
        std::size_t child_index;
        json_t* child;
        json_array_foreach(calls,child_index,child) {
            const auto target = required_integer(child,"service");
            const double n = json_number(child,"mean_visits");
            if (target < 0 || target >= count || n <= 0.0 || n > 10'000 || seen[target]) {
                throw std::runtime_error("invalid or duplicate staged child call");
            }
            seen[target] = true;
            stage.calls.push_back({static_cast<int>(target),n});
        }
        json_t* policy = required(item,"call_policy");
        const auto execution = json_text(policy,"execution");
        const auto completion = json_text(policy,"completion");
        if (execution == "parallel") stage.policy.execution = CallExecution::Parallel;
        else if (execution == "sequential") stage.policy.execution = CallExecution::Sequential;
        else if (execution == "choice") stage.policy.execution = CallExecution::Choice;
        else throw std::runtime_error("invalid staged call execution");
        if (completion == "all") stage.policy.completion = CallCompletion::All;
        else if (completion == "quorum") stage.policy.completion = CallCompletion::Quorum;
        else if (completion == "background") stage.policy.completion = CallCompletion::Background;
        else throw std::runtime_error("invalid staged completion");
        stage.policy.required = required_integer(policy,"required");
        if (stage.policy.completion == CallCompletion::Quorum) {
            if (stage.policy.execution != CallExecution::Parallel || stage.policy.required <= 0
                || stage.policy.required > static_cast<std::int64_t>(stage.calls.size())) {
                throw std::runtime_error("invalid staged quorum");
            }
            for (const auto& c : stage.calls) if (c.mean_visits != 1.0) {
                throw std::runtime_error("staged quorum requires distinct one-call members");
            }
        } else if (stage.policy.required != 0) throw std::runtime_error("required only applies to staged quorum");
        if (stage.policy.completion == CallCompletion::Background
            && stage.policy.execution != CallExecution::Parallel) throw std::runtime_error("staged background calls must be parallel");
        if (stage.policy.execution == CallExecution::Choice) {
            double total = 0.0;
            for (const auto& c : stage.calls) total += c.mean_visits;
            if (stage.policy.completion != CallCompletion::All || std::abs(total-1.0)>1e-10) {
                throw std::runtime_error("stage choice probabilities must sum to one");
            }
        }
        json_t* retry = json_object_get(item, "retry");
        if (retry != nullptr && !json_is_null(retry)) {
            if (stage.resource >= 0 || stage.calls.size() != 1 || stage.calls[0].mean_visits != 1.0
                || stage.policy.execution != CallExecution::Parallel
                || stage.policy.completion != CallCompletion::All || !json_is_object(retry))
                throw std::runtime_error("retry requires a single-child resource-free all-response stage");
            const auto maximum = required_integer(retry, "max_attempts");
            if (maximum < 1 || maximum > 100) throw std::runtime_error("invalid retry attempt bound");
            stage.retry_max = static_cast<int>(maximum);
            stage.attempt_timeout = json_number(retry, "attempt_timeout_seconds");
            if (stage.attempt_timeout < 0.0) throw std::runtime_error("invalid attempt timeout");
            json_t* backoff = required(retry, "backoff");
            if (!json_is_null(backoff)) stage.retry_backoff = parse_distribution(backoff, false);
        }
        json_t* cache = json_object_get(item, "cache");
        if (cache != nullptr && !json_is_null(cache)) {
            if (!json_is_object(cache) || stage.resource < 0 || !services[stage.resource].cache.has_value()
                || stage.retry_max > 0 || stage.policy.execution != CallExecution::Parallel
                || stage.policy.completion != CallCompletion::All)
                throw std::runtime_error("invalid cache operation stage");
            const auto& spec = services[stage.resource].cache.value();
            const auto action = json_text(cache, "operation");
            if (action == "lookup") stage.cache_action = 0;
            else if (action == "put") stage.cache_action = 1;
            else if (action == "invalidate") stage.cache_action = 2;
            else throw std::runtime_error("unknown cache operation");
            const auto hit = required_integer(cache, "hit"), miss = required_integer(cache, "miss");
            if (hit < -1 || hit >= count || miss < -1 || miss >= count
                || (stage.cache_action == 0 && (miss < 0 || hit == miss))
                || (stage.cache_action != 0 && !stage.calls.empty()))
                throw std::runtime_error("invalid cache operation branches");
            json_t* fill = required(cache, "fill_on_miss");
            if (!json_is_boolean(fill)) throw std::runtime_error("invalid cache fill rule");
            stage.cache_fill = json_is_true(fill);
            stage.cache_hit = static_cast<int>(hit); stage.cache_miss = static_cast<int>(miss);
            std::vector<int> branches;
            if (stage.cache_action == 0) {
                if (hit >= 0) branches.push_back(static_cast<int>(hit));
                branches.push_back(static_cast<int>(miss));
            } else if (hit != -1 || miss != -1) throw std::runtime_error("non-lookup cache operation has branches");
            if (stage.calls.size() != branches.size()) throw std::runtime_error("cache branches disagree with graph edges");
            for (const auto& child : stage.calls) {
                if (child.mean_visits != 1.0 || std::find(branches.begin(),branches.end(),child.service)==branches.end())
                    throw std::runtime_error("cache branches disagree with graph edges");
            }
            if ((spec.fixed_ratio && (stage.cache_action != 0 || spec.hit_ratios[class_id] < 0.0))
                || (!spec.fixed_ratio && spec.accesses[class_id].empty()))
                throw std::runtime_error("cache stage lacks class-specific evidence");
            stage.cache_operation = true;
        }
        result.stages.push_back(std::move(stage));
    }
    bool any_entry = false;
    json_array_foreach(entries,index,item) {
        if (!json_is_integer(item) || json_integer_value(item) < -1 || json_integer_value(item) >= count) {
            throw std::runtime_error("invalid staged entry index");
        }
        const int entry = static_cast<int>(json_integer_value(item));
        result.stage_entries.push_back(entry);
        any_entry = any_entry || entry >= 0;
    }
    if (!any_entry) throw std::runtime_error("stage workflow needs an external entry");
    std::vector<int> state(static_cast<std::size_t>(count),0);
    std::vector<double> maximum(static_cast<std::size_t>(count),0.0);
    std::function<double(int)> walk = [&](int node) {
        if (state[node] == 1) throw std::runtime_error("stage graph must be acyclic");
        if (state[node] == 2) return maximum[node];
        state[node] = 1;
        const Stage& stage = result.stages[node];
        double branches = 0.0;
        for (const auto& c : stage.calls) {
            const double work = std::ceil(c.mean_visits)*walk(c.service);
            branches = stage.policy.execution == CallExecution::Choice
                ? std::max(branches,work) : branches+work;
        }
        maximum[node] = 1.0+branches*std::max(1,stage.retry_max)+(stage.next >= 0 ? walk(stage.next) : 0.0);
        if (maximum[node] > 10'000) throw std::runtime_error("stage graph exceeds visit guard");
        state[node] = 2;
        return maximum[node];
    };
    for (int entry : result.stage_entries) if (entry >= 0) walk(entry);
    for (int value : state) if (value != 2) throw std::runtime_error("unreachable execution stage");
}

CacheSpec parse_cache(json_t* raw, const std::vector<std::string>& classes) {
    if (!json_is_object(raw) || json_object_size(raw) != 5)
        throw std::runtime_error("invalid cache policy fields");
    CacheSpec cache;
    cache.hit_ratios.assign(classes.size(), -1.0);
    cache.accesses.resize(classes.size()); cache.cyclic.assign(classes.size(), false);
    auto bytes = [](json_t* object, const char* key) {
        const auto n = required_integer(object,key);
        if (n < 1 || n > (1LL<<60)) throw std::runtime_error("invalid cache byte capacity");
        return static_cast<std::uint64_t>(n);
    };
    cache.capacity = bytes(raw,"capacity_bytes_per_replica");
    const auto mode = json_text(raw,"mode");
    if (mode == "fixed_hit_ratio") {
        cache.fixed_ratio = true;
        if (bytes(raw,"observed_capacity_bytes_per_replica") != cache.capacity)
            throw std::runtime_error("fixed hit ratio cannot extrapolate cache memory");
        const auto replicas = required_integer(raw,"observed_replicas");
        if (replicas < 1 || replicas > 1'000'000) throw std::runtime_error("invalid observed cache replicas");
        cache.observed_replicas = static_cast<int>(replicas);
        json_t* ratios = required(raw,"hit_ratio_by_class");
        if (!json_is_object(ratios) || json_object_size(ratios) == 0) throw std::runtime_error("missing cache ratios");
        const char* key; json_t* value;
        json_object_foreach(ratios,key,value) {
            const auto found = std::find(classes.begin(),classes.end(),key);
            const double p = json_number(ratios,key);
            if (found == classes.end() || p < 0.0 || p > 1.0) throw std::runtime_error("invalid class cache ratio");
            cache.hit_ratios[std::distance(classes.begin(),found)] = p;
        }
        return cache;
    }
    if (mode != "lru") throw std::runtime_error("unsupported cache model");
    cache.ttl = json_number(raw,"ttl_seconds");
    if (cache.ttl < 0.0) throw std::runtime_error("invalid cache TTL");
    json_t* sizes = required(raw,"key_sizes_bytes");
    json_t* profiles = required(raw,"accesses_by_class");
    if (!json_is_object(sizes) || json_object_size(sizes) == 0 || json_object_size(sizes)>1'000'000
        || !json_is_object(profiles) || json_object_size(profiles) == 0) throw std::runtime_error("invalid cache key evidence");
    const char* key; json_t* value;
    json_object_foreach(sizes,key,value) cache.keys.push_back(key);
    std::sort(cache.keys.begin(),cache.keys.end());
    std::unordered_map<std::string,int> ids;
    for (std::size_t i=0;i<cache.keys.size();++i) {
        if (cache.keys[i].empty()) throw std::runtime_error("empty cache key");
        ids[cache.keys[i]] = static_cast<int>(i);
        cache.sizes.push_back(bytes(sizes,cache.keys[i].c_str()));
    }
    json_object_foreach(profiles,key,value) {
        const auto found = std::find(classes.begin(),classes.end(),key);
        if (found == classes.end()) throw std::runtime_error("unknown cache request class");
        const std::size_t c = std::distance(classes.begin(),found);
        const auto sampling = json_text(value,"sampling");
        if (sampling != "iid" && sampling != "cyclic_trace") throw std::runtime_error("invalid cache key sampling");
        cache.cyclic[c] = sampling == "cyclic_trace";
        json_t* keys = required(value,"keys");
        if (!json_is_array(keys) || json_array_size(keys)==0 || json_array_size(keys)>1'000'000)
            throw std::runtime_error("invalid cache access sequence");
        std::size_t index; json_t* raw_key;
        json_array_foreach(keys,index,raw_key) {
            const char* name = json_string_value(raw_key);
            if (name == nullptr || !ids.count(name)) throw std::runtime_error("unknown accessed cache key");
            cache.accesses[c].push_back(ids[name]);
        }
    }
    return cache;
}

void parse_component_policies(Service& service, json_t* raw, const std::vector<std::string>& classes) {
    if (raw == nullptr) return;
    if (!json_is_object(raw)) throw std::runtime_error("component_policies must be an object");
    const char* key; json_t* value;
    json_object_foreach(raw, key, value) {
        const std::string name(key);
        if (name == "admission_queue") {
            if (!json_is_object(value) || json_object_size(value) != 2)
                throw std::runtime_error("invalid admission queue policy");
            json_t* capacity = required(value, "capacity_per_replica");
            if (!json_is_null(capacity)) {
                const auto count = required_integer(value, "capacity_per_replica");
                if (count < 0 || count > 1'000'000) throw std::runtime_error("invalid pending capacity");
                service.pending_capacity = static_cast<int>(count);
            }
            service.pending_timeout = json_number(value, "timeout_seconds");
            if (service.pending_timeout < 0.0) throw std::runtime_error("invalid pending timeout");
        } else if (name == "rate_limit") {
            if (!json_is_object(value) || json_object_size(value) != 3
                || service.replica_queue_model == ReplicaQueueModel::Pooled)
                throw std::runtime_error("rate limit requires independent replica queues");
            service.token_rate = json_number(value, "rate_per_second");
            const auto burst = required_integer(value, "burst");
            const auto action = json_text(value, "action");
            if (service.token_rate <= 0.0 || burst < 1 || burst > 1'000'000
                || (action != "wait" && action != "reject")) throw std::runtime_error("invalid token bucket");
            service.token_burst = static_cast<int>(burst);
            service.token_reject = action == "reject";
        } else if (name == "failure_probability") {
            service.failure_probability = json_number(raw, key);
            if (service.failure_probability < 0.0 || service.failure_probability > 1.0)
                throw std::runtime_error("invalid failure probability");
        } else if (name == "lifecycle") {
            if (!json_is_object(value) || json_object_size(value) != 3
                || service.replica_queue_model == ReplicaQueueModel::Pooled)
                throw std::runtime_error("instance lifecycle requires independent queues");
            service.startup_delay = parse_distribution(required(value, "startup_delay"), false);
            service.idle_timeout = json_number(value, "idle_timeout_seconds");
            json_t* warm = required(value, "initially_warm");
            if (service.idle_timeout <= 0.0 || !json_is_boolean(warm))
                throw std::runtime_error("invalid instance lifecycle");
            service.initially_warm = json_is_true(warm);
        } else if (name == "cache") {
            service.cache = parse_cache(value,classes);
            if ((service.cache->fixed_ratio && service.replicas != service.cache->observed_replicas)
                || (!service.cache->fixed_ratio && service.replica_queue_model != ReplicaQueueModel::IndependentKeyHash))
                throw std::runtime_error("cache replica/dispatch policy does not match its evidence");
        } else throw std::runtime_error("unknown component policy: " + name);
    }
    if ((service.pending_capacity >= 0 || service.pending_timeout > 0.0)
        && service.concurrency_capacity == 0 && service.token_rate == 0.0)
        throw std::runtime_error("admission policy requires concurrency or rate limit");
    if (service.cache.has_value() && service.cache->fixed_ratio && service.startup_delay.has_value())
        throw std::runtime_error("cold cache lifecycle requires explicit key state");
}

Workload parse_workload(json_t* raw) {
    Workload workload;
    workload.name = json_text(raw, "name");
    json_t* classes = required(raw, "request_classes");
    if (!json_is_array(classes) || json_array_size(classes) == 0) {
        throw std::runtime_error("workload must contain request classes");
    }
    const std::size_t request_class_count = json_array_size(classes);
    std::vector<std::string> class_names;
    for (std::size_t c=0;c<request_class_count;++c) class_names.push_back(json_text(json_array_get(classes,c),"name"));
    json_t* services = required(raw, "services");
    if (!json_is_array(services) || json_array_size(services) == 0) {
        throw std::runtime_error("workload must contain services");
    }
    std::size_t index;
    json_t* item;
    json_array_foreach(services, index, item) {
        const std::int64_t capacity = required_integer(item, "capacity");
        if (capacity <= 0 || capacity > 1'000'000) {
            throw std::runtime_error("service capacity must be positive");
        }
        Service service;
        service.name = json_text(item, "name");
        json_t* raw_execution_model = json_object_get(item, "execution_model");
        if (raw_execution_model != nullptr) {
            const char* value = json_string_value(raw_execution_model);
            if (value == nullptr) {
                throw std::runtime_error("execution_model must be a string");
            }
            const std::string model(value);
            if (model == "fixed_slots") {
                service.execution_model = ExecutionModel::FixedSlots;
            } else if (model == "processor_sharing") {
                service.execution_model = ExecutionModel::ProcessorSharing;
            } else if (model == "delay") {
                service.execution_model = ExecutionModel::Delay;
            } else {
                throw std::runtime_error(
                    "unsupported execution_model: " + model
                );
            }
        }
        json_t* raw_replica_queue_model = json_object_get(
            item,
            "replica_queue_model"
        );
        if (raw_replica_queue_model != nullptr) {
            const char* value = json_string_value(raw_replica_queue_model);
            if (value == nullptr) {
                throw std::runtime_error("replica_queue_model must be a string");
            }
            const std::string model(value);
            if (model == "pooled") {
                service.replica_queue_model = ReplicaQueueModel::Pooled;
            } else if (model == "independent_random") {
                service.replica_queue_model = ReplicaQueueModel::IndependentRandom;
            } else if (model == "independent_round_robin") {
                service.replica_queue_model =
                    ReplicaQueueModel::IndependentRoundRobin;
            } else if (model == "independent_key_hash") {
                service.replica_queue_model = ReplicaQueueModel::IndependentKeyHash;
            } else {
                throw std::runtime_error(
                    "unsupported replica_queue_model: " + model
                );
            }
        }
        service.capacity = static_cast<int>(capacity);
        json_t* bandwidth = json_object_get(item, "cpu_bandwidth");
        if (bandwidth != nullptr && !json_is_null(bandwidth)) {
            if (!json_is_object(bandwidth)
                || service.execution_model != ExecutionModel::ProcessorSharing
                || service.replica_queue_model == ReplicaQueueModel::Pooled) {
                throw std::runtime_error("CPU bandwidth requires independent processor-sharing queues");
            }
            service.cpu_quota = json_number(bandwidth, "quota_seconds");
            service.cpu_period = json_number(bandwidth, "period_seconds");
            const std::int64_t parallelism = required_integer(bandwidth, "parallelism");
            if (!std::isfinite(service.cpu_quota) || service.cpu_quota < 0.001
                || !std::isfinite(service.cpu_period) || service.cpu_period < 0.001
                || service.cpu_period > 1.0 || parallelism < 1 || parallelism > 4096
                || service.cpu_quota / service.cpu_period > parallelism) {
                throw std::runtime_error("invalid CPU bandwidth declaration");
            }
            service.cpu_parallelism = static_cast<int>(parallelism);
        }
        json_t* raw_concurrency_capacity = json_object_get(
            item,
            "concurrency_capacity"
        );
        service.concurrency_capacity = (
            raw_concurrency_capacity == nullptr
            ? 0
            : static_cast<int>(json_integer_value(raw_concurrency_capacity))
        );
        if (
            raw_concurrency_capacity != nullptr
            && (!json_is_integer(raw_concurrency_capacity)
                || service.concurrency_capacity < service.capacity
                || service.concurrency_capacity > 10'000'000)
        ) {
            throw std::runtime_error("service concurrency capacity is invalid");
        }
        json_t* raw_replicas = json_object_get(item, "replicas");
        if (raw_replicas == nullptr) {
            service.replicas = service.capacity;
        } else {
            if (!json_is_integer(raw_replicas)
                || json_integer_value(raw_replicas) <= 0
                || json_integer_value(raw_replicas) > capacity) {
                throw std::runtime_error("service replicas are invalid");
            }
            service.replicas = static_cast<int>(
                json_integer_value(raw_replicas)
            );
        }
        if (
            service.execution_model == ExecutionModel::ProcessorSharing
            && service.capacity != service.replicas
        ) {
            throw std::runtime_error(
                "processor-sharing capacity must equal replica count"
            );
        }
        if (
            service.replica_queue_model != ReplicaQueueModel::Pooled
            && service.capacity % service.replicas != 0
        ) {
            throw std::runtime_error(
                "independent replica queues require equal processing capacity"
            );
        }
        if (
            service.replica_queue_model != ReplicaQueueModel::Pooled
            && service.concurrency_capacity != 0
            && service.concurrency_capacity % service.replicas != 0
        ) {
            throw std::runtime_error(
                "independent replica queues require equal concurrency capacity"
            );
        }
        json_t* distributions = json_object_get(
            item,
            "service_time_distributions"
        );
        parse_component_policies(service, json_object_get(item, "component_policies"), class_names);
        if (service.replica_queue_model == ReplicaQueueModel::IndependentKeyHash && !service.cache.has_value())
            throw std::runtime_error("key-hash queues require cache key evidence");
        if (distributions != nullptr) {
            if (!json_is_array(distributions)
                || json_array_size(distributions) != request_class_count) {
                throw std::runtime_error(
                    "service_time_distributions must have one item per "
                    "request class"
                );
            }
            std::size_t class_index;
            json_t* raw_distribution;
            json_array_foreach(
                distributions,
                class_index,
                raw_distribution
            ) {
                service.service_times.push_back(
                    parse_distribution(raw_distribution, false)
                );
            }
        } else {
            const Distribution shared = parse_distribution(
                required(item, "service_time_distribution"),
                false
            );
            service.service_times.assign(request_class_count, shared);
        }
        json_t* blocking_distributions = json_object_get(
            item,
            "blocking_time_distributions"
        );
        if (blocking_distributions == nullptr
            || !json_is_array(blocking_distributions)
            || json_array_size(blocking_distributions) != request_class_count) {
            throw std::runtime_error(
                "blocking_time_distributions must have one item per request class"
            );
        }
        std::size_t blocking_class_index;
        json_t* raw_blocking_distribution;
        json_array_foreach(
            blocking_distributions,
            blocking_class_index,
            raw_blocking_distribution
        ) {
            if (json_is_null(raw_blocking_distribution)) {
                service.blocking_times.push_back(std::nullopt);
            } else {
                service.blocking_times.push_back(
                    parse_distribution(raw_blocking_distribution, false)
                );
            }
        }
        json_t* raw_post_release_delay = required(item, "post_release_delay");
        if (!json_is_null(raw_post_release_delay)) {
            service.post_release_delay = parse_distribution(raw_post_release_delay, false);
        }
        workload.services.push_back(std::move(service));
    }

    json_array_foreach(classes, index, item) {
        RequestClass request_class;
        request_class.name = json_text(item, "name");
        json_t* boundary = json_object_get(item, "completion_boundary");
        if (boundary != nullptr) {
            const auto value = json_text(item, "completion_boundary");
            if (value != "client_response" && value != "all_work")
                throw std::runtime_error("invalid request completion boundary");
            request_class.all_work_completion = value == "all_work";
        }
        request_class.concurrency_weight = json_number(item, "concurrency_weight");
        if (request_class.concurrency_weight < 0.0
            || request_class.concurrency_weight > 1.0) {
            throw std::runtime_error("concurrency weights must lie in [0, 1]");
        }
        json_t* raw_flow_kind = json_object_get(item, "flow_kind");
        if (raw_flow_kind != nullptr) {
            const char* value = json_string_value(raw_flow_kind);
            if (value == nullptr) {
                throw std::runtime_error("flow_kind must be a string");
            }
            const std::string name(value);
            if (name == "routing") {
                request_class.flow_kind = FlowKind::Routing;
            } else if (name == "workflow") {
                request_class.flow_kind = FlowKind::Workflow;
            } else if (name == "calls") {
                request_class.flow_kind = FlowKind::Calls;
            } else if (name == "stages") {
                request_class.flow_kind = FlowKind::Stages;
            } else {
                throw std::runtime_error("unsupported flow_kind: " + name);
            }
        }
        json_t* routing = required(item, "routing");
        if (!json_is_array(routing)
            || json_array_size(routing) != workload.services.size()) {
            throw std::runtime_error("routing must have one row per service");
        }
        request_class.routes.resize(workload.services.size());
        std::size_t service_id;
        json_t* route_list;
        json_array_foreach(routing, service_id, route_list) {
            if (!json_is_array(route_list)) throw std::runtime_error("route row must be an array");
            double total = 0.0;
            std::size_t route_index;
            json_t* route;
            json_array_foreach(route_list, route_index, route) {
                const std::int64_t target = required_integer(route, "target");
                const double probability = json_number(route, "probability");
                if (target < -1 || target >= static_cast<std::int64_t>(workload.services.size())
                    || probability < 0.0) {
                    throw std::runtime_error("invalid routing target or probability");
                }
                total += probability;
                request_class.routes[service_id].push_back({
                    static_cast<int>(target), probability
                });
            }
            if (!request_class.routes[service_id].empty()
                && std::abs(total - 1.0) > 1e-10) {
                throw std::runtime_error("routing probabilities must sum to one");
            }
        }
        json_t* raw_workflow = json_object_get(item, "workflow");
        if (raw_workflow != nullptr) {
            if (!json_is_array(raw_workflow)) {
                throw std::runtime_error("workflow must be an array");
            }
            std::size_t visit_index;
            json_t* raw_visit;
            json_array_foreach(raw_workflow, visit_index, raw_visit) {
                const std::int64_t service = required_integer(
                    raw_visit,
                    "service"
                );
                const double mean_visits = json_number(
                    raw_visit,
                    "mean_visits"
                );
                if (service < 0
                    || service >= static_cast<std::int64_t>(
                        workload.services.size()
                    )
                    || mean_visits <= 0.0) {
                    throw std::runtime_error("invalid workflow visit");
                }
                request_class.workflow.push_back({
                    static_cast<int>(service),
                    mean_visits
                });
            }
        }
        json_t* raw_calls = json_object_get(item, "calls");
        request_class.calls.resize(workload.services.size());
        if (raw_calls != nullptr) {
            if (!json_is_array(raw_calls)
                || json_array_size(raw_calls) != workload.services.size()) {
                throw std::runtime_error(
                    "calls must have one row per source service"
                );
            }
            std::size_t source_id;
            json_t* raw_children;
            json_array_foreach(raw_calls, source_id, raw_children) {
                if (!json_is_array(raw_children)) {
                    throw std::runtime_error("call row must be an array");
                }
                std::size_t child_index;
                json_t* raw_child;
                json_array_foreach(raw_children, child_index, raw_child) {
                    const std::int64_t service = required_integer(
                        raw_child,
                        "service"
                    );
                    const double mean_visits = json_number(
                        raw_child,
                        "mean_visits"
                    );
                    if (service < 0
                        || service >= static_cast<std::int64_t>(
                            workload.services.size()
                        )
                        || mean_visits <= 0.0) {
                        throw std::runtime_error("invalid synchronous child call");
                    }
                    request_class.calls[source_id].push_back({
                        static_cast<int>(service),
                        mean_visits
                    });
                }
            }
        }
        json_t* raw_call_execution = json_object_get(item, "call_execution");
        if (raw_call_execution != nullptr) {
            const char* value = json_string_value(raw_call_execution);
            if (value == nullptr) {
                throw std::runtime_error("call_execution must be a string");
            }
            const std::string name(value);
            if (name == "parallel") {
                request_class.call_execution = CallExecution::Parallel;
            } else if (name == "sequential") {
                request_class.call_execution = CallExecution::Sequential;
            } else {
                throw std::runtime_error(
                    "unsupported call_execution: " + name
                );
            }
        }
        request_class.call_policies.assign(
            workload.services.size(),
            CallPolicy{request_class.call_execution, CallCompletion::All, 0}
        );
        json_t* raw_policies = json_object_get(item, "call_policies");
        if (raw_policies != nullptr) {
            if (!json_is_array(raw_policies)
                || json_array_size(raw_policies) != workload.services.size()) {
                throw std::runtime_error("call_policies needs one row per service");
            }
            std::size_t source;
            json_t* raw_policy;
            json_array_foreach(raw_policies, source, raw_policy) {
                const char* execution = json_string_value(json_object_get(raw_policy, "execution"));
                const char* completion = json_string_value(json_object_get(raw_policy, "completion"));
                if (execution == nullptr || completion == nullptr) {
                    throw std::runtime_error("call policy needs execution and completion strings");
                }
                CallPolicy& policy = request_class.call_policies[source];
                const std::string execution_name(execution);
                const std::string completion_name(completion);
                if (execution_name == "parallel") policy.execution = CallExecution::Parallel;
                else if (execution_name == "sequential") policy.execution = CallExecution::Sequential;
                else if (execution_name == "choice") policy.execution = CallExecution::Choice;
                else throw std::runtime_error("unsupported per-service call execution");
                if (completion_name == "all") policy.completion = CallCompletion::All;
                else if (completion_name == "quorum") policy.completion = CallCompletion::Quorum;
                else if (completion_name == "background") policy.completion = CallCompletion::Background;
                else throw std::runtime_error("unsupported per-service call completion");
                policy.required = required_integer(raw_policy, "required");
                const auto& children = request_class.calls[source];
                if (policy.completion == CallCompletion::Quorum) {
                    if (policy.execution != CallExecution::Parallel
                        || policy.required <= 0
                        || policy.required > static_cast<std::int64_t>(children.size())) {
                        throw std::runtime_error("invalid quorum call policy");
                    }
                    std::vector<bool> seen(workload.services.size(), false);
                    for (const WorkflowVisit& child : children) {
                        if (child.mean_visits != 1.0 || seen[child.service]) {
                            throw std::runtime_error("quorum requires distinct one-call targets");
                        }
                        seen[child.service] = true;
                    }
                } else if (policy.required != 0) {
                    throw std::runtime_error("required is only valid for quorum completion");
                }
                if (policy.completion == CallCompletion::Background
                    && policy.execution != CallExecution::Parallel) {
                    throw std::runtime_error("background completion requires parallel calls");
                }
                if (policy.execution == CallExecution::Choice) {
                    double total = 0.0;
                    for (const WorkflowVisit& child : children) total += child.mean_visits;
                    if (policy.completion != CallCompletion::All || std::abs(total - 1.0) > 1e-10) {
                        throw std::runtime_error("choice needs probabilities summing to one");
                    }
                }
            }
        }
        json_t* staged = json_object_get(item,"stage_workflow");
        if (request_class.flow_kind == FlowKind::Stages) {
            if (staged == nullptr || !json_is_object(staged) || !request_class.workflow.empty()) {
                throw std::runtime_error("stages flow_kind needs an exclusive stage_workflow");
            }
            parse_stage_workflow(staged,request_class,workload.services,index);
        } else if (staged != nullptr && !json_is_null(staged)) {
            throw std::runtime_error("stage_workflow requires stages flow_kind");
        }
        if (request_class.flow_kind == FlowKind::Workflow
            && request_class.workflow.empty()) {
            throw std::runtime_error("workflow flow_kind needs workflow visits");
        }
        json_t* raw_overhead = json_object_get(
            item,
            "latency_overhead_distribution"
        );
        if (raw_overhead != nullptr && !json_is_null(raw_overhead)) {
            request_class.latency_overhead = parse_distribution(
                raw_overhead,
                false
            );
        }
        json_t* raw_components = json_object_get(item, "latency_components");
        if (raw_components != nullptr) {
            if (!json_is_array(raw_components)) {
                throw std::runtime_error("latency_components must be an array");
            }
            std::size_t component_index;
            json_t* raw_component;
            json_array_foreach(
                raw_components,
                component_index,
                raw_component
            ) {
                const std::string component_name = json_text(
                    raw_component,
                    "name"
                );
                if (component_name.empty()) {
                    throw std::runtime_error(
                        "latency component names must be nonempty"
                    );
                }
                request_class.latency_components.push_back(
                    parse_distribution(
                        required(raw_component, "distribution"),
                        false
                    )
                );
            }
        }
        if (request_class.latency_overhead.has_value()
            && !request_class.latency_components.empty()) {
            throw std::runtime_error(
                "legacy latency overhead and components are mutually exclusive"
            );
        }
        json_t* raw_timeout = json_object_get(item, "end_to_end_timeout");
        if (raw_timeout != nullptr) {
            if (!json_is_number(raw_timeout)) {
                throw std::runtime_error("end_to_end_timeout must be numeric");
            }
            request_class.end_to_end_timeout = json_number_value(raw_timeout);
            if (!std::isfinite(request_class.end_to_end_timeout)
                || request_class.end_to_end_timeout < 0.0) {
                throw std::runtime_error(
                    "end_to_end_timeout must be finite and non-negative"
                );
            }
        }
        workload.request_classes.push_back(std::move(request_class));
    }
    double concurrency_weight_total = 0.0;
    for (const RequestClass& request_class : workload.request_classes) {
        concurrency_weight_total += request_class.concurrency_weight;
    }
    if (std::abs(concurrency_weight_total - 1.0) > 1e-10) {
        throw std::runtime_error("request-class concurrency weights must sum to one");
    }

    json_t* streams = required(raw, "external_streams");
    if (!json_is_array(streams)) {
        throw std::runtime_error("external_streams must be an array");
    }
    json_array_foreach(streams, index, item) {
        const std::int64_t request_class = required_integer(item, "request_class");
        const std::int64_t entry_service = required_integer(item, "entry_service");
        if (request_class < 0
            || request_class >= static_cast<std::int64_t>(workload.request_classes.size())
            || entry_service < 0
            || entry_service >= static_cast<std::int64_t>(workload.services.size())) {
            throw std::runtime_error("invalid external stream");
        }
        workload.external_streams.push_back({
            static_cast<int>(request_class),
            static_cast<int>(entry_service),
            parse_distribution(required(item, "interarrival_distribution"), true)
        });
        const auto& workflow = workload.request_classes[request_class].workflow;
        const auto& staged_class = workload.request_classes[request_class];
        if (staged_class.flow_kind == FlowKind::Stages && staged_class.stage_entries[entry_service] < 0) {
            throw std::runtime_error("external stream lacks a stage entry");
        }
        if (!workflow.empty()
            && workflow.front().service != static_cast<int>(entry_service)) {
            throw std::runtime_error(
                "workflow must begin at its external entry service"
            );
        }
    }

    json_t* raw_communication = json_object_get(raw, "communication_latency");
    if (raw_communication != nullptr) {
        if (!json_is_object(raw_communication)) {
            throw std::runtime_error("communication_latency must be an object");
        }
        workload.same_node_one_way_latency = parse_distribution(
            required(raw_communication, "same_node_one_way"),
            false
        );
        workload.cross_node_one_way_latency = parse_distribution(
            required(raw_communication, "cross_node_one_way"),
            false
        );
        workload.cross_node_probability = json_number(
            raw_communication,
            "cross_node_probability"
        );
        if (workload.cross_node_probability < 0.0
            || workload.cross_node_probability > 1.0) {
            throw std::runtime_error(
                "cross_node_probability must lie in [0, 1]"
            );
        }
    }

    json_t* raw_input_uncertainty = json_object_get(raw, "input_uncertainty");
    if (raw_input_uncertainty != nullptr) {
        if (!json_is_object(raw_input_uncertainty)) {
            throw std::runtime_error("input_uncertainty must be an object");
        }
        const std::string sampling = json_text(
            raw_input_uncertainty,
            "sampling"
        );
        if (sampling != "moving_block_bootstrap") {
            throw std::runtime_error(
                "input_uncertainty sampling must be moving_block_bootstrap"
            );
        }
        const std::int64_t block_length = required_integer(
            raw_input_uncertainty,
            "block_length"
        );
        json_t* epochs = required(raw_input_uncertainty, "epochs");
        if (!json_is_array(epochs) || json_array_size(epochs) < 2) {
            throw std::runtime_error(
                "input_uncertainty must contain at least two epochs"
            );
        }
        if (block_length <= 0
            || block_length > static_cast<std::int64_t>(json_array_size(epochs))) {
            throw std::runtime_error("input_uncertainty block_length is invalid");
        }
        workload.input_block_length = static_cast<std::size_t>(block_length);
        std::size_t epoch_index;
        json_t* raw_epoch;
        json_array_foreach(epochs, epoch_index, raw_epoch) {
            if (!json_is_object(raw_epoch)) {
                throw std::runtime_error("input uncertainty epoch must be an object");
            }
            InputEpoch epoch;
            const auto parse_multipliers = [](
                json_t* values,
                std::size_t expected_size,
                const char* label
            ) {
                if (!json_is_array(values)
                    || json_array_size(values) != expected_size) {
                    throw std::runtime_error(
                        std::string(label) + " has the wrong length"
                    );
                }
                std::vector<double> parsed;
                parsed.reserve(expected_size);
                std::size_t value_index;
                json_t* raw_value;
                json_array_foreach(values, value_index, raw_value) {
                    if (!json_is_number(raw_value)) {
                        throw std::runtime_error(
                            std::string(label) + " must be numeric"
                        );
                    }
                    const double value = json_number_value(raw_value);
                    if (!std::isfinite(value) || value <= 0.0) {
                        throw std::runtime_error(
                            std::string(label) + " must be finite and positive"
                        );
                    }
                    parsed.push_back(value);
                }
                return parsed;
            };
            epoch.arrival_rate_multipliers = parse_multipliers(
                required(raw_epoch, "arrival_rate_multipliers"),
                workload.request_classes.size(),
                "arrival_rate_multipliers"
            );
            epoch.service_time_multipliers = parse_multipliers(
                required(raw_epoch, "service_time_multipliers"),
                workload.services.size(),
                "service_time_multipliers"
            );
            workload.input_epochs.push_back(std::move(epoch));
        }
    }

    return workload;
}

json_t* json_ints(const std::vector<int>& values) {
    json_t* output = json_array();
    for (int value : values) json_array_append_new(output, json_integer(value));
    return output;
}

json_t* json_int64s(const std::vector<std::int64_t>& values) {
    json_t* output = json_array();
    for (std::int64_t value : values) json_array_append_new(output, json_integer(value));
    return output;
}

json_t* json_doubles(const std::vector<double>& values) {
    json_t* output = json_array();
    for (double value : values) json_array_append_new(output, json_real(value));
    return output;
}

json_t* json_double_matrix(const std::vector<std::vector<double>>& values) {
    json_t* output = json_array();
    for (const auto& row : values) json_array_append_new(output, json_doubles(row));
    return output;
}

json_t* class_double_array_map(
    const Workload& workload,
    const std::vector<std::vector<double>>& observations
) {
    json_t* output = json_object();
    for (std::size_t request_class = 0; request_class < observations.size(); ++request_class) {
        json_object_set_new(
            output,
            workload.request_classes[request_class].name.c_str(),
            json_doubles(observations[request_class])
        );
    }
    return output;
}

json_t* class_int_array_map(
    const Workload& workload,
    const std::vector<std::vector<int>>& observations
) {
    json_t* output = json_object();
    for (std::size_t request_class = 0;
         request_class < observations.size();
         ++request_class) {
        json_object_set_new(
            output,
            workload.request_classes[request_class].name.c_str(),
            json_ints(observations[request_class])
        );
    }
    return output;
}

json_t* json_history(const std::vector<Point>& history) {
    json_t* output = json_array();
    for (const Point& point : history) {
        json_t* value = json_array();
        json_array_append_new(value, json_real(point.time));
        json_array_append_new(value, json_integer(point.value));
        json_array_append_new(output, value);
    }
    return output;
}

json_t* json_histories(const std::vector<std::vector<Point>>& histories) {
    json_t* output = json_array();
    for (const auto& history : histories) json_array_append_new(output, json_history(history));
    return output;
}

json_t* json_service_names(const Workload& workload) {
    json_t* output = json_array();
    for (const Service& service : workload.services) {
        json_array_append_new(output, json_string(service.name.c_str()));
    }
    return output;
}

json_t* json_capacities(const Workload& workload) {
    json_t* output = json_array();
    for (const Service& service : workload.services) {
        json_array_append_new(output, json_integer(service.replicas));
    }
    return output;
}

json_t* class_double_map(
    const Workload& workload,
    const std::vector<std::vector<double>>& observations,
    double requested_percentile
) {
    json_t* output = json_object();
    for (std::size_t request_class = 0; request_class < observations.size(); ++request_class) {
        if (!observations[request_class].empty()) {
            json_object_set_new(
                output,
                workload.request_classes[request_class].name.c_str(),
                json_real(percentile(observations[request_class], requested_percentile))
            );
        }
    }
    return output;
}

json_t* class_count_map(
    const Workload& workload,
    const std::vector<std::int64_t>& counts
) {
    json_t* output = json_object();
    for (std::size_t request_class = 0; request_class < counts.size(); ++request_class) {
        json_object_set_new(
            output,
            workload.request_classes[request_class].name.c_str(),
            json_integer(counts[request_class])
        );
    }
    return output;
}

std::int64_t peak_memory_bytes() {
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
    return static_cast<std::int64_t>(usage.ru_maxrss) * 1024LL;
}

json_t* single_result(
    const Workload& workload,
    const Kernel& kernel,
    const KernelSummary& summary,
    bool capture_state,
    double horizon
) {
    json_t* result = json_object();
    json_object_set_new(result, "seed", json_integer(kernel.seed()));
    json_object_set_new(result, "horizon", json_real(horizon));
    json_object_set_new(result, "stop_time", json_real(summary.stop_time));
    json_object_set_new(result, "processed_event_count", json_integer(summary.processed_event_count));
    json_object_set_new(result, "external_arrival_count", json_integer(summary.external_arrival_count));
    json_object_set_new(result, "completed_request_count", json_integer(summary.completed_request_count));
    json_object_set_new(result, "active_requests_at_end", json_integer(kernel.recorder().active()));
    json_object_set_new(result, "queue_lengths_at_end", json_ints(kernel.recorder().queues()));
    json_object_set_new(result, "busy_replicas_at_end", json_ints(kernel.recorder().busy()));
    json_object_set_new(result, "queue_areas", json_doubles(kernel.recorder().queue_areas()));
    json_object_set_new(result, "busy_areas", json_doubles(kernel.recorder().busy_areas()));

    std::vector<double> mean_utilizations(workload.services.size());
    std::vector<double> mean_latencies(workload.services.size());
    std::vector<double> p95_latencies(workload.services.size());
    std::vector<double> p99_latencies(workload.services.size());
    for (std::size_t service = 0; service < workload.services.size(); ++service) {
        mean_utilizations[service] = kernel.recorder().busy_areas()[service]
            / (static_cast<double>(workload.services[service].capacity) * horizon);
        const auto& observations = kernel.service_response_times()[service];
        if (!observations.empty()) {
            double total = 0.0;
            for (double value : observations) total += value;
            mean_latencies[service] = total / static_cast<double>(observations.size());
        }
        p95_latencies[service] = percentile(observations, 95.0);
        p99_latencies[service] = percentile(observations, 99.0);
    }
    json_t* metrics = json_object();
    json_object_set_new(metrics, "mean_utilizations", json_doubles(mean_utilizations));
    json_object_set_new(metrics, "mean_latencies", json_doubles(mean_latencies));
    json_object_set_new(metrics, "p95_latencies", json_doubles(p95_latencies));
    json_object_set_new(metrics, "p99_latencies", json_doubles(p99_latencies));
    json_object_set_new(
        metrics,
        "request_class_p95_latencies",
        class_double_map(workload, kernel.request_class_response_times(), 95.0)
    );
    json_object_set_new(result, "metrics", metrics);

    if (capture_state) {
        json_object_set_new(result, "active_history", json_history(kernel.recorder().active_history()));
        json_object_set_new(result, "queue_history", json_histories(kernel.recorder().queue_history()));
        json_object_set_new(result, "busy_history", json_histories(kernel.recorder().busy_history()));
    } else {
        json_object_set_new(result, "active_history", json_null());
        json_object_set_new(result, "queue_history", json_null());
        json_object_set_new(result, "busy_history", json_null());
    }
    return result;
}

struct BucketMoments {
    BucketOutput sum;
    BucketOutput sum_squares;
};

void add_bucket_values(BucketOutput& target, const BucketOutput& source, bool square) {
    if (target.active.empty()) {
        target.active.assign(source.active.size(), 0.0);
        target.queues.resize(source.queues.size());
        target.busy.resize(source.busy.size());
        for (std::size_t service = 0; service < source.queues.size(); ++service) {
            target.queues[service].assign(source.queues[service].size(), 0.0);
            target.busy[service].assign(source.busy[service].size(), 0.0);
        }
    }
    for (std::size_t bucket = 0; bucket < source.active.size(); ++bucket) {
        const double value = source.active[bucket];
        target.active[bucket] += square ? value * value : value;
    }
    for (std::size_t service = 0; service < source.queues.size(); ++service) {
        for (std::size_t bucket = 0; bucket < source.queues[service].size(); ++bucket) {
            const double value = source.queues[service][bucket];
            target.queues[service][bucket] += square ? value * value : value;
        }
        for (std::size_t bucket = 0; bucket < source.busy[service].size(); ++bucket) {
            const double value = source.busy[service][bucket];
            target.busy[service][bucket] += square ? value * value : value;
        }
    }
}

BucketOutput means(const BucketOutput& sums, double count) {
    BucketOutput output = sums;
    for (double& value : output.active) value /= count;
    for (auto& row : output.queues) for (double& value : row) value /= count;
    for (auto& row : output.busy) for (double& value : row) value /= count;
    return output;
}

BucketOutput sample_variances(
    const BucketOutput& sums,
    const BucketOutput& squares,
    std::size_t count
) {
    BucketOutput output = squares;
    const double divisor = count > 1 ? static_cast<double>(count - 1) : 1.0;
    for (std::size_t i = 0; i < output.active.size(); ++i) {
        output.active[i] = count > 1
            ? std::max(0.0, (squares.active[i] - sums.active[i] * sums.active[i] / count) / divisor)
            : 0.0;
    }
    for (std::size_t service = 0; service < output.queues.size(); ++service) {
        for (std::size_t i = 0; i < output.queues[service].size(); ++i) {
            output.queues[service][i] = count > 1
                ? std::max(0.0, (squares.queues[service][i]
                    - sums.queues[service][i] * sums.queues[service][i] / count) / divisor)
                : 0.0;
            output.busy[service][i] = count > 1
                ? std::max(0.0, (squares.busy[service][i]
                    - sums.busy[service][i] * sums.busy[service][i] / count) / divisor)
                : 0.0;
        }
    }
    return output;
}

json_t* bucket_fields(const BucketOutput& values) {
    json_t* output = json_object();
    json_object_set_new(output, "active_requests", json_doubles(values.active));
    json_object_set_new(output, "queue_length_by_service", json_double_matrix(values.queues));
    json_object_set_new(output, "busy_replicas_by_service", json_double_matrix(values.busy));
    return output;
}

template <typename Function>
void run_parallel(std::size_t count, int workers, Function function) {
    const int bounded_workers = std::min(workers, static_cast<int>(count));
    std::atomic<std::size_t> next{0};
    std::vector<std::thread> pool;
    for (int worker = 0; worker < bounded_workers; ++worker) {
        pool.emplace_back([&] {
            while (true) {
                const std::size_t index = next.fetch_add(1);
                if (index >= count) return;
                function(index);
            }
        });
    }
    for (auto& thread : pool) thread.join();
}

json_t* run_simulate(json_t* request, const Workload& workload) {
    const std::int64_t seed = required_integer(request, "seed");
    const double horizon = json_number(request, "horizon");
    const double warmup = json_number(request, "warmup_duration");
    const bool capture = required_boolean(request, "capture_state");
    if (seed < 0 || horizon <= 0.0 || warmup < 0.0 || warmup > horizon) {
        throw std::runtime_error("invalid seed, horizon, or warmup_duration");
    }
    Kernel kernel(
        workload, seed, RunMode::FixedHorizon, horizon, warmup, 0.0,
        capture, true, true
    );
    const KernelSummary summary = kernel.run();
    return single_result(workload, kernel, summary, capture, horizon);
}

json_t* run_warmup_ensemble(json_t* request, const Workload& workload) {
    const std::vector<std::int64_t> seeds = parse_seeds(request);
    const double horizon = json_number(request, "horizon");
    const double bucket_width = json_number(request, "bucket_width");
    const std::int64_t workers_raw = required_integer(request, "workers");
    if (horizon <= 0.0 || bucket_width <= 0.0 || workers_raw <= 0) {
        throw std::runtime_error("invalid horizon, bucket_width, or workers");
    }
    const int workers = static_cast<int>(std::min<std::int64_t>(workers_raw, seeds.size()));
    const auto start = std::chrono::steady_clock::now();
    BucketMoments moments;
    std::int64_t processed = 0;
    std::int64_t external = 0;
    std::int64_t completed = 0;

    // Retain at most one output per active worker. Combining batches in seed
    // order preserves deterministic floating-point summation while bounding
    // memory independently of the replication count.
    for (std::size_t batch_start = 0; batch_start < seeds.size();
         batch_start += static_cast<std::size_t>(workers)) {
        const std::size_t batch_size = std::min(
            static_cast<std::size_t>(workers),
            seeds.size() - batch_start
        );
        std::vector<std::optional<BucketOutput>> outputs(batch_size);
        std::vector<KernelSummary> summaries(batch_size);
        run_parallel(batch_size, workers, [&](std::size_t local_index) {
            const std::size_t seed_index = batch_start + local_index;
            Kernel kernel(
                workload, seeds[seed_index], RunMode::FixedHorizon,
                horizon, 0.0, 0.0, false, false, false, bucket_width
            );
            summaries[local_index] = kernel.run();
            outputs[local_index] = kernel.buckets();
        });
        for (std::size_t local_index = 0; local_index < batch_size; ++local_index) {
            add_bucket_values(moments.sum, outputs[local_index].value(), false);
            add_bucket_values(
                moments.sum_squares,
                outputs[local_index].value(),
                true
            );
            processed += summaries[local_index].processed_event_count;
            external += summaries[local_index].external_arrival_count;
            completed += summaries[local_index].completed_request_count;
        }
    }
    const double wall = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start
    ).count();
    const BucketOutput average = means(moments.sum, static_cast<double>(seeds.size()));
    const BucketOutput variance = sample_variances(
        moments.sum, moments.sum_squares, seeds.size()
    );

    json_t* result = json_object();
    json_object_set_new(result, "service_names", json_service_names(workload));
    json_object_set_new(result, "replica_configuration", json_capacities(workload));
    json_object_set_new(result, "horizon", json_real(horizon));
    json_object_set_new(result, "bucket_width", json_real(bucket_width));
    json_object_set_new(result, "seeds", json_int64s(seeds));
    json_object_set_new(result, "means", bucket_fields(average));
    json_object_set_new(result, "sample_variances", bucket_fields(variance));
    json_object_set_new(result, "processed_event_count", json_integer(processed));
    json_object_set_new(result, "external_arrival_count", json_integer(external));
    json_object_set_new(result, "completed_request_count", json_integer(completed));
    json_object_set_new(result, "wall_time_seconds", json_real(wall));
    json_object_set_new(result, "peak_memory_bytes", json_integer(peak_memory_bytes()));
    return result;
}

struct MeasurementOutput {
    KernelSummary summary;
    std::vector<std::int64_t> measured_counts;
    std::vector<std::vector<double>> latencies;
    std::vector<std::vector<double>> arrival_times;
    std::vector<std::vector<int>> failure_flags;
    int active_requests_at_end{};
    std::vector<int> queue_lengths_at_end;
    std::vector<int> busy_replicas_at_end;
    std::int64_t tagged_requests_in_flight_at_end{};
    int sampled_input_block_start{-1};
    std::vector<double> input_arrival_rate_multipliers;
    std::vector<double> input_service_time_multipliers;
};

json_t* run_measurement_replications(json_t* request, const Workload& workload) {
    const std::vector<std::int64_t> seeds = parse_seeds(request);
    const double warmup = json_number(request, "warmup_duration");
    const double measurement = json_number(request, "measurement_duration");
    const double maximum_drain_time = json_number(request, "maximum_drain_time");
    const std::int64_t workers_raw = required_integer(request, "workers");
    if (warmup < 0.0 || measurement <= 0.0 || maximum_drain_time <= 0.0
        || workers_raw <= 0) {
        throw std::runtime_error("invalid warmup_duration, measurement_duration, or workers");
    }
    const int workers = static_cast<int>(std::min<std::int64_t>(workers_raw, seeds.size()));
    std::vector<std::optional<MeasurementOutput>> outputs(seeds.size());
    run_parallel(seeds.size(), workers, [&](std::size_t index) {
        Kernel kernel(
            workload, seeds[index], RunMode::MeasurementDrain,
            warmup + measurement, warmup, measurement,
            false, false, true, 0.0, maximum_drain_time
        );
        MeasurementOutput output;
        output.summary = kernel.run();
        output.measured_counts = kernel.measured_request_counts();
        output.latencies = kernel.take_request_class_response_times();
        output.arrival_times = kernel.take_request_class_arrival_times();
        output.failure_flags = kernel.take_request_class_failure_flags();
        output.active_requests_at_end = kernel.recorder().active();
        output.queue_lengths_at_end = kernel.recorder().queues();
        output.busy_replicas_at_end = kernel.recorder().busy();
        output.tagged_requests_in_flight_at_end = kernel.tagged_requests_in_flight();
        output.sampled_input_block_start = kernel.sampled_input_block_start();
        output.input_arrival_rate_multipliers = (
            kernel.input_arrival_rate_multipliers()
        );
        output.input_service_time_multipliers = (
            kernel.input_service_time_multipliers()
        );
        outputs[index] = std::move(output);
    });

    json_t* result = json_object();
    json_object_set_new(result, "service_names", json_service_names(workload));
    json_object_set_new(result, "replica_configuration", json_capacities(workload));
    json_t* replications = json_array();
    for (std::size_t index = 0; index < seeds.size(); ++index) {
        const MeasurementOutput& output = outputs[index].value();
        json_t* item = json_object();
        json_object_set_new(item, "seed", json_integer(seeds[index]));
        json_object_set_new(item, "stop_time", json_real(output.summary.stop_time));
        json_object_set_new(
            item, "measured_request_counts",
            class_count_map(workload, output.measured_counts)
        );
        json_object_set_new(
            item, "request_class_p95_latencies",
            class_double_map(workload, output.latencies, 95.0)
        );
        json_object_set_new(
            item, "request_class_latencies",
            class_double_array_map(workload, output.latencies)
        );
        json_object_set_new(
            item, "request_class_arrival_times",
            class_double_array_map(workload, output.arrival_times)
        );
        json_object_set_new(
            item, "request_class_failures",
            class_int_array_map(workload, output.failure_flags)
        );
        std::vector<std::int64_t> completed_measured_counts;
        std::vector<std::int64_t> failed_measured_counts;
        completed_measured_counts.reserve(output.latencies.size());
        failed_measured_counts.reserve(output.failure_flags.size());
        for (std::size_t request_class = 0;
             request_class < output.latencies.size();
             ++request_class) {
            const auto& observations = output.latencies[request_class];
            completed_measured_counts.push_back(
                static_cast<std::int64_t>(observations.size())
            );
            std::int64_t failed = 0;
            for (int value : output.failure_flags[request_class]) failed += value;
            failed_measured_counts.push_back(failed);
        }
        json_object_set_new(
            item, "completed_measured_request_counts",
            class_count_map(workload, completed_measured_counts)
        );
        json_object_set_new(
            item, "failed_measured_request_counts",
            class_count_map(workload, failed_measured_counts)
        );
        json_object_set_new(
            item, "active_requests_at_end", json_integer(output.active_requests_at_end)
        );
        json_object_set_new(
            item, "queue_lengths_at_end", json_ints(output.queue_lengths_at_end)
        );
        json_object_set_new(
            item, "busy_replicas_at_end", json_ints(output.busy_replicas_at_end)
        );
        json_object_set_new(
            item, "tagged_requests_in_flight_at_end",
            json_integer(output.tagged_requests_in_flight_at_end)
        );
        if (output.sampled_input_block_start < 0) {
            json_object_set_new(item, "sampled_input_block_start", json_null());
        } else {
            json_object_set_new(
                item,
                "sampled_input_block_start",
                json_integer(output.sampled_input_block_start)
            );
        }
        json_object_set_new(
            item,
            "input_arrival_rate_multipliers",
            json_doubles(output.input_arrival_rate_multipliers)
        );
        json_object_set_new(
            item,
            "input_service_time_multipliers",
            json_doubles(output.input_service_time_multipliers)
        );
        json_object_set_new(item, "processed_event_count", json_integer(output.summary.processed_event_count));
        json_object_set_new(item, "external_arrival_count", json_integer(output.summary.external_arrival_count));
        json_object_set_new(item, "completed_request_count", json_integer(output.summary.completed_request_count));
        json_array_append_new(replications, item);
        outputs[index].reset();
    }
    json_object_set_new(result, "replications", replications);
    json_object_set_new(result, "peak_memory_bytes", json_integer(peak_memory_bytes()));
    return result;
}

void emit_response(const std::string& operation, bool ok, json_t* result, const char* error) {
    JsonOwner response(json_object());
    json_object_set_new(response.value, "protocol_version", json_integer(kProtocolVersion));
    json_object_set_new(response.value, "operation", json_string(operation.c_str()));
    json_object_set_new(response.value, "ok", json_boolean(ok));
    if (ok) {
        json_object_set_new(response.value, "result", result);
    } else {
        json_t* detail = json_object();
        json_object_set_new(detail, "code", json_string("NATIVE_SIMULATION_ERROR"));
        json_object_set_new(detail, "message", json_string(error));
        json_object_set_new(response.value, "error", detail);
    }
    if (json_dumpf(response.value, stdout, JSON_COMPACT | JSON_ENCODE_ANY) != 0) {
        throw std::runtime_error("failed to encode JSON response");
    }
    std::cout << '\n';
}

}  // namespace

int main() {
    std::string operation = "unknown";
    try {
        json_error_t error{};
        JsonOwner request(json_loadf(stdin, JSON_REJECT_DUPLICATES, &error));
        if (!request.value) throw std::runtime_error(error.text);
        if (required_integer(request.value, "protocol_version") != kProtocolVersion) {
            throw std::runtime_error("unsupported protocol_version");
        }
        operation = json_text(request.value, "operation");
        const Workload workload = parse_workload(required(request.value, "workload"));
        json_t* result = nullptr;
        if (operation == "simulate") {
            result = run_simulate(request.value, workload);
        } else if (operation == "warmup_ensemble") {
            result = run_warmup_ensemble(request.value, workload);
        } else if (operation == "measurement_replications") {
            result = run_measurement_replications(request.value, workload);
        } else {
            throw std::runtime_error("unsupported operation");
        }
        emit_response(operation, true, result, nullptr);
        return 0;
    } catch (const std::exception& error) {
        try {
            emit_response(operation, false, nullptr, error.what());
        } catch (...) {
            std::cerr << "native simulator failed: " << error.what() << '\n';
        }
        return 2;
    }
}
