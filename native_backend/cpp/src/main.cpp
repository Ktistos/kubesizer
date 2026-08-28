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
#include <iostream>
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

constexpr int kProtocolVersion = 11;
constexpr int kUrgentPriority = 0;
constexpr int kNormalPriority = 1;
constexpr int kDeadlinePriority = 2;

enum class EventKind {
    ExternalArrival,
    Admit,
    ServiceCompletion,
    Grant,
    Resume,
    ConcurrencyGrant,
    CallReturn,
    FinalExit,
    RequestTimeout
};
enum class RunMode { FixedHorizon, MeasurementDrain };
enum class FlowKind { Routing, Workflow, Calls };
enum class CallExecution { Parallel, Sequential };
enum class ExecutionModel { FixedSlots, ProcessorSharing };
enum class ReplicaQueueModel { Pooled, IndependentRandom, IndependentRoundRobin };
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

struct Service {
    std::string name;
    int replicas{};
    int capacity{};
    int concurrency_capacity{};
    ExecutionModel execution_model{ExecutionModel::FixedSlots};
    ReplicaQueueModel replica_queue_model{ReplicaQueueModel::Pooled};
    std::vector<Distribution> service_times;
};

struct Route {
    int target{-1};
    double probability{};
};

struct WorkflowVisit {
    int service{-1};
    double mean_visits{};
};

struct RequestClass {
    std::string name;
    double concurrency_weight{};
    FlowKind flow_kind{FlowKind::Routing};
    std::vector<std::vector<Route>> routes;
    std::vector<WorkflowVisit> workflow;
    std::vector<std::vector<WorkflowVisit>> calls;
    CallExecution call_execution{CallExecution::Parallel};
    std::optional<Distribution> latency_overhead;
    std::vector<Distribution> latency_components;
    double end_to_end_timeout{};
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
    double elapsed{};
    bool initialized{};
};

struct Workload {
    std::string name;
    std::vector<Service> services;
    std::vector<RequestClass> request_classes;
    std::vector<ExternalStream> external_streams;
    std::vector<InputEpoch> input_epochs;
    std::size_t input_block_length{};
    std::vector<std::vector<int>> service_replica_counts_by_node;
    std::vector<std::vector<double>> node_communication_latencies;
    std::vector<double> node_execution_time_multipliers;
    std::optional<Distribution> same_node_one_way_latency;
    std::optional<Distribution> cross_node_one_way_latency;
    double unplaced_cross_node_probability{};
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
};

struct Visit {
    Request* request{};
    int service{-1};
    int node{-1};
    int replica{-1};
    Visit* parent{};
    std::size_t ownership_index{};
    std::int64_t pending_children{};
    double arrival_time{};
    std::vector<int> sequential_children;
    std::size_t next_sequential_child{};
    double return_network_delay{};
    double remaining_work{};
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
        }
        initialize_input_uncertainty();
    }

    KernelSummary run() {
        const auto start = std::chrono::steady_clock::now();
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
        int queue = -1
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
            generation
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
                if (!trace_state->initialized
                    || trace_state->elapsed >= distribution.block_duration
                    || trace_state->next_index >= distribution.samples.size()) {
                    std::uniform_int_distribution<std::size_t> choose(
                        0,
                        distribution.samples.size() - 1
                    );
                    trace_state->next_index = choose(random_);
                    trace_state->elapsed = 0.0;
                    trace_state->initialized = true;
                }
                result = distribution.samples[trace_state->next_index++];
                trace_state->elapsed += result;
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
        if (!std::isfinite(result) || result <= 0.0) {
            throw std::runtime_error(
                "a distribution generated a non-positive or non-finite sample"
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
            case EventKind::Grant:
                grant(event.index, event.queue, event.time);
                break;
            case EventKind::Resume: resume(event.visit, event.time); break;
            case EventKind::ConcurrencyGrant:
                concurrency_grant(event.index, event.queue, event.time);
                break;
            case EventKind::CallReturn:
                complete_child_return(event.visit, event.time); break;
            case EventKind::FinalExit: final_exit(event.request, event.time); break;
            case EventKind::RequestTimeout:
                request_timeout(event.request, event.time); break;
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
        requests_[ownership_index] = std::make_unique<Request>(Request{
            next_request_id_++, stream.request_class, now, ownership_index,
            eligible, tagged, 0, 0, 0, false, false, false
        });
        Request* request = requests_[ownership_index].get();
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
        Visit* root = create_visit(request, entry_service, nullptr, now);
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
        const int replica = choose_replica(service);
        visits_[ownership_index] = std::make_unique<Visit>(Visit{
            request,
            service,
            choose_node(service, replica),
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

    int choose_replica(int service) {
        const Service& specification = workload_.services[service];
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

    int choose_node(int service, int replica) {
        if (workload_.service_replica_counts_by_node.empty()) return -1;
        const auto& counts = workload_.service_replica_counts_by_node[service];
        if (replica >= 0) {
            int offset = replica;
            for (std::size_t node = 0; node < counts.size(); ++node) {
                if (offset < counts[node]) return static_cast<int>(node);
                offset -= counts[node];
            }
            throw std::runtime_error("could not map service replica to a node");
        }
        int total = 0;
        for (int count : counts) total += count;
        if (total <= 0) {
            throw std::runtime_error("placed service has no replicas");
        }
        std::uniform_int_distribution<int> choose(0, total - 1);
        int draw = choose(random_);
        for (std::size_t node = 0; node < counts.size(); ++node) {
            if (draw < counts[node]) return static_cast<int>(node);
            draw -= counts[node];
        }
        throw std::runtime_error("could not select a placed service replica");
    }

    double network_delay(int source_node, int destination_node) {
        if (source_node >= 0 && destination_node >= 0) {
            return workload_.node_communication_latencies[
                source_node
            ][destination_node];
        }
        if (!workload_.same_node_one_way_latency.has_value()) return 0.0;
        std::bernoulli_distribution cross_node(
            workload_.unplaced_cross_node_probability
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
        double node_multiplier = 1.0;
        if (visit->node >= 0) {
            node_multiplier = workload_.node_execution_time_multipliers[
                visit->node
            ];
        }
        return sample(service_time(visit->request, visit->service))
            * input_service_time_multipliers_[visit->service]
            * node_multiplier;
    }

    int total_waiting(int service) const {
        std::size_t total = 0;
        for (const auto& queue : admission_waiting_[service]) total += queue.size();
        for (const auto& queue : waiting_[service]) total += queue.size();
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

    void admit(Visit* visit, double now) {
        const int service = visit->service;
        const int queue = queue_index(visit);
        const int limit = concurrency_capacity(service, queue);
        if (
            (limit == 0 || active_concurrency_by_queue_[service][queue] < limit)
            && admission_waiting_[service][queue].empty()
        ) {
            ++active_concurrency_by_queue_[service][queue];
            ++active_concurrency_[service];
            if (
                limit > 0
                && active_concurrency_by_queue_[service][queue] > limit
            ) {
                throw std::runtime_error("active service concurrency exceeds capacity");
            }
            queue_for_processing(visit, now);
        } else {
            admission_waiting_[service][queue].push_back(visit);
            record_service_state(service, now);
        }
    }

    void finish_or_delay(Request* request, double now) {
        const auto& request_class = workload_.request_classes[
            request->request_class
        ];
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

    void release_concurrency_and_destroy(Visit* visit, double now) {
        const int service = visit->service;
        const int queue = queue_index(visit);
        if (collect_service_samples_) {
            service_response_times_[service].push_back(
                now - visit->arrival_time
            );
        }
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
        const std::size_t ownership_index = visit->ownership_index;
        visits_[ownership_index].reset();
        free_visit_slots_.push_back(ownership_index);
    }

    void schedule_child(Visit* parent, int child_service, double now) {
        Visit* child_visit = create_visit(
            parent->request,
            child_service,
            parent,
            now
        );
        ++parent->pending_children;
        const double outbound_delay = network_delay(
            parent->node,
            child_visit->node
        );
        child_visit->return_network_delay = (
            parent->node < 0 || child_visit->node < 0
                ? outbound_delay
                : network_delay(child_visit->node, parent->node)
        );
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
        schedule_child(parent, child_service, now);
        return true;
    }

    void complete_child_return(Visit* parent, double now) {
        if (parent == nullptr || parent->pending_children <= 0) {
            throw std::runtime_error("call-graph child count became negative");
        }
        --parent->pending_children;
        if (parent->pending_children != 0) return;
        const RequestClass& request_class = workload_.request_classes[
            parent->request->request_class
        ];
        if (
            request_class.call_execution == CallExecution::Sequential
            && start_next_sequential_child(parent, now)
        ) {
            return;
        }
        complete_call_visit(parent, now);
    }

    void complete_call_visit(Visit* visit, double now) {
        Request* request = visit->request;
        Visit* parent = visit->parent;
        const double return_network_delay = visit->return_network_delay;
        release_concurrency_and_destroy(visit, now);
        if (parent != nullptr) {
            const double return_time = now + return_network_delay;
            if (return_time == now) {
                complete_child_return(parent, now);
            } else {
                schedule(
                    return_time,
                    kUrgentPriority,
                    EventKind::CallReturn,
                    nullptr,
                    parent->service,
                    parent
                );
            }
        } else {
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
        } else {
            release_processing(visit, now);
        }
        ++processed_event_count_;
        ++completed_visit_count_;
        Request* request = visit->request;
        const RequestClass& request_class = workload_.request_classes[
            request->request_class
        ];
        if (request_class.flow_kind == FlowKind::Calls) {
            for (const WorkflowVisit& child : request_class.calls[service]) {
                const std::int64_t count = sample_workflow_visits(
                    child.mean_visits
                );
                for (std::int64_t occurrence = 0; occurrence < count; ++occurrence) {
                    if (request_class.call_execution == CallExecution::Parallel) {
                        schedule_child(visit, child.service, now);
                    } else {
                        visit->sequential_children.push_back(child.service);
                    }
                }
            }
            if (
                request_class.call_execution == CallExecution::Sequential
                && start_next_sequential_child(visit, now)
            ) {
                return;
            }
            if (visit->pending_children == 0) {
                complete_call_visit(visit, now);
            }
            return;
        }

        const int target = choose_route(request, service);
        if (target >= 0) {
            Visit* next = create_visit(request, target, nullptr, now);
            const double arrival_time = now + network_delay(
                visit->node,
                next->node
            );
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
        release_concurrency_and_destroy(visit, now);
        if (target < 0) {
            finish_or_delay(request, now);
        }
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
        if (
            !admission_waiting_[service][queue].empty()
            && (
                limit == 0
                || active_concurrency_by_queue_[service][queue] < limit
            )
        ) {
            Visit* visit = admission_waiting_[service][queue].front();
            admission_waiting_[service][queue].pop_front();
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

    void request_timeout(Request* request, double now) {
        ++processed_event_count_;
        request->deadline_processed = true;
        if (!request->server_completed) {
            record_request_outcome(request, now, true);
            return;
        }
        release_request(request);
    }

    void final_exit(Request* request, double now) {
        ++completed_request_count_;
        request->server_completed = true;
        record_request_outcome(request, now, false);
        recorder_.final_exit(now);
        const double timeout = workload_.request_classes[
            request->request_class
        ].end_to_end_timeout;
        if (timeout == 0.0 || request->deadline_processed) {
            release_request(request);
        }
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
            for (std::size_t i = 1; i < result.samples.size(); ++i) {
                const double interval = result.samples[i] - result.samples[i - 1];
                if (interval <= 0.0) {
                    throw std::runtime_error(
                        "arrival_trace timestamps must be strictly increasing"
                    );
                }
                intervals.push_back(interval);
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

Workload parse_workload(json_t* raw) {
    Workload workload;
    workload.name = json_text(raw, "name");
    json_t* classes = required(raw, "request_classes");
    if (!json_is_array(classes) || json_array_size(classes) == 0) {
        throw std::runtime_error("workload must contain request classes");
    }
    const std::size_t request_class_count = json_array_size(classes);
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
            } else {
                throw std::runtime_error(
                    "unsupported replica_queue_model: " + model
                );
            }
        }
        service.capacity = static_cast<int>(capacity);
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
        workload.services.push_back(std::move(service));
    }

    json_array_foreach(classes, index, item) {
        RequestClass request_class;
        request_class.name = json_text(item, "name");
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
        workload.unplaced_cross_node_probability = json_number(
            raw_communication,
            "unplaced_cross_node_probability"
        );
        if (workload.unplaced_cross_node_probability < 0.0
            || workload.unplaced_cross_node_probability > 1.0) {
            throw std::runtime_error(
                "unplaced_cross_node_probability must lie in [0, 1]"
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

    json_t* raw_placement = json_object_get(raw, "placement_model");
    if (raw_placement != nullptr) {
        if (!json_is_object(raw_placement)) {
            throw std::runtime_error("placement_model must be an object");
        }
        json_t* raw_node_names = required(raw_placement, "node_names");
        const std::size_t node_count = json_array_size(raw_node_names);
        if (!json_is_array(raw_node_names) || node_count == 0) {
            throw std::runtime_error("placement_model needs at least one node");
        }
        std::size_t node_index;
        json_t* raw_node_name;
        json_array_foreach(raw_node_names, node_index, raw_node_name) {
            if (!json_is_string(raw_node_name)
                || std::string(json_string_value(raw_node_name)).empty()) {
                throw std::runtime_error("placement node names must be nonempty strings");
            }
        }

        json_t* raw_counts = required(raw_placement, "service_replica_counts");
        if (!json_is_array(raw_counts)
            || json_array_size(raw_counts) != workload.services.size()) {
            throw std::runtime_error(
                "service_replica_counts must have one row per service"
            );
        }
        std::size_t service_index;
        json_t* raw_count_row;
        json_array_foreach(raw_counts, service_index, raw_count_row) {
            if (!json_is_array(raw_count_row)
                || json_array_size(raw_count_row) != node_count) {
                throw std::runtime_error(
                    "service replica count row has the wrong node count"
                );
            }
            std::vector<int> count_row;
            int total = 0;
            json_t* raw_count;
            json_array_foreach(raw_count_row, node_index, raw_count) {
                if (!json_is_integer(raw_count)
                    || json_integer_value(raw_count) < 0
                    || json_integer_value(raw_count) > 1'000'000) {
                    throw std::runtime_error(
                        "placed replica counts must be non-negative integers"
                    );
                }
                const int count = static_cast<int>(json_integer_value(raw_count));
                count_row.push_back(count);
                total += count;
            }
            if (total != workload.services[service_index].replicas) {
                throw std::runtime_error(
                    "placed replica total does not match service replicas"
                );
            }
            workload.service_replica_counts_by_node.push_back(
                std::move(count_row)
            );
        }

        json_t* raw_latencies = required(
            raw_placement,
            "communication_latency_seconds"
        );
        if (!json_is_array(raw_latencies)
            || json_array_size(raw_latencies) != node_count) {
            throw std::runtime_error(
                "communication latency matrix has the wrong row count"
            );
        }
        json_t* raw_latency_row;
        json_array_foreach(raw_latencies, node_index, raw_latency_row) {
            if (!json_is_array(raw_latency_row)
                || json_array_size(raw_latency_row) != node_count) {
                throw std::runtime_error(
                    "communication latency row has the wrong length"
                );
            }
            std::vector<double> latency_row;
            std::size_t destination_index;
            json_t* raw_latency;
            json_array_foreach(
                raw_latency_row,
                destination_index,
                raw_latency
            ) {
                if (!json_is_number(raw_latency)) {
                    throw std::runtime_error("node latency must be numeric");
                }
                const double latency = json_number_value(raw_latency);
                if (!std::isfinite(latency) || latency < 0.0) {
                    throw std::runtime_error(
                        "node latency must be finite and non-negative"
                    );
                }
                latency_row.push_back(latency);
            }
            workload.node_communication_latencies.push_back(
                std::move(latency_row)
            );
        }

        json_t* raw_multipliers = required(
            raw_placement,
            "execution_time_multipliers"
        );
        if (!json_is_array(raw_multipliers)
            || json_array_size(raw_multipliers) != node_count) {
            throw std::runtime_error(
                "execution_time_multipliers has the wrong node count"
            );
        }
        json_t* raw_multiplier;
        json_array_foreach(raw_multipliers, node_index, raw_multiplier) {
            if (!json_is_number(raw_multiplier)) {
                throw std::runtime_error("node execution multiplier must be numeric");
            }
            const double multiplier = json_number_value(raw_multiplier);
            if (!std::isfinite(multiplier) || multiplier <= 0.0) {
                throw std::runtime_error(
                    "node execution multiplier must be finite and positive"
                );
            }
            workload.node_execution_time_multipliers.push_back(multiplier);
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
        json_object_set_new(
            item,
            "placement_model_applied",
            json_boolean(!workload.service_replica_counts_by_node.empty())
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
