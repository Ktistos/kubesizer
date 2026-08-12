#include <jansson.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <deque>
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

constexpr int kProtocolVersion = 3;
constexpr int kUrgentPriority = 0;
constexpr int kNormalPriority = 1;

enum class EventKind { ExternalArrival, Admit, ServiceCompletion, Grant, Resume };
enum class RunMode { FixedHorizon, MeasurementDrain };

struct Service {
    std::string name;
    int capacity{};
    double rate{};
};

struct Route {
    int target{-1};
    double probability{};
};

struct RequestClass {
    std::string name;
    double concurrency_weight{};
    std::vector<std::vector<Route>> routes;
};

struct ExternalStream {
    int request_class{};
    int entry_service{};
    double rate{};
};

struct Workload {
    std::string name;
    std::vector<Service> services;
    std::vector<RequestClass> request_classes;
    std::vector<ExternalStream> external_streams;
};

struct Request {
    std::int64_t id{};
    int request_class{};
    double external_entry_time{};
    std::size_t ownership_index{};
    bool latency_eligible{};
    bool measurement_tagged{};
};

struct Event {
    double time{};
    int priority{};
    std::uint64_t sequence{};
    EventKind kind{};
    Request* request{};
    int index{};
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
            const std::size_t index = std::min(
                static_cast<std::size_t>(current / width_),
                areas_.size() - 1
            );
            const double bucket_end = static_cast<double>(index + 1) * width_;
            const double segment_end = std::min(interval_end, bucket_end);
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
        busy_(workload.services.size()),
        recorder_(capacities(workload), capture_history, bucket_width, horizon),
        service_response_times_(workload.services.size()),
        request_class_response_times_(workload.request_classes.size()),
        request_class_arrival_times_(workload.request_classes.size()),
        measured_request_counts_(workload.request_classes.size()) {}

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
    const std::vector<std::int64_t>& measured_request_counts() const {
        return measured_request_counts_;
    }
    std::int64_t tagged_requests_in_flight() const {
        return tagged_requests_in_flight_;
    }
    std::int64_t seed() const { return seed_; }

  private:
    static std::vector<int> capacities(const Workload& workload) {
        std::vector<int> result;
        for (const Service& service : workload.services) result.push_back(service.capacity);
        return result;
    }

    void schedule(double time, int priority, EventKind kind, Request* request, int index) {
        future_events_.push({time, priority, sequence_++, kind, request, index});
    }

    double exponential(double rate) {
        std::uniform_real_distribution<double> uniform(0.0, 1.0);
        const double draw = uniform(random_);
        return -std::log(1.0 - draw) / rate;
    }

    void schedule_next_external(int stream, double now) {
        schedule(
            now + exponential(workload_.external_streams[stream].rate),
            kNormalPriority,
            EventKind::ExternalArrival,
            nullptr,
            stream
        );
    }

    int choose_route(Request* request, int service) {
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
            case EventKind::Admit: admit(event.request, event.index, event.time); break;
            case EventKind::ServiceCompletion:
                service_completion(event.request, event.index, event.time); break;
            case EventKind::Grant: grant(event.index, event.time); break;
            case EventKind::Resume: resume(event.request, event.index, event.time); break;
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
            eligible, tagged
        });
        Request* request = requests_[ownership_index].get();
        ++processed_event_count_;
        ++external_arrival_count_;
        if (tagged) {
            ++tagged_requests_in_flight_;
            ++measured_request_counts_[stream.request_class];
        }
        recorder_.external_arrival(now);
        schedule(now, kUrgentPriority, EventKind::Admit, request, stream.entry_service);
        schedule_next_external(stream_id, now);
    }

    void admit(Request* request, int service, double now) {
        current_visit_start_[request] = now;
        if (busy_[service] < workload_.services[service].capacity
            && waiting_[service].empty()) {
            ++busy_[service];
            recorder_.service_state(
                service, now, static_cast<int>(waiting_[service].size()), busy_[service]
            );
            schedule(
                now + exponential(workload_.services[service].rate),
                kNormalPriority,
                EventKind::ServiceCompletion,
                request,
                service
            );
        } else {
            waiting_[service].push_back(request);
            recorder_.service_state(
                service, now, static_cast<int>(waiting_[service].size()), busy_[service]
            );
        }
    }

    void service_completion(Request* request, int service, double now) {
        ++processed_event_count_;
        ++completed_visit_count_;
        if (collect_service_samples_) {
            service_response_times_[service].push_back(
                now - current_visit_start_.at(request)
            );
        }
        current_visit_start_.erase(request);
        const int target = choose_route(request, service);
        if (target < 0) {
            ++completed_request_count_;
            const double latency = now - request->external_entry_time;
            if (request->latency_eligible && collect_request_class_samples_) {
                request_class_arrival_times_[request->request_class].push_back(
                    request->external_entry_time
                );
                request_class_response_times_[request->request_class].push_back(latency);
            }
            if (request->measurement_tagged) {
                if (tagged_requests_in_flight_ <= 0) {
                    throw std::runtime_error("tagged request count became negative");
                }
                --tagged_requests_in_flight_;
            }
            recorder_.final_exit(now);
            const std::size_t ownership_index = request->ownership_index;
            requests_[ownership_index].reset();
            free_request_slots_.push_back(ownership_index);
        } else {
            schedule(now, kUrgentPriority, EventKind::Admit, request, target);
        }

        --busy_[service];
        if (busy_[service] < 0) throw std::runtime_error("busy replicas became negative");
        recorder_.service_state(
            service, now, static_cast<int>(waiting_[service].size()), busy_[service]
        );
        schedule(now, kNormalPriority, EventKind::Grant, nullptr, service);
    }

    void grant(int service, double now) {
        if (!waiting_[service].empty()
            && busy_[service] < workload_.services[service].capacity) {
            Request* request = waiting_[service].front();
            waiting_[service].pop_front();
            ++busy_[service];
            schedule(now, kNormalPriority, EventKind::Resume, request, service);
        }
    }

    void resume(Request* request, int service, double now) {
        recorder_.service_state(
            service, now, static_cast<int>(waiting_[service].size()), busy_[service]
        );
        schedule(
            now + exponential(workload_.services[service].rate),
            kNormalPriority,
            EventKind::ServiceCompletion,
            request,
            service
        );
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
    std::priority_queue<Event, std::vector<Event>, LaterEvent> future_events_;
    std::uint64_t sequence_{};
    std::vector<std::deque<Request*>> waiting_;
    std::vector<int> busy_;
    StateRecorder recorder_;
    std::vector<std::unique_ptr<Request>> requests_;
    std::vector<std::size_t> free_request_slots_;
    std::unordered_map<Request*, double> current_visit_start_;
    std::vector<std::vector<double>> service_response_times_;
    std::vector<std::vector<double>> request_class_response_times_;
    std::vector<std::vector<double>> request_class_arrival_times_;
    std::vector<std::int64_t> measured_request_counts_;
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
    json_t* services = required(raw, "services");
    if (!json_is_array(services) || json_array_size(services) == 0) {
        throw std::runtime_error("workload must contain services");
    }
    std::size_t index;
    json_t* item;
    json_array_foreach(services, index, item) {
        const std::int64_t capacity = required_integer(item, "capacity");
        const double rate = json_number(item, "service_rate");
        if (capacity <= 0 || capacity > 1'000'000 || rate <= 0.0) {
            throw std::runtime_error("service capacity and rate must be positive");
        }
        workload.services.push_back({
            json_text(item, "name"), static_cast<int>(capacity), rate
        });
    }

    json_t* classes = required(raw, "request_classes");
    if (!json_is_array(classes) || json_array_size(classes) == 0) {
        throw std::runtime_error("workload must contain request classes");
    }
    json_array_foreach(classes, index, item) {
        RequestClass request_class;
        request_class.name = json_text(item, "name");
        request_class.concurrency_weight = json_number(item, "concurrency_weight");
        if (request_class.concurrency_weight < 0.0
            || request_class.concurrency_weight > 1.0) {
            throw std::runtime_error("concurrency weights must lie in [0, 1]");
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
        const double rate = json_number(item, "arrival_rate");
        if (request_class < 0
            || request_class >= static_cast<std::int64_t>(workload.request_classes.size())
            || entry_service < 0
            || entry_service >= static_cast<std::int64_t>(workload.services.size())
            || rate <= 0.0) {
            throw std::runtime_error("invalid external stream");
        }
        workload.external_streams.push_back({
            static_cast<int>(request_class), static_cast<int>(entry_service), rate
        });
    }

    std::vector<double> configured_class_rates(
        workload.request_classes.size(),
        0.0
    );
    double configured_total_rate = 0.0;
    for (const ExternalStream& stream : workload.external_streams) {
        configured_class_rates[stream.request_class] += stream.rate;
        configured_total_rate += stream.rate;
    }
    if (configured_total_rate > 0.0) {
        for (std::size_t request_class = 0;
             request_class < workload.request_classes.size();
             ++request_class) {
            if (workload.request_classes[request_class].concurrency_weight > 0.0
                && configured_class_rates[request_class] <= 0.0) {
                throw std::runtime_error(
                    "a positively weighted request class has no external stream"
                );
            }
        }
        for (ExternalStream& stream : workload.external_streams) {
            const double target_class_rate = configured_total_rate
                * workload.request_classes[stream.request_class].concurrency_weight;
            stream.rate *= target_class_rate
                / configured_class_rates[stream.request_class];
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
        json_array_append_new(output, json_integer(service.capacity));
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
    int active_requests_at_end{};
    std::vector<int> queue_lengths_at_end;
    std::vector<int> busy_replicas_at_end;
    std::int64_t tagged_requests_in_flight_at_end{};
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
        output.active_requests_at_end = kernel.recorder().active();
        output.queue_lengths_at_end = kernel.recorder().queues();
        output.busy_replicas_at_end = kernel.recorder().busy();
        output.tagged_requests_in_flight_at_end = kernel.tagged_requests_in_flight();
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
        std::vector<std::int64_t> completed_measured_counts;
        completed_measured_counts.reserve(output.latencies.size());
        for (const auto& observations : output.latencies) {
            completed_measured_counts.push_back(
                static_cast<std::int64_t>(observations.size())
            );
        }
        json_object_set_new(
            item, "completed_measured_request_counts",
            class_count_map(workload, completed_measured_counts)
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
