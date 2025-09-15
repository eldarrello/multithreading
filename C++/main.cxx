#include <thread>
#include <queue>
#include <mutex>
#include <vector>
#include <iostream>

/**
Simulate infinitely fast input
*/
class Generator {
  unsigned int count_ = 1;

  public:

  unsigned int getValue() {
    return count_++;
  }
};

/**
Find 3 greatest unique values
*/
class Task {
  std::vector<unsigned int> values;

  public:

  Task() : values(3, 0) {}
  void add(unsigned int value) {
    if (value <= values[2] || value == values[1] || value == values[0]) {
      return;
    }
    if (value > values[0]) {
      values[2] = values[1];
      values[1] = values[0];
      values[0] = value;
    } else if (value > values[1]) {
      values[2] = values[1];
      values[1] = value;
    } else if (value > values[2]) {
      values[2] = value;
    }
  }
  const std::vector<unsigned int>& getValues() const {
    return values;
  }
};

class LargestValuesFinderV1 {
  static const int BATCH_SIZE = 10000;

  std::mutex m;
  std::queue<std::vector<unsigned int>> q;
  std::condition_variable cv;
  std::vector<std::thread> workerThreads;
  std::thread inputThread;
  bool stopped = false;
  Task global_task;
  size_t max_queue_size = 0;                // For debug

  public:

  void run(int thread_count) {
    for (int i = 0; i < thread_count; ++i) {
      workerThreads.emplace_back(&LargestValuesFinderV1::workerThreadFunc, this);
    }
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();    
    std::thread inputThread(&LargestValuesFinderV1::inputThreadFunc, this);
    inputThread.join();
    
    for (auto& thread : workerThreads) {
      thread.join();
    }
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << typeid(this).name() << " time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << std::endl;

    for (auto i: global_task.getValues()) {
      std::cout << i << std::endl;
    }
    //std::cout << "Queue size: " << max_queue_size;
  }

  private:

  void inputThreadFunc() {
    Generator generator;
    std::vector<unsigned int> values;
    values.reserve(BATCH_SIZE);
    while(true) {
      unsigned int value = generator.getValue();
      values.push_back(value);
      
      if (values.size() == BATCH_SIZE || value == 0) {
        std::unique_lock l(m);
        q.push(std::move(values));
        if (q.size() > max_queue_size) {
          max_queue_size = q.size();
        }
        values.reserve(BATCH_SIZE);
        cv.notify_one();
        if (value == 0) {
          break;
        }
      }
    }
    stopped = true;
    cv.notify_all();
  }
  void workerThreadFunc() {
    Task task;
    while(true) {
      std::vector<unsigned int> values;
      std::unique_lock l(m);
      cv.wait(l, [&] {return !q.empty() || stopped;});
      if (q.empty() && stopped) {
        break;
      }
      values = std::move(q.front());
      q.pop();
      l.unlock();
      for (size_t i = 0; i < values.size(); i++) {
        task.add(values[i]);
      }
    }
    std::unique_lock l(m);
    for (auto i: task.getValues()) {
      global_task.add(i);
    }
    l.unlock();
  }
};

class LargestValuesFinderV2 {
  static const int BATCH_SIZE = 10000;

  struct WorkerContext {
    WorkerContext():
      values(BATCH_SIZE) {}
    std::vector<unsigned int> values;
    volatile int filled_count = 0;
    Task task;
  };

  std::vector<std::thread> worker_threads_;
  std::vector<WorkerContext> workers_;
  volatile bool stopped_ = false;

  public:

  void run(int thread_count) {
    workers_.resize(thread_count);
    for (int i = 0; i < thread_count; ++i) {
      worker_threads_.emplace_back(&LargestValuesFinderV2::workerThreadFunc, this, i);
    }
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::thread inputThread(&LargestValuesFinderV2::inputThreadFunc, this);
    inputThread.join();
    
    // Wait for all worker threads to finish
    for (auto& thread : worker_threads_) {
      thread.join();
    }
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << typeid(this).name() << " time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << std::endl;

    Task task;
    for (auto& worker: workers_) {
      for (auto i: worker.task.getValues()) {
        task.add(i);
      }
    }
    std::ranges::for_each (task.getValues().begin(), task.getValues().end(), [] (const unsigned int& i) {std::cout << i << std::endl;});
  }

  private:

  void inputThreadFunc() {
    Generator generator;
    int worker_index = 0;
    while (true) {
      if (workers_[worker_index].filled_count == 0) {
        for (int k = 0; k < BATCH_SIZE; k++) {
          unsigned int value = generator.getValue();
          if (value > 0) {  [[likely]]
            workers_[worker_index].values[k] = value;
          } else {
            workers_[worker_index].filled_count = k;
            stopped_ = true;
            return;
          }
        }
        workers_[worker_index].filled_count = BATCH_SIZE;
      } else {
        worker_index++;
        worker_index %= workers_.size();
      }
    }
  }
  void workerThreadFunc(int worker_id) {
    while(true) {
      if (workers_[worker_id].filled_count > 0 || stopped_) {
        auto& task = workers_[worker_id].task;
        for (int i = 0; i < workers_[worker_id].filled_count; i++) {
          task.add(workers_[worker_id].values[i]);
        }
        if (stopped_) {
          return;
        }
        workers_[worker_id].filled_count = 0;
      }
    }
  }
};

int main() {
  unsigned int thread_count = std::thread::hardware_concurrency() - 1;  // 1 - input thread, main is idle.
  if (thread_count < 1) {
      thread_count = 1;
  }
  LargestValuesFinderV1 finderV1;
  finderV1.run(thread_count);
  LargestValuesFinderV2 finderV2;
  finderV2.run(thread_count);
  
  return 0;
}
