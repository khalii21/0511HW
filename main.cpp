#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <functional>
#include <chrono>

struct task_t { void operator()(); };
struct th_pool_t {
  using f_t = std::function< void(void) >;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();
  void submit(f_t f);
  void pause();
  void resume();
  void wait();
private:
  void execute();
  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable done_;
  bool stop_ { false };
  bool paused_ { false };
  unsigned int active_ { 0 };
  std::vector< std::thread > ths_;
};

void th_pool_t::submit(f_t f)
{
  mx_.lock();

  if (stop_)
  {
    mx_.unlock();
    return;
  }

  tasks_.push(std::move(f));

  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]() { execute(); };
  for (; k; --k)
  {
    ths_.emplace_back(thf);
  }
}

th_pool_t::~th_pool_t()
{
  mx_.lock();

  stop_ = true;

  mx_.unlock();
  cd_.notify_all();
  for (auto && th: ths_)
  {
    th.join();
  }
}

void th_pool_t::execute()
{
  std::unique_lock lck(mx_);
  do
  {
    auto proceed = [&]()
    {
      return (!tasks_.empty() && !paused_) || stop_;
    };
    cd_.wait(lck, proceed);
    if (stop_ && tasks_.empty())
    {
      continue;
    }
    if (paused_ && !stop_)
    {
      continue;
    }
    auto task = std::move(tasks_.front());
    tasks_.pop();
    ++active_;
    lck.unlock();
    try
    {
      task();
    } catch (...) {}
    lck.lock();
    --active_;
    if (tasks_.empty() && active_ == 0)
    {
      done_.notify_all();
    }
  }  while (!stop_ || !tasks_.empty());
}

void th_pool_t::wait()
{
  std::unique_lock lck(mx_);

  auto condwait = [this]()
  {
    return tasks_.empty() && active_ == 0;
  };

  done_.wait(lck, condwait);
}

void th_pool_t::pause()
{
  std::lock_guard lck(mx_);
  paused_ = true;
}

void th_pool_t::resume()
{
  {
    std::lock_guard lck(mx_);
    paused_ = false;
  }

  cd_.notify_all();
}

int main()
{}
