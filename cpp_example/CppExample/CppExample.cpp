
#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iomanip>
#include <locale>
#include <sstream>
#include <unordered_map>
#include <optional>

struct Quote
{
	long event_id;
	int instrument;
	long price;
};
std::ostream & operator<<(std::ostream &out, const Quote &q)
{
	out << "Quote{event_id=" << q.event_id << " instrument=" << q.instrument << " price=" << q.price << "}";
	return out;
}

struct Theo
{
	long event_id;
	int underlying_instrument;
	int option_instrument;
	double tv;
};
std::ostream & operator<<(std::ostream &out, const Theo &t)
{
	return (out << "Theo{event_id=" << t.event_id << " underlying_instrument=" << t.underlying_instrument << " option_instrument=" << t.option_instrument << " tv=" << t.tv << "}");
}

template <typename T>
class BlockingQueue
{
private:
	std::queue<T> m_queue;
	std::mutex m_mutex;
	std::condition_variable m_cond;
	constexpr static inline int BUFFER_SIZE = 5000;

public:
	T take()
	{
		std::unique_lock<std::mutex> mlock(m_mutex);
		while (m_queue.empty())
		{
			m_cond.wait(mlock);
		}
		auto val = m_queue.front();
		m_queue.pop();
		mlock.unlock();
		m_cond.notify_one();
		return val;
	}

	void push(const T& item)
	{
		std::unique_lock<std::mutex> mlock(m_mutex);
		while (m_queue.size() >= BUFFER_SIZE)
		{
			m_cond.wait(mlock);
		}
		m_queue.push(item);
		mlock.unlock();
		m_cond.notify_one();

	}

	BlockingQueue() = default;
	BlockingQueue(const BlockingQueue&) = delete;
	BlockingQueue& operator=(const BlockingQueue&) = delete;
};

std::atomic_long event_id = 1;

class Counter
{
private:
	std::atomic_long m_num_events = 0;
	std::chrono::system_clock::time_point m_start_time;
	constexpr static long ONE_SECOND = 1000 * 1000 * 1000;
	std::mutex m_mutex;

public:
	Counter()
	{
		m_start_time = std::chrono::system_clock::now();
	}

	void count()
	{
		const auto t1 = std::chrono::system_clock::now();
		const auto num_evts = m_num_events += 1;
		const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - m_start_time).count();
		if (elapsed_ns > ONE_SECOND)
		{
			std::unique_lock mlock(m_mutex);
			const auto seconds_elapsed = elapsed_ns / ONE_SECOND;
			const auto events_per_second = num_evts * seconds_elapsed;
			std::stringstream ss;
			ss.imbue(std::locale(""));
			ss << std::fixed << events_per_second;
			std::cout << "Handled " << ss.str() << " events/sec\n";
			m_num_events = 0;
			m_start_time = t1;
			mlock.unlock();
		}
		
	}


};





class QuoteSource {
private:
	int m_instrument_start;
	int m_instrument_end;
	std::shared_ptr<BlockingQueue<Quote>> m_calc_queue;

public:
	QuoteSource(int instrument_start, int instrument_end, std::shared_ptr<BlockingQueue<Quote>> calc_queue) :
		m_instrument_start(instrument_start), m_instrument_end(instrument_end), m_calc_queue(std::move(calc_queue))
	{
	}

	void start()
	{
		auto f = [&]()
		{
			while (true)
			{
				for (auto instrument_id = m_instrument_start; instrument_id <= m_instrument_end; instrument_id++)
				{
					m_calc_queue->push(Quote{ ++event_id,instrument_id,instrument_id });
				}
			}
		};
		std::thread mkt_data_thread{ f };
		mkt_data_thread.detach();
	}
};



class TheoCalculator {
private:
	std::shared_ptr<BlockingQueue<Quote>> m_quote_queue;
	std::shared_ptr<BlockingQueue<Theo>> m_dest_queue;
	std::unordered_map<int, Quote> m_quote_map;

public:
	TheoCalculator(std::shared_ptr<BlockingQueue<Quote>> quote_queue, std::shared_ptr<BlockingQueue<Theo>> dest_queue) :
	                   m_quote_queue(std::move(quote_queue)), m_dest_queue(std::move(dest_queue))
	{
	}



	void start()
	{
		auto f = [&]()
		{
			while (true)
			{
				const auto& quote = m_quote_queue->take();
				const auto instrument_id = quote.instrument;
				m_quote_map.insert_or_assign(instrument_id, quote);
				std::optional<Quote> underlying_quote = std::nullopt;
				std::optional<Quote> option_quote = std::nullopt;
				if (instrument_id > 0) {
					underlying_quote = quote;
					const auto f = m_quote_map.find(-instrument_id);
					if (f != m_quote_map.end())
					{
						option_quote = f->second;
					}
				}
				else {
					option_quote = quote;
					const auto f = m_quote_map.find(-instrument_id);
					if (f != m_quote_map.end())
					{
						underlying_quote = f->second;
					}
				}
				if (option_quote && underlying_quote) {
					const auto tv = (underlying_quote.value().price + option_quote.value().price) / 4.0;
					m_dest_queue->push(Theo{ ++event_id, underlying_quote->instrument, option_quote->instrument, tv });
				
				}
			}
		};
		std::thread theo_thread{ f };
		theo_thread.detach();
	}
};

class TradingStrategy
{
private:
	std::shared_ptr<BlockingQueue<Theo>> m_theo_queue;
	std::shared_ptr<Counter> m_counter;

public:

	TradingStrategy(std::shared_ptr<BlockingQueue<Theo>> theo_queue, std::shared_ptr<Counter> counter)
		: m_theo_queue(std::move(theo_queue)), m_counter(std::move(counter)) {}

	void start()
	{
		auto f = [&]()
		{
			while (true)
			{
				const auto theo = m_theo_queue->take();
				m_counter->count();
			}
		};
		std::thread theo_thread{ f };
		theo_thread.detach();
	}
};

void run_sample_one()
{
	auto counter = std::make_shared<Counter>();
	auto quote_queue_1 = std::make_shared<BlockingQueue<Quote>>();
	auto quote_queue_2 = std::make_shared<BlockingQueue<Quote>>();
	auto theo_queue_1 = std::make_shared<BlockingQueue<Theo>>();
	auto theo_queue_2 = std::make_shared<BlockingQueue<Theo>>();

	TheoCalculator calculator1{ quote_queue_1, theo_queue_1 };
	TheoCalculator calculator2{ quote_queue_2, theo_queue_2 };

	QuoteSource und_quote_source1{ 1, 100000, quote_queue_1 };
	QuoteSource und_quote_source2{ 100001, 200000, quote_queue_2 };
	QuoteSource opt_quote_source1{ -100000, -1, quote_queue_1 };
	QuoteSource opt_quote_source2{ -200000, -100001, quote_queue_2 };

	TradingStrategy strategy1{ theo_queue_1, counter};
	TradingStrategy strategy2{ theo_queue_2, counter };

	strategy1.start();
	strategy2.start();
	calculator1.start();
	calculator2.start();
	und_quote_source1.start();
	und_quote_source2.start();
	opt_quote_source1.start();
	opt_quote_source2.start();
	std::cout << "Started queue example";
	while (true) {
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
}

int main()
{
	std::cout << "Running sample\n";
	run_sample_one();


}
