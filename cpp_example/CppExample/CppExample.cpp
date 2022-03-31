

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
#include "phmap.h"
#include <inttypes.h>


std::atomic_long event_id = 1;
struct Quote
{
	long event_id;
	int instrument;
	long price;

	Quote() = default;
	Quote(long e, int i, long p) : event_id(e), instrument(i), price(p) {}
	Quote(const Quote& other) : event_id(other.event_id), instrument(other.instrument), price(other.price) {}
	Quote(Quote&& other) noexcept: event_id(std::move(other.event_id)), instrument(std::move(other.instrument)), price(std::move(other.price)) {}
	Quote & operator=(const Quote& other)
	{
		event_id = other.event_id;
		instrument = other.instrument;
		price = other.price;
		return *this;
	}
	Quote & operator=(const Quote&& other) noexcept
	{
		event_id = std::move(other.event_id);
		instrument = std::move(other.instrument);
		price = std::move(other.price);
		return *this;
	}

};
std::mutex print_mutex;
std::ostream & operator<<(std::ostream &out, const Quote &q)
{
	std::unique_lock print_lock(print_mutex);
	out << "Quote{event_id=" << q.event_id << " instrument=" << q.instrument << " price=" << q.price << "}";
	return out;
}

struct Theo
{
	long event_id;
	int underlying_instrument;
	int option_instrument;
	double tv;

	Theo() = default;
	Theo(long e, int u, int o, double t) : event_id(e), underlying_instrument(u), option_instrument(o), tv(t) {}
	Theo(const Theo & other) : event_id(other.event_id), underlying_instrument(other.underlying_instrument), option_instrument(other.option_instrument), tv(other.tv) {}
	Theo(Theo && other) noexcept : event_id(std::move(other.event_id)), underlying_instrument(std::move(other.underlying_instrument)), option_instrument(std::move(other.option_instrument)), tv(std::move(other.tv)) {}
	Theo& operator=(const Theo & other)
	{
		event_id = other.event_id;
		option_instrument = other.option_instrument;
		underlying_instrument = other.underlying_instrument;
		tv = other.tv;
		return *this;
	}
	Theo& operator=(const Theo && other) noexcept
	{
		event_id = std::move(other.event_id);
		option_instrument = std::move(other.option_instrument);
		underlying_instrument = std::move(other.underlying_instrument);
		tv = std::move(other.tv);
		return *this;
	}


};
std::ostream & operator<<(std::ostream &out, const Theo &t)
{
	std::unique_lock print_lock(print_mutex);
	out << "Theo{event_id=" << t.event_id << " underlying_instrument=" << t.underlying_instrument << " option_instrument=" << t.option_instrument << " tv=" << t.tv << "}";
	return out;
}


template <typename T>
class CoalescingQueue
{
private:
	//folly::ConcurrentHashMap<int, T> m_map;
	//libcuckoo::cuckoohash_map<uint32_t, T> m_map;
	phmap::parallel_flat_hash_map<int, T> m_map;

public:

	using hash_t = phmap::parallel_flat_hash_map<int32_t, T>;
	CoalescingQueue() : m_map(phmap::parallel_flat_hash_map<int, T>(32))
	{
		for (int i = -200000; i <= 200000; i++)
		{
			m_map.insert(hash_t::value_type(i, T{}));
		}
	}

	std::optional<T> take(const int instrument_id)
	{
		const auto f = m_map.find(instrument_id);
		if (f == m_map.end())
		{
			return std::nullopt;
		}
		return f->second;
	}


	void push(const T &item, const int id)
	{
		m_map.insert(hash_t::value_type(id, item));
		
	}
};


template <typename T>
class LockedCoalescingQueue
{
private:
	std::unordered_map<int, T> m_map;
	std::mutex m_mutex;

public:
	std::optional<T> take(const int instrument_id)
	{
		std::unique_lock map_lock(m_mutex);
		
		if (m_map.contains(instrument_id))
		{
			return m_map[instrument_id];
		}
		return std::nullopt;
	}

	void push(const T& item, const int id)
	{
		std::unique_lock map_lock(m_mutex);
		m_map.insert_or_assign(id, item);
	}
};


template <typename T>
class BlockingQueue
{
private:
	std::queue<T> m_queue;
	std::mutex m_mutex;
	std::condition_variable m_cond;
	constexpr static inline int BUFFER_SIZE = 5000;

public:
	std::optional<T> take(const int id=0)
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

	void push(const T item, const int id=0)
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


class Counter
{
private:
	std::atomic_long m_num_events = 0;
	std::chrono::system_clock::time_point m_start_time;
	constexpr static long ONE_SECOND = 1000 * 1000 * 1000;
	std::unordered_map<int, long> m_event_id_map;
	std::mutex m_counter_mutex;

public:
	Counter()
	{
		m_start_time = std::chrono::system_clock::now();
	}


	void count(const Theo& t)
	{
		//std::unique_lock counter_lock(m_counter_mutex);
		//if (m_event_id_map.contains(t.underlying_instrument) && t.event_id < m_event_id_map.at(t.underlying_instrument))
		//	return;
		//m_event_id_map.insert_or_assign(t.underlying_instrument, t.event_id);
		const auto t1 = std::chrono::system_clock::now();
		const auto num_evts = m_num_events += 1;
		const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - m_start_time).count();
		if (elapsed_ns > ONE_SECOND)
		{

			std::unique_lock counter_lock(m_counter_mutex);
			const auto seconds_elapsed = elapsed_ns / ONE_SECOND;
			const auto events_per_second = num_evts * seconds_elapsed;
			std::stringstream ss;
			ss.imbue(std::locale(""));
			ss << std::fixed << events_per_second;
			std::cout << "Handled " << ss.str() << " events/sec\n";
			m_num_events = 0;
			m_start_time = t1;
		}
	}

};

class QuoteAllocator
{
public:
	virtual Quote allocate(long event_id, int instrument_id, long price) = 0;
};


template <typename QuoteContainer>
class QuoteSource {
private:

	int m_instrument_start;
	int m_instrument_end;
	std::shared_ptr<QuoteContainer> m_calc_queue;


public:
	QuoteSource(int instrument_start, int instrument_end, std::shared_ptr<QuoteContainer> calc_queue) :
		m_instrument_start(instrument_start), m_instrument_end(instrument_end), m_calc_queue(std::move(calc_queue))
	{
	}

	void start()
	{
		auto f = [&]()
		{
			while (true)
			{
				for (auto instrument_id = m_instrument_start; instrument_id <= m_instrument_end; ++instrument_id)
				{
					auto q = Quote{ ++event_id,instrument_id,instrument_id };
					m_calc_queue->push(q, instrument_id);
				}
			}
		};
		std::thread mkt_data_thread{ f };
		mkt_data_thread.detach();
	}
};


template <typename QuoteContainer, typename TheoContainer>
class TheoCalculator {
private:

	std::shared_ptr<QuoteContainer> m_quote_queue;
	std::shared_ptr<TheoContainer> m_dest_queue;
	std::unordered_map<int, Quote> m_quote_map;
	int m_start_id;
	int m_end_id;

public:
	TheoCalculator(std::shared_ptr<QuoteContainer> quote_queue, std::shared_ptr<TheoContainer> dest_queue, int start_id = 0, int end_id = 0) :
		m_quote_queue(std::move(quote_queue)), m_dest_queue(std::move(dest_queue)), m_start_id(start_id), m_end_id(end_id)
	{
	}

	void start_poll()
	{
		const auto f = [&]()
		{
			while (true)
			{
				for (auto instrument_id = m_start_id; instrument_id <= m_end_id; ++instrument_id)
				{
					const auto underlying_quote = m_quote_queue->take(instrument_id);
					const auto option_quote = m_quote_queue->take(-instrument_id);
					if (underlying_quote && option_quote)
					{
						const auto tv = (underlying_quote.value().price + option_quote.value().price * .2) / 4.0;
						auto theo = Theo{ ++event_id, underlying_quote->instrument, option_quote->instrument, tv };
						m_dest_queue->push(theo, instrument_id);
					}
				}
			}
		};
		std::thread theo_thread{ f };
		theo_thread.detach();
	}


	void start()
	{
		auto f = [&]()
		{
			while (true)
			{
				const auto q = m_quote_queue->take();
				if (q)
				{
					const auto quote = q.value();
					const auto instrument_id = quote.instrument;
					m_quote_map[instrument_id] = quote;
					std::optional<Quote> underlying_quote = std::nullopt;
					std::optional<Quote> option_quote = std::nullopt;
					if (instrument_id > 0) {
						underlying_quote = quote;
						const auto itr = m_quote_map.find(-instrument_id);
						if (itr != m_quote_map.end())
						{
							option_quote = itr->second;
						}
					}
					else {
						option_quote = quote;
						const auto itr = m_quote_map.find(-instrument_id);
						if (itr != m_quote_map.end())
						{
							underlying_quote = itr->second;
						}
					}
					if (option_quote && underlying_quote) {
						const auto tv = (underlying_quote.value().price + option_quote.value().price) / 4.0;
						m_dest_queue->push(Theo{ ++event_id, underlying_quote.value().instrument, option_quote.value().instrument, tv });

					}
				}
			}

		};
		std::thread theo_thread{ f };
		theo_thread.detach();
	}
};

template <typename TheoContainer>
class TradingStrategy
{
private:
	std::shared_ptr<TheoContainer> m_theo_queue;
	std::shared_ptr<Counter> m_counter;
	int m_start_id;
	int m_end_id;

public:

	TradingStrategy(std::shared_ptr<TheoContainer> theo_queue, std::shared_ptr<Counter> counter, const int start_id=0, const int end_id=0)
		: m_theo_queue(std::move(theo_queue)), m_counter(std::move(counter)), m_start_id(start_id), m_end_id(end_id) {}

	void start_poll()
	{
		auto f = [&]()
		{
			while (true)
			{
				for (auto instrument_id = m_start_id; instrument_id <= m_end_id; ++instrument_id)
				{
					const auto theo = m_theo_queue->take(instrument_id);
					if (theo) {
						m_counter->count(theo.value());
					}
				}
				
			}
		};
		std::thread theo_thread{ f };
		theo_thread.detach();
	}

	void start()
	{
		auto f = [&]()
		{
			while (true)
			{
				const auto theo = m_theo_queue->take();
				if (theo) {
					m_counter->count(theo.value());
				}
			}
		};
		std::thread theo_thread{ f };
		theo_thread.detach();
	}
};


void run_locked_coalescing_sample()
{

	auto counter = std::make_shared<Counter>();
	auto quote_book = std::make_shared<LockedCoalescingQueue<Quote>>();
	auto theo_book = std::make_shared<LockedCoalescingQueue<Theo>>();


	TheoCalculator calculator1{ quote_book, theo_book, 1, 100000 };
	TheoCalculator calculator2{ quote_book, theo_book, 100001, 200000 };

	QuoteSource und_quote_source1{ 1, 100000, quote_book };
	QuoteSource opt_quote_source1{ -100000, -1, quote_book };
	QuoteSource und_quote_source2{ 100001, 200000, quote_book };
	QuoteSource opt_quote_source2{ -200000, -100001, quote_book };

	TradingStrategy strategy1{ theo_book, counter , 1, 100000 };
	TradingStrategy strategy2{ theo_book, counter , 100001, 200000 };

	strategy1.start_poll();
	strategy2.start_poll();
	calculator1.start_poll();
	calculator2.start_poll();
	und_quote_source1.start();
	und_quote_source2.start();
	opt_quote_source1.start();
	opt_quote_source2.start();

	std::cout << "Started coalescing example";
	while (true) {
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
}



void run_coalescing_sample()
{

	auto counter = std::make_shared<Counter>();
	auto quote_book = std::make_shared<CoalescingQueue<Quote>>();
	auto theo_book = std::make_shared<CoalescingQueue<Theo>>();


	TheoCalculator calculator1{	quote_book, theo_book, 1, 100000};
	TheoCalculator calculator2{ quote_book, theo_book, 100001, 200000};

	QuoteSource und_quote_source1{ 1, 100000, quote_book };
	QuoteSource opt_quote_source1{ -100000, -1, quote_book };
	QuoteSource und_quote_source2{ 100001, 200000, quote_book };
	QuoteSource opt_quote_source2{-200000, -100001, quote_book };

	TradingStrategy strategy1{ theo_book, counter , 1, 100000};
	TradingStrategy strategy2{ theo_book, counter , 100001, 200000};
	
	strategy1.start_poll();
	strategy2.start_poll();
	calculator1.start_poll();
	calculator2.start_poll();
	und_quote_source1.start();
	und_quote_source2.start();
	opt_quote_source1.start();
	opt_quote_source2.start();
	
	std::cout << "Started coalescing example";
	while (true) {
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
}

void run_queued_sample()
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

void count_object_allocation()
{
	Counter c{};
	long event_id = 1;
	while (true)
	{
		c.count(Theo{ ++event_id, 1, -1, 1.2 });
	}
}

int main()
{
	try
	{
		std::cout << "Running sample\n";
		//run_locked_coalescing_sample();
		run_coalescing_sample();
		//run_queued_sample();
		//count_object_allocation();
	}
	catch (const std::exception& ex)
	{
		std::cout << "Caught " << ex.what() << "\n";
	}
}
