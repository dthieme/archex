package gemini;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class SampleBase {
    static final Counter<Theo> counter = new Counter<>();
    static final AtomicLong eventCounter = new AtomicLong(0);
    static <T> BlockingQueue<T> newQueue() { return new ArrayBlockingQueue<>(5000);}
    static <K,V> Map<K,V> newMap(){return new HashMap<>();}

    public record Quote(long eventId, int instrument, long price) {}
    public record Theo(long eventId, int underlyingInstrument, int optionInstrument, double tv) {}
    public record QuoteSource(int instrumentStart, int instrumentEnd, BlockingQueue<Quote> theoCalcQueue) {}
    public record TheoCalculator(BlockingQueue<Quote> quoteQueue, BlockingQueue<Theo> destQueue, Map<Integer, Quote> quoteMap) {}
    public record TradingStrategy(BlockingQueue<Theo> theoQueue) {}

    public static void startQuoteSource(final QuoteSource quoteSource) {
        // generate market quotes for my range of instruments and enqueue them onto the theo calculator's work queue
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                for (int instrumentId = quoteSource.instrumentStart; instrumentId <= quoteSource.instrumentEnd; instrumentId++)
                {
                    final Quote q = new Quote(eventCounter.incrementAndGet(), instrumentId, instrumentId);
                    while (! quoteSource.theoCalcQueue.offer(q)) {
                    }
                }
            }
        });
    }

    public static void startTheoCalculator(final TheoCalculator calculator) {
        // de-queue quotes, do some mock calculation, and enqueue the result onto the trading strategies work queue
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                Quote q = null;
                while ((q = calculator.quoteQueue.poll()) == null) {
                }

                final int instrumentId = q.instrument;
                calculator.quoteMap.put(instrumentId, q);
                final Quote underlyingQuote, optionQuote;
                if (instrumentId > 0) {
                    underlyingQuote = q;
                    optionQuote = calculator.quoteMap.get(-instrumentId);
                }
                else {
                    optionQuote = q;
                    underlyingQuote = calculator.quoteMap.get(-instrumentId);
                }
                if (optionQuote != null && underlyingQuote != null) {
                    final double tv = (underlyingQuote.price + optionQuote.price) / 4.0;
                    final Theo theo = new Theo(eventCounter.incrementAndGet(), underlyingQuote.instrument, optionQuote.instrument, tv);
                    while (!calculator.destQueue.offer(theo)) {
                    }
                }
            }
        });
    }

    public static void startTradingStrategy(final TradingStrategy strategy) {
        // consume a theo and execute the trading strategy
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                Theo t = null;
                while ((t = strategy.theoQueue.poll()) == null) {
                }
                counter.event(t);
            }
        });
    }

    public static void main(final String[] args) throws Exception {

        final TheoCalculator calculator1 = new TheoCalculator(newQueue(), newQueue(), newMap());
        final TheoCalculator calculator2 = new TheoCalculator(newQueue(), newQueue(), newMap());

        final QuoteSource undQuoteSource1 = new QuoteSource(1, 100_000, calculator1.quoteQueue);
        final QuoteSource undQuoteSource2 = new QuoteSource(100_001, 200_000, calculator2.quoteQueue);
        final QuoteSource optQuoteSource1 = new QuoteSource(-100_000, -1, calculator1.quoteQueue);
        final QuoteSource optQuoteSource2 = new QuoteSource(-200_000, -100_001, calculator2.quoteQueue);

        final TradingStrategy strategy1 = new TradingStrategy(calculator1.destQueue);
        final TradingStrategy strategy2 = new TradingStrategy(calculator2.destQueue);

        startTradingStrategy(strategy1);
        startTradingStrategy(strategy2);
        startTheoCalculator(calculator1);
        startTheoCalculator(calculator2);
        startQuoteSource(undQuoteSource1);
        startQuoteSource(undQuoteSource2);
        startQuoteSource(optQuoteSource1);
        startQuoteSource(optQuoteSource2);

        while (true) {
            Thread.sleep(500);
        }

    }


}
