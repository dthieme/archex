package gemini;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class SampleTwo {
    static final Counter<SampleBase.Theo> counter = new Counter<>();
    static <K,V> Map<K,V> newBook(){return new ConcurrentHashMap<>();}

    public record QuoteSource(int instrumentStart, int instrumentEnd, Map<Integer, SampleBase.Quote> destBook) {}
    public record TheoCalculator(int instrumentStart, int instrumentEnd, Map<Integer, SampleBase.Quote> quoteBook, Map<Integer, SampleBase.Theo> theoBook) {}
    public record TradingStrategy(int instrumentStart, int instrumentEnd, Map<Integer, SampleBase.Theo> theoBook) {}

    public static void startQuoteSource(final SampleTwo.QuoteSource quoteSource) {
        // generate market quotes for my range of instruments and enqueue them onto the theo calculator's work queue
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                for (int instrumentId = quoteSource.instrumentStart; instrumentId <= quoteSource.instrumentEnd; instrumentId++)
                {
                    final SampleBase.Quote q = new SampleBase.Quote(instrumentId, instrumentId);
                    quoteSource.destBook.put(q.instrument(), q);
                }
            }
        });
    }

    public static void startTheoCalculator(final SampleTwo.TheoCalculator calculator) {
        // de-queue quotes, do some mock calculation, and enqueue the result onto the trading strategies work queue
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                for (int instrumentId = calculator.instrumentStart; instrumentId < calculator.instrumentEnd; instrumentId++)
                {
                    final int optionId = -instrumentId;
                    final SampleBase.Quote underlyingQuote = calculator.quoteBook.get(instrumentId);
                    SampleBase.Quote optionQuote = calculator.quoteBook.get(optionId);
                    if (underlyingQuote != null && optionQuote != null) {
                        final double tv = (underlyingQuote.price() + optionQuote.price()) / 4.0;
                        final SampleBase.Theo theo = new SampleBase.Theo(underlyingQuote.instrument(), optionQuote.instrument(), tv);
                        calculator.theoBook.put(instrumentId, theo);
                    }
                }
            }
        });
    }

    public static void startTradingStrategy(final SampleTwo.TradingStrategy strategy) {
        // consume a theo and execute the trading strategy
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                for (int instrumentId = strategy.instrumentStart; instrumentId <= strategy.instrumentEnd; instrumentId++)
                {
                    final SampleBase.Theo t = strategy.theoBook.get(instrumentId);
                    if (t != null) {
                        counter.event(t);
                    }

                }
            }
        });
    }

    static void startSample(final Map<Integer, SampleBase.Quote> quoteBook, final Map<Integer, SampleBase.Theo> theoBook) {
        final SampleTwo.TheoCalculator calculator1 = new SampleTwo.TheoCalculator(1, 100_000, quoteBook, theoBook);
        final SampleTwo.TheoCalculator calculator2 = new SampleTwo.TheoCalculator(100_001, 200_0000, quoteBook, theoBook);

        final SampleTwo.QuoteSource undQuoteSource1 = new SampleTwo.QuoteSource(1, 100_000, quoteBook);
        final SampleTwo.QuoteSource undQuoteSource2 = new SampleTwo.QuoteSource(100_001, 200_000, quoteBook);
        final SampleTwo.QuoteSource optQuoteSource1 = new SampleTwo.QuoteSource(-100_001, -1, quoteBook);
        final SampleTwo.QuoteSource optQuoteSource2 = new SampleTwo.QuoteSource(-200_000, -100_001, quoteBook);


        final SampleTwo.TradingStrategy strategy1 = new SampleTwo.TradingStrategy(1, 100_000, theoBook);
        final SampleTwo.TradingStrategy strategy2 = new SampleTwo.TradingStrategy(100_001, 200_000, theoBook);

        startTradingStrategy(strategy1);
        startTradingStrategy(strategy2);
        startTheoCalculator(calculator1);
        startTheoCalculator(calculator2);
        startQuoteSource(undQuoteSource1);
        startQuoteSource(undQuoteSource2);
        startQuoteSource(optQuoteSource1);
        startQuoteSource(optQuoteSource2);
    }

    public static void main(final String[] args) throws Exception {
        startSample(newBook(), newBook());
        while (true) {
            Thread.sleep(500);
        }
    }

}
