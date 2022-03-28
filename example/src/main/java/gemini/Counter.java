package gemini;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class Counter<T> {
    private final AtomicLong numEvents = new AtomicLong(0);
    private long lastTimestamp = System.nanoTime();
    private final DecimalFormat formatter = new DecimalFormat("#,###");
    private static final long OneSecond = 1000 * 1000 * 1000;

    public Counter() {
    }

    public void event(T event) {

        final long numEvts = numEvents.addAndGet(1);
        final long curTime = System.nanoTime();
        final long timeElapsed = curTime - lastTimestamp;
        if (timeElapsed > OneSecond) {
            final double secondsElapsed = (double)timeElapsed / (double)OneSecond;
            final double eventsPerSecond = (double)numEvts * secondsElapsed;

            System.out.println("Handled " + formatter.format(eventsPerSecond) + " events/sec");
            lastTimestamp = System.nanoTime();
            numEvents.set(0);
        }
    }

}
