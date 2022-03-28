package gemini;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class SampleThree extends SampleTwo {
    static <K,V> Map<K,V> newBook(){return Collections.synchronizedMap(new HashMap<>());}

    public static void main(final String[] args) throws Exception {
        startSample(newBook(), newBook());
        while (true) {
            Thread.sleep(500);
        }

    }


}
