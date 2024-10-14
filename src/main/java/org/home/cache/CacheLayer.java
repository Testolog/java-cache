package org.home.cache;


import java.util.*;

/**
 * org.example
 *
 * @author Robert Nad
 */
// This is not optimal, however as i have started on interview i will contention with this idea.
public class CacheLayer {
    private int capacity;
    private int usage;
    Map<Integer, List<Long>> innerState;

    public CacheLayer(int capacity) {
        innerState = new HashMap<>(capacity);
        this.capacity = capacity;
        this.usage = 0;
    }

    public Integer get(int key) {
        if (innerState.containsKey(key)) {
            List<Long> tmp = innerState.get(key);
            tmp.set(1, tmp.get(1) + 1);
            return innerState.get(key).get(0).intValue();
        }
        return null;
    }

    public void put(int key, int value) {
        List<Long> newValue = new ArrayList<>(3);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.getTimeInMillis();
        newValue.add(0, (long) value);
        newValue.add(1, ((long) 0));
        newValue.add(2, System.nanoTime());
        if (usage == capacity) {
            Integer minKey = null;
            for (Integer skey : innerState.keySet()) {
                if (minKey == null) {
                    minKey = skey;
                } else {
                    if (innerState.get(minKey).get(1) >= innerState.get(skey).get(1) &&
                            innerState.get(minKey).get(2) > innerState.get(skey).get(2)) {
                        minKey = skey;
                    }
                }
            }
            innerState.remove(minKey);
            usage -= 1;
            innerState.put(key, newValue);
            return;
        }
        innerState.put(key, newValue);
        usage += 1;
    }
}
