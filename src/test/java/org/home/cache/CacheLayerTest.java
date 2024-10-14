package org.home.cache;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * org.home.cache
 *
 * @author Robert Nad
 */
class CacheLayerTest {

    @Test
    void testGet() {
        CacheLayer cacheLayer = new CacheLayer(2);
        cacheLayer.put(1, 1);
        assertEquals(cacheLayer.get(1), 1);
    }

    @Test
    void testGetNotExistsKey() {
        CacheLayer cacheLayer = new CacheLayer(2);
        assertNull(cacheLayer.get(1));
    }

    @Test
    void testPut() {
        CacheLayer cacheLayer = new CacheLayer(2);
        long nano = System.nanoTime();
        cacheLayer.put(1, 5);
        //i change access to innerState as should in first place use private
        // i don't want to spent time to reflection
        //Also use get method is not correct in testing, as get method should be also tested.
        List<Long> value = cacheLayer.innerState.get(1);
        assertEquals(value.get(0), 5);//value
        assertEquals(value.get(1), 0);//usage
        assertTrue(nano < value.get(2));//time created
    }

    @Test
    void testReplace() {
        CacheLayer cacheLayer = new CacheLayer(1);
        // i don't know what policy to use when we get a replacement data in cache by key, so i keep it as new one without save a prev count
        cacheLayer.put(1, 5);
        long newItem = cacheLayer.innerState.get(1).get(2);
        cacheLayer.put(1, 7);
        List<Long> value = cacheLayer.innerState.get(1);
        assertEquals(value.get(0), 7);//value
        assertEquals(value.get(1), 0);//usage
        assertTrue(newItem < value.get(2));//time created
    }

    @Test
    void testOverFlowScenario1() {
        CacheLayer cacheLayer = new CacheLayer(2);
        cacheLayer.put(2, 2);
        cacheLayer.put(1, 1);
        cacheLayer.put(3, 3);
        assertEquals(cacheLayer.get(3), 3);
        assertEquals(cacheLayer.get(1), 1);
        assertNull(cacheLayer.get(2));
    }

    @Test
    void testOverFlowScenario2() {
        CacheLayer cacheLayer = new CacheLayer(2);
        cacheLayer.put(2, 2);
        cacheLayer.put(1, 1);
        cacheLayer.get(2);
        cacheLayer.put(3, 3);
        assertEquals(cacheLayer.get(3), 3);
        assertEquals(cacheLayer.get(2), 2);
        assertNull(cacheLayer.get(1));
    }
}