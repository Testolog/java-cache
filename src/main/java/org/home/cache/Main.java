package org.home.cache;

/**
 * org.example
 *
 * @author Robert Nad
 */
public class Main {
    public static void main(String[] args) {
        CacheLayer cacheLayer = new CacheLayer(2);
        cacheLayer.put(2, 2);
        cacheLayer.put(1, 1);
        cacheLayer.put(3, 3);
        System.out.println(cacheLayer.get(2));
        CacheLayer cacheLayer2 = new CacheLayer(2);
        cacheLayer2.put(2, 2);
        cacheLayer2.put(1, 1);
        cacheLayer2.get(2);
        cacheLayer2.put(3, 3);
        System.out.println(cacheLayer2.get(2));
        System.out.println(cacheLayer2.get(1));
    }

}