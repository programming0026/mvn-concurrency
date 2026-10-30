package com.juniverse.concurrency.locks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Caché con {@link ReadWriteLock}: muchos hilos pueden LEER a la vez, pero la escritura es exclusiva. Conviene cuando
 * hay muchas más lecturas que escrituras.
 *
 * @param <K> tipo de la clave
 * @param <V> tipo del valor
 */
public class ReadWriteCache<K, V> {

    private final Map<K, V> map = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Lee un valor (bloqueo compartido).
     *
     * @param key clave
     * @return el valor o {@code null}
     */
    public V get(K key) {
        lock.readLock().lock();
        try {
            return map.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Guarda un valor (bloqueo exclusivo).
     *
     * @param key clave
     * @param value valor
     */
    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            map.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Cantidad de entradas.
     *
     * @return tamaño de la caché
     */
    public int size() {
        lock.readLock().lock();
        try {
            return map.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
