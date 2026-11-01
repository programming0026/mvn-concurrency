package com.juniverse.concurrency.locks;

import com.juniverse.concurrency.util.Concurrent;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import java.util.logging.Logger;
import com.juniverse.concurrency.AppLogger;
/**
 * Locks explícitos de {@code java.util.concurrent.locks}: más flexibles que {@code synchronized} (intento sin esperar,
 * espera con tiempo límite, varias condiciones, bloqueos de lectura/escritura y lectura optimista).
 */
public final class LocksDemo {
    private static final Logger LOGGER = AppLogger.get(LocksDemo.class);


    private LocksDemo() {
    }

    /**
     * Contador protegido con {@link ReentrantLock}. El {@code unlock} va SIEMPRE en un {@code finally}.
     *
     * @param threads cantidad de hilos
     * @param increments incrementos por hilo
     * @return valor final exacto
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int lockedCounter(int threads, int increments) throws InterruptedException {
        ReentrantLock lock = new ReentrantLock();
        int[] counter = {0};
        Concurrent.runConcurrently(threads, () -> {
            for (int i = 0; i < increments; i++) {
                lock.lock();
                try {
                    counter[0]++;
                } finally {
                    lock.unlock();
                }
            }
        });
        return counter[0];
    }

    /**
     * {@code tryLock} no se queda esperando: si otro hilo tiene el lock devuelve {@code false} al instante, algo que
     * {@code synchronized} no permite (evita bloqueos indefinidos y interbloqueos).
     *
     * @return {@code true} si el segundo hilo NO pudo tomar el lock mientras estaba ocupado
     * @throws InterruptedException si se interrumpe la espera
     */
    public static boolean tryLockFailsWhileHeld() throws InterruptedException {
        ReentrantLock lock = new ReentrantLock();
        AtomicBoolean acquired = new AtomicBoolean(true);
        lock.lock();
        try {
            Thread other = new Thread(() -> {
                try {
                    boolean ok = lock.tryLock(50, TimeUnit.MILLISECONDS);
                    acquired.set(ok);
                    if (ok) {
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            other.start();
            other.join();
        } finally {
            lock.unlock();
        }
        return !acquired.get();
    }

    /**
     * {@link Condition}: equivalente a {@code wait/notify} pero con varias colas de espera por lock. Un hilo espera a que
     * otro active una bandera.
     *
     * @return {@code true} si el hilo que esperaba fue despertado por la señal
     * @throws InterruptedException si se interrumpe la espera
     */
    public static boolean conditionSignalsWaiter() throws InterruptedException {
        ReentrantLock lock = new ReentrantLock();
        Condition ready = lock.newCondition();
        boolean[] flag = {false};
        AtomicBoolean woke = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            lock.lock();
            try {
                while (!flag[0]) {          // siempre en un while: protege de despertares espurios
                    ready.awaitUninterruptibly();
                }
                woke.set(true);
            } finally {
                lock.unlock();
            }
        });
        waiter.start();
        Concurrent.sleep(50);
        lock.lock();
        try {
            flag[0] = true;
            ready.signalAll();
        } finally {
            lock.unlock();
        }
        waiter.join(2_000);
        return woke.get();
    }

    /**
     * Varias escrituras concurrentes en {@link ReadWriteCache}.
     *
     * @param threads cantidad de hilos
     * @param perThread claves distintas que escribe cada hilo
     * @return tamaño final de la caché
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int readWriteCacheSize(int threads, int perThread) throws InterruptedException {
        ReadWriteCache<String, Integer> cache = new ReadWriteCache<>();
        int[] ids = {0};
        Object idLock = new Object();
        Concurrent.runConcurrently(threads, () -> {
            int id;
            synchronized (idLock) {
                id = ids[0]++;
            }
            for (int i = 0; i < perThread; i++) {
                cache.put(id + "-" + i, i);
                cache.get(id + "-" + i);
            }
        });
        return cache.size();
    }

    /** Muestra cada tipo de lock. */
    public static void run() throws InterruptedException {
        LOGGER.info("ReentrantLock, contador exacto: " + lockedCounter(4, 100_000));
        LOGGER.info("tryLock con timeout falla si el lock está ocupado: " + tryLockFailsWhileHeld());
        LOGGER.info("Condition despierta al que espera: " + conditionSignalsWaiter());
        LOGGER.info("ReadWriteCache tras 4 x 500 escrituras: " + readWriteCacheSize(4, 500) + " entradas");
        StampedPoint point = new StampedPoint();
        point.move(3, 4);
        LOGGER.info("StampedLock (lectura optimista), distancia de (3,4): " + point.distanceFromOrigin());
    }
}
