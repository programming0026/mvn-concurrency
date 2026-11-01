package com.juniverse.concurrency.synchronizers;

import com.juniverse.concurrency.util.Concurrent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.logging.Logger;
import com.juniverse.concurrency.AppLogger;
/**
 * Sincronizadores: objetos que coordinan hilos. {@code CountDownLatch} (esperar a que ocurran N eventos),
 * {@code CyclicBarrier} (encontrarse todos en un punto, repetidamente) y {@code Semaphore} (limitar el acceso concurrente).
 */
public final class SynchronizersDemo {
    private static final Logger LOGGER = AppLogger.get(SynchronizersDemo.class);


    private SynchronizersDemo() {
    }

    /**
     * {@link CountDownLatch} como "pistola de salida": todos los hilos esperan la señal y arrancan juntos, y el hilo
     * principal espera a que todos terminen.
     *
     * @param runners cantidad de hilos
     * @return cuántos hilos completaron su trabajo
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int startGun(int runners) throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(runners);
        AtomicInteger completed = new AtomicInteger();
        for (int i = 0; i < runners; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    completed.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            }).start();
        }
        start.countDown();
        finished.await();
        return completed.get();
    }

    /**
     * {@link CyclicBarrier}: ningún hilo pasa a la fase 2 hasta que TODOS terminen la fase 1. Se ejecuta una acción al
     * abrirse la barrera. A diferencia del latch, se puede reutilizar.
     *
     * @param parties cantidad de hilos
     * @return bitácora: todas las "fase1", luego "barrera" y por último todas las "fase2"
     * @throws InterruptedException si se interrumpe la espera
     */
    public static List<String> barrierPhases(int parties) throws InterruptedException {
        List<String> log = Collections.synchronizedList(new ArrayList<>());
        CyclicBarrier barrier = new CyclicBarrier(parties, () -> log.add("barrera"));
        Concurrent.runConcurrently(parties, () -> {
            try {
                Concurrent.sleep((long) (Math.random() * 30));
                log.add("fase1");
                barrier.await();
                log.add("fase2");
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new IllegalStateException(e);
            }
        });
        return log;
    }

    /**
     * {@link Semaphore}: solo {@code permits} hilos a la vez pueden estar en la sección (por ejemplo, un pool de
     * conexiones o un límite de llamadas a un servicio externo).
     *
     * @param permits permisos disponibles
     * @param tasks cantidad de hilos que compiten
     * @return máximo de hilos observados dentro a la vez (nunca supera {@code permits})
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int maxConcurrentWithSemaphore(int permits, int tasks) throws InterruptedException {
        Semaphore semaphore = new Semaphore(permits);
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger max = new AtomicInteger();
        Concurrent.runConcurrently(tasks, () -> {
            try {
                semaphore.acquire();
                try {
                    max.accumulateAndGet(inside.incrementAndGet(), Math::max);
                    Concurrent.sleep(20);
                    inside.decrementAndGet();
                } finally {
                    semaphore.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return max.get();
    }

    /** Muestra cada sincronizador. */
    public static void run() throws InterruptedException {
        LOGGER.info("CountDownLatch (pistola de salida), corredores que terminaron: " + startGun(5));
        LOGGER.info("CyclicBarrier, orden de eventos: " + barrierPhases(3));
        LOGGER.info("Semaphore(3) con 10 hilos, máximo simultáneo observado: " + maxConcurrentWithSemaphore(3, 10));
    }
}
