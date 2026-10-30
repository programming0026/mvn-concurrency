package com.juniverse.concurrency.util;

import java.util.ArrayList;
import java.util.List;

/** Utilidades pequeñas que comparten los ejemplos. */
public final class Concurrent {

    private Concurrent() {
    }

    /**
     * Ejecuta la misma tarea en varios hilos a la vez y espera a que todos terminen.
     *
     * @param threads cantidad de hilos
     * @param task tarea que ejecuta cada hilo
     * @throws InterruptedException si el hilo actual es interrumpido mientras espera
     */
    public static void runConcurrently(int threads, Runnable task) throws InterruptedException {
        List<Thread> list = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread thread = new Thread(task, "worker-" + i);
            list.add(thread);
            thread.start();
        }
        for (Thread thread : list) {
            thread.join();
        }
    }

    /**
     * Duerme el hilo actual sin obligar a manejar la excepción comprobada.
     *
     * @param millis milisegundos
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Hilo interrumpido", e);
        }
    }

    /**
     * Mide cuánto tarda una tarea.
     *
     * @param task tarea a medir
     * @return duración en milisegundos
     */
    public static long timeMillis(Runnable task) {
        long start = System.nanoTime();
        task.run();
        return (System.nanoTime() - start) / 1_000_000;
    }
}
