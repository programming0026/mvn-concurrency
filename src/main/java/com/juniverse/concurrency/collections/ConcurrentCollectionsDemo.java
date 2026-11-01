package com.juniverse.concurrency.collections;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import com.juniverse.concurrency.util.Concurrent;

import java.util.logging.Logger;
import com.juniverse.concurrency.AppLogger;
/**
 * Colecciones concurrentes: {@code ConcurrentHashMap} (operaciones atómicas como {@code merge}),
 * {@code CopyOnWriteArrayList} (iterar mientras se modifica) y {@code BlockingQueue} (productor-consumidor).
 */
public final class ConcurrentCollectionsDemo {
    private static final Logger LOGGER = AppLogger.get(ConcurrentCollectionsDemo.class);


    private static final int POISON_PILL = -1;

    private ConcurrentCollectionsDemo() {
    }

    /**
     * Cuenta palabras desde varios hilos con {@code ConcurrentHashMap.merge}, que es atómico por clave.
     * Con un {@code HashMap} normal se perderían cuentas o la estructura se corrompería.
     *
     * @param words palabras; cada hilo procesa la lista completa
     * @param threads cantidad de hilos
     * @return conteo por palabra (frecuencia en la lista multiplicada por {@code threads})
     * @throws InterruptedException si se interrumpe la espera
     */
    public static Map<String, Integer> wordCount(List<String> words, int threads) throws InterruptedException {
        Map<String, Integer> counts = new ConcurrentHashMap<>();
        Concurrent.runConcurrently(threads, () -> words.forEach(word -> counts.merge(word, 1, Integer::sum)));
        return counts;
    }

    /**
     * Productor-consumidor con {@link ArrayBlockingQueue}: {@code put} espera si la cola está llena y {@code take} espera
     * si está vacía, así no hace falta {@code wait/notify}. Se termina con una "píldora de veneno".
     *
     * @param items cantidad de números 1..items que produce el productor
     * @return suma de lo consumido
     * @throws InterruptedException si se interrumpe la espera
     */
    public static long producerConsumer(int items) throws InterruptedException {
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(5);
        Thread producer = new Thread(() -> {
            try {
                for (int i = 1; i <= items; i++) {
                    queue.put(i);
                }
                queue.put(POISON_PILL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "productor");
        producer.start();
        long sum = 0;
        while (true) {
            int value = queue.take();
            if (value == POISON_PILL) {
                break;
            }
            sum += value;
        }
        producer.join();
        return sum;
    }

    /**
     * Modificar un {@code ArrayList} mientras se recorre lanza {@link ConcurrentModificationException}.
     *
     * @return {@code true} si se lanzó la excepción
     */
    public static boolean arrayListThrowsWhenModified() {
        List<Integer> list = new ArrayList<>(List.of(1, 2, 3));
        try {
            for (Integer n : list) {
                list.add(n * 10);
            }
            return false;
        } catch (ConcurrentModificationException e) {
            return true;
        }
    }

    /**
     * {@link CopyOnWriteArrayList} recorre una "foto" de la lista, así que se puede modificar durante el recorrido
     * (cada escritura copia el arreglo: úsala cuando se lee mucho y se escribe poco).
     *
     * @return tamaño final de la lista (6)
     */
    public static int copyOnWriteAllowsModification() {
        List<Integer> list = new CopyOnWriteArrayList<>(List.of(1, 2, 3));
        for (Integer n : list) {
            list.add(n * 10);
        }
        return list.size();
    }

    /** Muestra cada colección. */
    public static void run() throws InterruptedException {
        LOGGER.info("ConcurrentHashMap.merge desde 4 hilos: " + new java.util.TreeMap<>(wordCount(List.of("java", "hilos", "java", "lock", "java"), 4)));
        LOGGER.info("BlockingQueue, suma consumida de 1..1000: " + producerConsumer(1_000));
        LOGGER.info("ArrayList lanza ConcurrentModificationException: " + arrayListThrowsWhenModified());
        LOGGER.info("CopyOnWriteArrayList permite modificar al recorrer: " + copyOnWriteAllowsModification());
    }
}
