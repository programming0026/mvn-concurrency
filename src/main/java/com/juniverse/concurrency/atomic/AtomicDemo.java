package com.juniverse.concurrency.atomic;

import com.juniverse.concurrency.util.Concurrent;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.atomic.LongAdder;

import java.util.logging.Logger;
import com.juniverse.concurrency.AppLogger;
/**
 * Variables atómicas: operaciones de lectura-modificación-escritura sin bloqueos, basadas en la instrucción de hardware
 * <i>compare-and-set</i> (CAS). Son la base de muchas estructuras concurrentes y más baratas que {@code synchronized}
 * cuando la operación es sencilla.
 */
public final class AtomicDemo {
    private static final Logger LOGGER = AppLogger.get(AtomicDemo.class);


    /**
     * Configuración inmutable: para cambiarla se crea otra y se intercambia con CAS.
     *
     * @param name nombre
     * @param retries reintentos
     */
    public record Config(String name, int retries) {
    }

    private AtomicDemo() {
    }

    /**
     * {@link AtomicInteger#incrementAndGet()} es atómico: no se pierden incrementos.
     *
     * @param threads cantidad de hilos
     * @param increments incrementos por hilo
     * @return valor final, siempre exacto
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int atomicCounter(int threads, int increments) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        Concurrent.runConcurrently(threads, () -> {
            for (int i = 0; i < increments; i++) {
                counter.incrementAndGet();
            }
        });
        return counter.get();
    }

    /**
     * {@link LongAdder} reparte las sumas en varias celdas y las junta al leer: escala mucho mejor que
     * {@code AtomicLong} cuando hay muchos hilos escribiendo y pocas lecturas (contadores, métricas).
     *
     * @param threads cantidad de hilos
     * @param increments incrementos por hilo
     * @return suma final
     * @throws InterruptedException si se interrumpe la espera
     */
    public static long adderCounter(int threads, int increments) throws InterruptedException {
        LongAdder adder = new LongAdder();
        Concurrent.runConcurrently(threads, () -> {
            for (int i = 0; i < increments; i++) {
                adder.increment();
            }
        });
        return adder.sum();
    }

    /**
     * {@code accumulateAndGet} aplica una función de forma atómica: aquí se calcula el máximo desde varios hilos.
     *
     * @param values valores a recorrer en paralelo
     * @return el máximo
     */
    public static int maxOf(int[] values) {
        AtomicInteger max = new AtomicInteger(Integer.MIN_VALUE);
        Arrays.stream(values).parallel().forEach(v -> max.accumulateAndGet(v, Math::max));
        return max.get();
    }

    /**
     * {@code compareAndSet(false, true)} solo lo gana un hilo: sirve para ejecutar algo UNA sola vez.
     *
     * @param threads cantidad de hilos que compiten
     * @return cuántas veces se ejecutó la acción (siempre 1)
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int runOnlyOnce(int threads) throws InterruptedException {
        AtomicBoolean done = new AtomicBoolean();
        AtomicInteger executions = new AtomicInteger();
        Concurrent.runConcurrently(threads, () -> {
            if (done.compareAndSet(false, true)) {
                executions.incrementAndGet();
            }
        });
        return executions.get();
    }

    /**
     * {@code updateAndGet} sobre una referencia a un objeto inmutable: reintenta con CAS hasta que ningún otro hilo
     * haya cambiado el valor entre la lectura y la escritura. La función debe ser pura (puede ejecutarse varias veces).
     *
     * @param threads cantidad de hilos
     * @param updates actualizaciones por hilo
     * @return reintentos finales de la configuración
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int concurrentConfigUpdates(int threads, int updates) throws InterruptedException {
        AtomicReference<Config> config = new AtomicReference<>(new Config("app", 0));
        Concurrent.runConcurrently(threads, () -> {
            for (int i = 0; i < updates; i++) {
                config.updateAndGet(c -> new Config(c.name(), c.retries() + 1));
            }
        });
        return config.get().retries();
    }

    /**
     * Problema ABA: el valor pasa de A a B y vuelve a A, y un CAS normal no lo nota. {@link AtomicStampedReference} añade
     * un número de versión que sí lo delata.
     *
     * @return {@code true} si el CAS con sello detectó el cambio intermedio
     */
    public static boolean abaDetectedWithStamp() {
        AtomicStampedReference<String> ref = new AtomicStampedReference<>("A", 0);
        int stampBefore = ref.getStamp();
        ref.compareAndSet("A", "B", 0, 1);
        ref.compareAndSet("B", "A", 1, 2);
        boolean swapped = ref.compareAndSet("A", "C", stampBefore, stampBefore + 1);
        return !swapped;
    }

    /** Compara contadores y muestra las demás técnicas. */
    public static void run() throws InterruptedException {
        int threads = 8;
        int increments = 200_000;
        LOGGER.info("Esperado: " + (long) threads * increments);
        LOGGER.info("AtomicInteger: " + atomicCounter(threads, increments));
        LOGGER.info("LongAdder:     " + adderCounter(threads, increments));
        LOGGER.info("Máximo con accumulateAndGet: " + maxOf(new int[] {4, 91, 17, 55, 3, 88}));
        LOGGER.info("Acción ejecutada una sola vez con compareAndSet: " + runOnlyOnce(16) + " vez");
        LOGGER.info("AtomicReference<Config> tras 4 x 1000 actualizaciones: " + concurrentConfigUpdates(4, 1_000));
        LOGGER.info("ABA detectado con AtomicStampedReference: " + abaDetectedWithStamp());
    }
}
