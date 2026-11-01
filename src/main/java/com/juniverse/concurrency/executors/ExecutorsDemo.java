package com.juniverse.concurrency.executors;

import com.juniverse.concurrency.util.Concurrent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import java.util.logging.Logger;
import com.juniverse.concurrency.AppLogger;
/**
 * Executors: en lugar de crear hilos a mano se envían tareas a un pool. Se ven {@code Runnable} vs {@code Callable},
 * {@code Future}, {@code invokeAll/invokeAny}, tiempos de espera, cancelación, tareas programadas y políticas de rechazo.
 * Desde Java 19 {@link ExecutorService} es {@code AutoCloseable}: el {@code try-with-resources} espera a que terminen las tareas.
 */
public final class ExecutorsDemo {
    private static final Logger LOGGER = AppLogger.get(ExecutorsDemo.class);


    private ExecutorsDemo() {
    }

    /**
     * Calcula cuadrados en un pool fijo con {@code Callable} y recoge los {@code Future}.
     *
     * @param numbers números de entrada
     * @return cuadrados en el mismo orden
     * @throws Exception si alguna tarea falla
     */
    public static List<Integer> squares(List<Integer> numbers) throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int n : numbers) {
                futures.add(pool.submit(() -> n * n));
            }
            List<Integer> result = new ArrayList<>();
            for (Future<Integer> future : futures) {
                result.add(future.get());
            }
            return result;
        }
    }

    private static Callable<String> delayed(String name, long millis) {
        return () -> {
            Concurrent.sleep(millis);
            return name;
        };
    }

    /**
     * {@code invokeAll} espera a TODAS las tareas y devuelve sus futures en el orden de entrada.
     *
     * @return nombres de las tareas, en el orden de entrada
     * @throws Exception si falla alguna tarea
     */
    public static List<String> invokeAll() throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(3)) {
            List<Future<String>> futures = pool.invokeAll(List.of(delayed("A", 90), delayed("B", 30), delayed("C", 60)));
            List<String> names = new ArrayList<>();
            for (Future<String> future : futures) {
                names.add(future.get());
            }
            return names;
        }
    }

    /**
     * {@code invokeAny} devuelve el resultado de la PRIMERA tarea que termina bien y cancela las demás.
     *
     * @return nombre de la tarea más rápida
     * @throws Exception si todas fallan
     */
    public static String invokeAny() throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(3)) {
            return pool.invokeAny(List.of(delayed("lenta", 1_000), delayed("media", 500), delayed("rápida", 20)));
        }
    }

    /**
     * {@code Future.get(timeout)} deja de esperar pasado el límite y {@code cancel(true)} interrumpe la tarea.
     *
     * @return {@code true} si hubo timeout y la tarea quedó cancelada
     * @throws Exception si falla algo inesperado
     */
    public static boolean timeoutAndCancel() throws Exception {
        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            Future<String> future = pool.submit(() -> {
                Thread.sleep(5_000);
                return "tarde";
            });
            try {
                future.get(100, TimeUnit.MILLISECONDS);
                return false;
            } catch (TimeoutException e) {
                future.cancel(true);
                return future.isCancelled();
            }
        }
    }

    /**
     * Una excepción dentro de la tarea se recibe al llamar a {@code get()} envuelta en {@link ExecutionException}.
     *
     * @return mensaje de la causa original
     * @throws Exception si falla algo inesperado
     */
    public static String failedTaskMessage() throws Exception {
        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            Future<String> future = pool.submit(() -> {
                throw new IllegalStateException("falló la tarea");
            });
            try {
                return future.get();
            } catch (ExecutionException e) {
                return e.getCause().getMessage();
            }
        }
    }

    /**
     * Tarea periódica con {@code scheduleAtFixedRate}.
     *
     * @param ticks número de ejecuciones que se esperan
     * @return {@code true} si la tarea se ejecutó al menos esas veces
     * @throws Exception si se interrumpe la espera
     */
    public static boolean scheduledRunsRepeatedly(int ticks) throws Exception {
        CountDownLatch latch = new CountDownLatch(ticks);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        try {
            scheduler.scheduleAtFixedRate(latch::countDown, 0, 20, TimeUnit.MILLISECONDS);
            return latch.await(2, TimeUnit.SECONDS);
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static ThreadPoolExecutor tinyPool(java.util.concurrent.RejectedExecutionHandler handler) {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), handler);
    }

    /**
     * Pool con 1 hilo y cola de 1: la tercera tarea no cabe y con {@code AbortPolicy} se lanza
     * {@link RejectedExecutionException} (el pool aplica así contrapresión).
     *
     * @return {@code true} si la tercera tarea fue rechazada
     */
    public static boolean abortPolicyRejects() {
        CountDownLatch release = new CountDownLatch(1);
        ThreadPoolExecutor pool = tinyPool(new ThreadPoolExecutor.AbortPolicy());
        Runnable blocker = () -> {
            try {
                release.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        };
        try {
            pool.execute(blocker);
            pool.execute(blocker);
            try {
                pool.execute(blocker);
                return false;
            } catch (RejectedExecutionException e) {
                return true;
            }
        } finally {
            release.countDown();
            pool.shutdown();
        }
    }

    /**
     * Con {@code CallerRunsPolicy} la tarea que no cabe se ejecuta en el hilo que la envió, lo que frena al productor.
     *
     * @return {@code true} si la tercera tarea se ejecutó en el hilo que llamó a {@code execute}
     */
    public static boolean callerRunsPolicyRunsInCaller() {
        CountDownLatch release = new CountDownLatch(1);
        ThreadPoolExecutor pool = tinyPool(new ThreadPoolExecutor.CallerRunsPolicy());
        Runnable blocker = () -> {
            try {
                release.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        };
        AtomicReference<String> ranIn = new AtomicReference<>();
        try {
            pool.execute(blocker);
            pool.execute(blocker);
            pool.execute(() -> ranIn.set(Thread.currentThread().getName()));
            return Thread.currentThread().getName().equals(ranIn.get());
        } finally {
            release.countDown();
            pool.shutdown();
        }
    }

    /** Ejecuta cada técnica y muestra el resultado. */
    public static void run() throws Exception {
        LOGGER.info("Cuadrados con Callable/Future: " + squares(List.of(1, 2, 3, 4, 5)));
        LOGGER.info("invokeAll (orden de entrada): " + invokeAll());
        LOGGER.info("invokeAny (la primera en terminar): " + invokeAny());
        LOGGER.info("get con timeout + cancel: " + timeoutAndCancel());
        LOGGER.info("Excepción de la tarea llega en ExecutionException: " + failedTaskMessage());
        LOGGER.info("scheduleAtFixedRate se repite: " + scheduledRunsRepeatedly(3));
        LOGGER.info("AbortPolicy rechaza la tarea que no cabe: " + abortPolicyRejects());
        LOGGER.info("CallerRunsPolicy ejecuta en el hilo que envía: " + callerRunsPolicyRunsInCaller());
    }
}
