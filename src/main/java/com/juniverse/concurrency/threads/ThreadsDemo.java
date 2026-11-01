package com.juniverse.concurrency.threads;

import com.juniverse.concurrency.util.Concurrent;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.logging.Logger;
import com.juniverse.concurrency.AppLogger;
/**
 * Hilos básicos: crear, esperar con {@code join}, interrumpir, hilos demonio y el problema de las condiciones de carrera
 * (varios hilos que modifican el mismo dato) con su solución mediante {@code synchronized}.
 */
public final class ThreadsDemo {
    private static final Logger LOGGER = AppLogger.get(ThreadsDemo.class);


    private ThreadsDemo() {
    }

    /**
     * Incrementa un contador compartido SIN sincronización: {@code c++} son tres pasos (leer, sumar, escribir) y los
     * hilos se pisan, así que el resultado suele ser menor al esperado.
     *
     * @param threads cantidad de hilos
     * @param increments incrementos por hilo
     * @return valor final (casi siempre menor que {@code threads * increments})
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int unsafeCounter(int threads, int increments) throws InterruptedException {
        int[] counter = {0};
        Concurrent.runConcurrently(threads, () -> {
            for (int i = 0; i < increments; i++) {
                counter[0]++;
            }
        });
        return counter[0];
    }

    /**
     * Igual que {@link #unsafeCounter} pero con un bloque {@code synchronized}: solo un hilo a la vez entra a la sección
     * crítica, así que el resultado es siempre exacto.
     *
     * @param threads cantidad de hilos
     * @param increments incrementos por hilo
     * @return valor final, siempre {@code threads * increments}
     * @throws InterruptedException si se interrumpe la espera
     */
    public static int synchronizedCounter(int threads, int increments) throws InterruptedException {
        int[] counter = {0};
        Object lock = new Object();
        Concurrent.runConcurrently(threads, () -> {
            for (int i = 0; i < increments; i++) {
                synchronized (lock) {
                    counter[0]++;
                }
            }
        });
        return counter[0];
    }

    /**
     * Pide a un hilo que termine con {@code interrupt()}: la interrupción es una petición cooperativa, el hilo debe
     * revisarla (o estar bloqueado en una operación que la detecte, como {@code sleep}).
     *
     * @return {@code true} si el hilo terminó por la interrupción
     * @throws InterruptedException si se interrumpe la espera
     */
    public static boolean interruptStopsWorker() throws InterruptedException {
        AtomicBoolean stoppedByInterrupt = new AtomicBoolean();
        Thread worker = new Thread(() -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                stoppedByInterrupt.set(true);
            }
        }, "interruptible");
        worker.start();
        worker.interrupt();
        worker.join();
        return stoppedByInterrupt.get();
    }

    /**
     * Un volatile garantiza que los cambios de una bandera sean visibles entre hilos; sin él, el hilo lector podría no
     * ver nunca el cambio.
     *
     * @return {@code true} si el hilo vio la bandera y terminó
     * @throws InterruptedException si se interrumpe la espera
     */
    public static boolean volatileFlagStopsLoop() throws InterruptedException {
        class Flag {
            volatile boolean running = true;
        }
        Flag flag = new Flag();
        Thread worker = new Thread(() -> {
            while (flag.running) {
                Thread.onSpinWait();
            }
        });
        worker.start();
        Concurrent.sleep(50);
        flag.running = false;
        worker.join(2_000);
        return !worker.isAlive();
    }

    /** Muestra por consola las diferencias entre las versiones segura e insegura. */
    public static void run() throws InterruptedException {
        Thread thread = new Thread(() -> LOGGER.info("Hola desde el hilo " + Thread.currentThread().getName()), "mi-hilo");
        thread.start();
        thread.join();

        Thread daemon = Thread.ofPlatform().daemon(true).name("demonio").unstarted(() -> { });
        LOGGER.info("¿'demonio' es hilo demonio? " + daemon.isDaemon() + " (no impide que la JVM termine)");

        int expected = 4 * 100_000;
        LOGGER.info("Esperado: " + expected);
        LOGGER.info("Sin sincronizar: " + unsafeCounter(4, 100_000) + "  <- condición de carrera, casi siempre menor");
        LOGGER.info("synchronized:    " + synchronizedCounter(4, 100_000));
        LOGGER.info("interrupt() detiene al hilo: " + interruptStopsWorker());
        LOGGER.info("volatile hace visible la bandera: " + volatileFlagStopsLoop());
    }
}
