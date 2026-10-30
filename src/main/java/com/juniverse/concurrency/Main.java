package com.juniverse.concurrency;

import com.juniverse.concurrency.atomic.AtomicDemo;
import com.juniverse.concurrency.collections.ConcurrentCollectionsDemo;
import com.juniverse.concurrency.completable.CompletableFutureDemo;
import com.juniverse.concurrency.executors.ExecutorsDemo;
import com.juniverse.concurrency.forkjoin.ForkJoinDemo;
import com.juniverse.concurrency.locks.LocksDemo;
import com.juniverse.concurrency.synchronizers.SynchronizersDemo;
import com.juniverse.concurrency.threads.ThreadsDemo;

/**
 * Recorrido por la concurrencia en Java: hilos, executors, CompletableFuture, Fork/Join, variables atómicas, locks,
 * sincronizadores y colecciones concurrentes. (Los hilos virtuales de Java 21 tienen su propio ejemplo: mvn-virtual-threads.)
 */
public final class Main {

    private Main() {
    }

    private static void title(String text) {
        System.out.println();
        System.out.println("=========== " + text + " ===========");
    }

    /**
     * Punto de entrada.
     *
     * @param args no se usan
     * @throws Exception si falla alguna demostración
     */
    public static void main(String[] args) throws Exception {
        // Salida en UTF-8 para que se vean bien los acentos en cualquier consola
        System.setOut(new java.io.PrintStream(System.out, true, java.nio.charset.StandardCharsets.UTF_8));
        title("HILOS: Thread, join, interrupt, volatile, synchronized");
        ThreadsDemo.run();
        title("EXECUTORS: pools, Future, invokeAll/Any, scheduled");
        ExecutorsDemo.run();
        title("COMPLETABLEFUTURE: encadenar, combinar, errores, timeouts");
        CompletableFutureDemo.run();
        title("FORK/JOIN: divide y vencerás");
        ForkJoinDemo.run();
        title("VARIABLES ATÓMICAS: CAS, LongAdder, AtomicReference");
        AtomicDemo.run();
        title("LOCKS: ReentrantLock, Condition, ReadWriteLock, StampedLock");
        LocksDemo.run();
        title("SINCRONIZADORES: CountDownLatch, CyclicBarrier, Semaphore");
        SynchronizersDemo.run();
        title("COLECCIONES CONCURRENTES: ConcurrentHashMap, BlockingQueue, CopyOnWrite");
        ConcurrentCollectionsDemo.run();
    }
}
