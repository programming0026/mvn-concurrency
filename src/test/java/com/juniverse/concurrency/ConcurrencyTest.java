package com.juniverse.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.juniverse.concurrency.atomic.AtomicDemo;
import com.juniverse.concurrency.collections.ConcurrentCollectionsDemo;
import com.juniverse.concurrency.completable.CompletableFutureDemo;
import com.juniverse.concurrency.executors.ExecutorsDemo;
import com.juniverse.concurrency.forkjoin.ForkJoinDemo;
import com.juniverse.concurrency.locks.LocksDemo;
import com.juniverse.concurrency.locks.StampedPoint;
import com.juniverse.concurrency.synchronizers.SynchronizersDemo;
import com.juniverse.concurrency.threads.ThreadsDemo;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.LongStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Pruebas de los ejemplos de concurrencia. Todas tienen un tiempo máximo para detectar interbloqueos. */
@Timeout(30)
class ConcurrencyTest {

    @Nested
    @DisplayName("Hilos")
    class Threads {

        @Test
        void synchronizedCounterIsExactAndUnsafeNeverExceeds() throws Exception {
            assertEquals(400_000, ThreadsDemo.synchronizedCounter(4, 100_000));
            assertTrue(ThreadsDemo.unsafeCounter(4, 100_000) <= 400_000);
        }

        @Test
        void interruptAndVolatile() throws Exception {
            assertTrue(ThreadsDemo.interruptStopsWorker());
            assertTrue(ThreadsDemo.volatileFlagStopsLoop());
        }
    }

    @Nested
    @DisplayName("Executors")
    class Executors {

        @Test
        void callableAndFuture() throws Exception {
            assertEquals(List.of(1, 4, 9, 16), ExecutorsDemo.squares(List.of(1, 2, 3, 4)));
            assertEquals(List.of("A", "B", "C"), ExecutorsDemo.invokeAll());
            assertEquals("rápida", ExecutorsDemo.invokeAny());
        }

        @Test
        void timeoutsAndErrors() throws Exception {
            assertTrue(ExecutorsDemo.timeoutAndCancel());
            assertEquals("falló la tarea", ExecutorsDemo.failedTaskMessage());
            assertTrue(ExecutorsDemo.scheduledRunsRepeatedly(3));
        }

        @Test
        void rejectionPolicies() {
            assertTrue(ExecutorsDemo.abortPolicyRejects());
            assertTrue(ExecutorsDemo.callerRunsPolicyRunsInCaller());
        }
    }

    @Nested
    @DisplayName("CompletableFuture")
    class Completable {

        @Test
        void chainingAndCombining() {
            assertEquals("JAVA 21", CompletableFutureDemo.pipeline());
            assertEquals(List.of("pedido-1 de usuario7", "pedido-2 de usuario7"), CompletableFutureDemo.composeUserOrders(7));
            assertEquals(List.of(1, 4, 9), CompletableFutureDemo.allOfSquares(List.of(1, 2, 3)));
            assertEquals("rápida", CompletableFutureDemo.anyOfFastest());
        }

        @Test
        void errorsAndTimeouts() {
            assertEquals("ok", CompletableFutureDemo.recover(false));
            assertEquals("recuperado: falló el servicio remoto", CompletableFutureDemo.recover(true));
            assertEquals("valor 42", CompletableFutureDemo.handleBoth(false));
            assertEquals("error: dato inválido", CompletableFutureDemo.handleBoth(true));
            assertEquals("valor por defecto", CompletableFutureDemo.completeOnTimeout());
            assertTrue(CompletableFutureDemo.orTimeoutFails());
        }

        @Test
        void customPool() {
            assertTrue(CompletableFutureDemo.threadNameInCustomPool().startsWith("io-pool-"));
        }
    }

    @Nested
    @DisplayName("Fork/Join")
    class ForkJoin {

        private final ForkJoinPool pool = new ForkJoinPool(4);

        @Test
        void parallelSumMatchesSequential() {
            long[] data = LongStream.rangeClosed(1, 500_000).toArray();
            assertEquals(500_000L * 500_001L / 2, ForkJoinDemo.parallelSum(data, pool));
        }

        @Test
        void parallelSquareAndSort() {
            long[] data = LongStream.rangeClosed(1, 100_000).toArray();
            ForkJoinDemo.parallelSquare(data, pool);
            assertEquals(1L, data[0]);
            assertEquals(100_000L * 100_000L, data[99_999]);

            int[] numbers = new Random(7).ints(200_000, 0, 1_000).toArray();
            int[] expected = numbers.clone();
            java.util.Arrays.sort(expected);
            ForkJoinDemo.parallelSort(numbers, pool);
            org.junit.jupiter.api.Assertions.assertArrayEquals(expected, numbers);
        }
    }

    @Nested
    @DisplayName("Atómicos")
    class Atomics {

        @Test
        void countersAreExact() throws Exception {
            assertEquals(800_000, AtomicDemo.atomicCounter(8, 100_000));
            assertEquals(800_000L, AtomicDemo.adderCounter(8, 100_000));
            assertEquals(4_000, AtomicDemo.concurrentConfigUpdates(4, 1_000));
        }

        @Test
        void casOperations() throws Exception {
            assertEquals(91, AtomicDemo.maxOf(new int[] {4, 91, 17, 55, 3}));
            assertEquals(1, AtomicDemo.runOnlyOnce(16));
            assertTrue(AtomicDemo.abaDetectedWithStamp());
        }
    }

    @Nested
    @DisplayName("Locks")
    class Locks {

        @Test
        void lockBasedTools() throws Exception {
            assertEquals(400_000, LocksDemo.lockedCounter(4, 100_000));
            assertTrue(LocksDemo.tryLockFailsWhileHeld());
            assertTrue(LocksDemo.conditionSignalsWaiter());
            assertEquals(2_000, LocksDemo.readWriteCacheSize(4, 500));
        }

        @Test
        void stampedPoint() {
            StampedPoint point = new StampedPoint();
            point.move(3, 4);
            assertEquals(5.0, point.distanceFromOrigin(), 1e-9);
        }
    }

    @Nested
    @DisplayName("Sincronizadores")
    class Synchronizers {

        @Test
        void latchBarrierAndSemaphore() throws Exception {
            assertEquals(5, SynchronizersDemo.startGun(5));
            assertTrue(SynchronizersDemo.maxConcurrentWithSemaphore(3, 12) <= 3);
        }

        @Test
        void barrierSeparatesPhases() throws Exception {
            List<String> log = SynchronizersDemo.barrierPhases(4);
            assertEquals(9, log.size());
            assertEquals(List.of("fase1", "fase1", "fase1", "fase1"), log.subList(0, 4));
            assertEquals("barrera", log.get(4));
            assertEquals(List.of("fase2", "fase2", "fase2", "fase2"), log.subList(5, 9));
        }
    }

    @Nested
    @DisplayName("Colecciones concurrentes")
    class Collections {

        @Test
        void concurrentMapAndQueue() throws Exception {
            Map<String, Integer> counts = ConcurrentCollectionsDemo.wordCount(List.of("a", "b", "a", "a"), 4);
            assertEquals(12, counts.get("a"));
            assertEquals(4, counts.get("b"));
            assertEquals(500_500L, ConcurrentCollectionsDemo.producerConsumer(1_000));
        }

        @Test
        void copyOnWriteVsArrayList() {
            assertTrue(ConcurrentCollectionsDemo.arrayListThrowsWhenModified());
            assertEquals(6, ConcurrentCollectionsDemo.copyOnWriteAllowsModification());
            assertFalse(ConcurrentCollectionsDemo.copyOnWriteAllowsModification() < 6);
        }
    }
}
