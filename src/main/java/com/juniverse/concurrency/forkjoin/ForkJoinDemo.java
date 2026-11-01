package com.juniverse.concurrency.forkjoin;

import com.juniverse.concurrency.util.Concurrent;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

import java.util.logging.Logger;
import com.juniverse.concurrency.AppLogger;
/**
 * Fork/Join: "divide y vencerás" con robo de trabajo. Una tarea grande se parte en subtareas ({@code fork}) hasta que
 * son lo bastante pequeñas, se resuelven y se unen los resultados ({@code join}). {@link RecursiveTask} devuelve un
 * valor y {@link RecursiveAction} no.
 */
public final class ForkJoinDemo {
    private static final Logger LOGGER = AppLogger.get(ForkJoinDemo.class);


    private ForkJoinDemo() {
    }

    /** Suma un tramo de un arreglo dividiéndolo recursivamente. */
    public static final class SumTask extends RecursiveTask<Long> {

        private static final long serialVersionUID = 1L;
        private static final int THRESHOLD = 20_000;
        private final long[] data;
        private final int from;
        private final int to;

        /**
         * Crea la tarea.
         *
         * @param data arreglo
         * @param from inicio (inclusive)
         * @param to fin (exclusivo)
         */
        public SumTask(long[] data, int from, int to) {
            this.data = data;
            this.from = from;
            this.to = to;
        }

        @Override
        protected Long compute() {
            if (to - from <= THRESHOLD) {
                long sum = 0;
                for (int i = from; i < to; i++) {
                    sum += data[i];
                }
                return sum;
            }
            int mid = (from + to) >>> 1;
            SumTask left = new SumTask(data, from, mid);
            SumTask right = new SumTask(data, mid, to);
            left.fork();                      // la mitad izquierda se ejecuta en otro hilo
            long rightSum = right.compute();  // la derecha se calcula en este mismo hilo
            return left.join() + rightSum;
        }
    }

    /** Eleva al cuadrado cada elemento en su lugar (no devuelve valor: RecursiveAction). */
    public static final class SquareAction extends RecursiveAction {

        private static final long serialVersionUID = 1L;
        private static final int THRESHOLD = 20_000;
        private final long[] data;
        private final int from;
        private final int to;

        /**
         * Crea la tarea.
         *
         * @param data arreglo que se modifica
         * @param from inicio (inclusive)
         * @param to fin (exclusivo)
         */
        public SquareAction(long[] data, int from, int to) {
            this.data = data;
            this.from = from;
            this.to = to;
        }

        @Override
        protected void compute() {
            if (to - from <= THRESHOLD) {
                for (int i = from; i < to; i++) {
                    data[i] = data[i] * data[i];
                }
                return;
            }
            int mid = (from + to) >>> 1;
            invokeAll(new SquareAction(data, from, mid), new SquareAction(data, mid, to));
        }
    }

    /** Ordenamiento por mezcla paralelo: ordena las dos mitades a la vez y luego las mezcla. */
    public static final class MergeSortTask extends RecursiveAction {

        private static final long serialVersionUID = 1L;
        private static final int THRESHOLD = 4_096;
        private final int[] data;
        private final int[] buffer;
        private final int from;
        private final int to;

        /**
         * Crea la tarea.
         *
         * @param data arreglo a ordenar
         * @param buffer arreglo auxiliar del mismo tamaño
         * @param from inicio (inclusive)
         * @param to fin (exclusivo)
         */
        public MergeSortTask(int[] data, int[] buffer, int from, int to) {
            this.data = data;
            this.buffer = buffer;
            this.from = from;
            this.to = to;
        }

        @Override
        protected void compute() {
            if (to - from <= THRESHOLD) {
                Arrays.sort(data, from, to);
                return;
            }
            int mid = (from + to) >>> 1;
            invokeAll(new MergeSortTask(data, buffer, from, mid), new MergeSortTask(data, buffer, mid, to));
            merge(mid);
        }

        private void merge(int mid) {
            System.arraycopy(data, from, buffer, from, to - from);
            int i = from;
            int j = mid;
            int k = from;
            while (i < mid && j < to) {
                data[k++] = buffer[i] <= buffer[j] ? buffer[i++] : buffer[j++];
            }
            while (i < mid) {
                data[k++] = buffer[i++];
            }
        }
    }

    /**
     * Suma un arreglo con Fork/Join.
     *
     * @param data arreglo
     * @param pool pool donde se ejecuta
     * @return suma de todos los elementos
     */
    public static long parallelSum(long[] data, ForkJoinPool pool) {
        return pool.invoke(new SumTask(data, 0, data.length));
    }

    /**
     * Eleva al cuadrado todos los elementos en paralelo.
     *
     * @param data arreglo que se modifica
     * @param pool pool donde se ejecuta
     */
    public static void parallelSquare(long[] data, ForkJoinPool pool) {
        pool.invoke(new SquareAction(data, 0, data.length));
    }

    /**
     * Ordena un arreglo en paralelo.
     *
     * @param data arreglo que se ordena en su lugar
     * @param pool pool donde se ejecuta
     */
    public static void parallelSort(int[] data, ForkJoinPool pool) {
        pool.invoke(new MergeSortTask(data, new int[data.length], 0, data.length));
    }

    /** Compara el tiempo secuencial y el paralelo con un arreglo grande. */
    public static void run() {
        long[] data = new long[20_000_000];
        for (int i = 0; i < data.length; i++) {
            data[i] = i % 1000;
        }
        ForkJoinPool pool = ForkJoinPool.commonPool();
        LOGGER.info("Núcleos disponibles: " + Runtime.getRuntime().availableProcessors() + ", paralelismo del commonPool: " + pool.getParallelism());

        long[] result = new long[1];
        long sequential = Concurrent.timeMillis(() -> {
            long sum = 0;
            for (long v : data) {
                sum += v;
            }
            result[0] = sum;
        });
        long parallelResult = parallelSum(data, pool);
        long parallel = Concurrent.timeMillis(() -> parallelSum(data, pool));
        LOGGER.info("Suma secuencial: " + result[0] + " en " + sequential + " ms");
        LOGGER.info("Suma Fork/Join:  " + parallelResult + " en " + parallel + " ms (la mejora depende del número de núcleos)");

        int[] numbers = new Random(1).ints(2_000_000, 0, 1_000_000).toArray();
        long sortTime = Concurrent.timeMillis(() -> parallelSort(numbers, pool));
        boolean sorted = true;
        for (int i = 1; i < numbers.length; i++) {
            sorted &= numbers[i - 1] <= numbers[i];
        }
        LOGGER.info("MergeSort paralelo de 2.000.000 números: " + sortTime + " ms, ordenado = " + sorted);
        LOGGER.info("Los streams paralelos (parallel()) usan este mismo commonPool por debajo.");
    }
}
