package com.juniverse.concurrency.completable;

import com.juniverse.concurrency.util.Concurrent;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.logging.Logger;
import com.juniverse.concurrency.AppLogger;
/**
 * {@link CompletableFuture}: composición de tareas asíncronas sin bloquear. Encadenar ({@code thenApply}, {@code thenCompose}),
 * combinar ({@code thenCombine}, {@code allOf}, {@code anyOf}), manejar errores ({@code exceptionally}, {@code handle}) y
 * límites de tiempo ({@code orTimeout}, {@code completeOnTimeout}).
 */
public final class CompletableFutureDemo {
    private static final Logger LOGGER = AppLogger.get(CompletableFutureDemo.class);


    /**
     * Usuario simulado.
     *
     * @param id identificador
     * @param name nombre
     */
    public record User(int id, String name) {
    }

    private CompletableFutureDemo() {
    }

    /**
     * Encadena y combina: dos tareas independientes corren en paralelo y {@code thenCombine} une sus resultados.
     *
     * @return el texto combinado
     */
    public static String pipeline() {
        CompletableFuture<String> language = CompletableFuture.supplyAsync(() -> "java").thenApply(String::toUpperCase);
        CompletableFuture<String> version = CompletableFuture.supplyAsync(() -> "21");
        return language.thenCombine(version, (name, number) -> name + " " + number).join();
    }

    private static CompletableFuture<User> fetchUser(int id) {
        return CompletableFuture.supplyAsync(() -> {
            Concurrent.sleep(30);
            return new User(id, "usuario" + id);
        });
    }

    private static CompletableFuture<List<String>> fetchOrders(User user) {
        return CompletableFuture.supplyAsync(() -> {
            Concurrent.sleep(30);
            return List.of("pedido-1 de " + user.name(), "pedido-2 de " + user.name());
        });
    }

    /**
     * {@code thenCompose}: el segundo paso DEPENDE del resultado del primero y devuelve otro CompletableFuture
     * (con {@code thenApply} quedaría un {@code CompletableFuture<CompletableFuture<...>>}).
     *
     * @param userId usuario
     * @return pedidos del usuario
     */
    public static List<String> composeUserOrders(int userId) {
        return fetchUser(userId).thenCompose(CompletableFutureDemo::fetchOrders).join();
    }

    /**
     * {@code allOf}: lanza todas las tareas a la vez y espera a que terminen todas.
     *
     * @param numbers números de entrada
     * @return cuadrados en el mismo orden
     */
    public static List<Integer> allOfSquares(List<Integer> numbers) {
        List<CompletableFuture<Integer>> futures = numbers.stream()
                .map(n -> CompletableFuture.supplyAsync(() -> {
                    Concurrent.sleep(30);
                    return n * n;
                }))
                .toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private static CompletableFuture<String> delayed(String name, long millis) {
        return CompletableFuture.supplyAsync(() -> {
            Concurrent.sleep(millis);
            return name;
        });
    }

    /**
     * {@code anyOf}: devuelve el resultado de la primera tarea que termine.
     *
     * @return nombre de la tarea ganadora
     */
    public static String anyOfFastest() {
        return (String) CompletableFuture.anyOf(delayed("lenta", 500), delayed("rápida", 20)).join();
    }

    /**
     * {@code exceptionally}: sustituye el error por un valor de respaldo. La excepción original llega envuelta en
     * {@link CompletionException}.
     *
     * @param fail si la tarea debe fallar
     * @return "ok" o el mensaje de recuperación
     */
    public static String recover(boolean fail) {
        return CompletableFuture.supplyAsync(() -> {
            if (fail) {
                throw new IllegalStateException("falló el servicio remoto");
            }
            return "ok";
        }).exceptionally(ex -> "recuperado: " + ex.getCause().getMessage()).join();
    }

    /**
     * {@code handle}: recibe siempre resultado y error (uno de los dos es nulo), como un try/catch/finally.
     *
     * @param fail si la tarea debe fallar
     * @return descripción del resultado
     */
    public static String handleBoth(boolean fail) {
        return CompletableFuture.supplyAsync(() -> {
            if (fail) {
                throw new IllegalArgumentException("dato inválido");
            }
            return 42;
        }).handle((value, error) -> error == null ? "valor " + value : "error: " + error.getCause().getMessage()).join();
    }

    /**
     * {@code completeOnTimeout}: si la tarea tarda demasiado se completa con un valor por defecto.
     *
     * @return el valor por defecto (la tarea lenta no alcanza a responder)
     */
    public static String completeOnTimeout() {
        return delayed("respuesta lenta", 1_000).completeOnTimeout("valor por defecto", 100, TimeUnit.MILLISECONDS).join();
    }

    /**
     * {@code orTimeout}: si la tarea tarda demasiado se completa con {@link TimeoutException}.
     *
     * @return {@code true} si el fallo fue por tiempo agotado
     */
    public static boolean orTimeoutFails() {
        try {
            delayed("respuesta lenta", 1_000).orTimeout(100, TimeUnit.MILLISECONDS).join();
            return false;
        } catch (CompletionException e) {
            return e.getCause() instanceof TimeoutException;
        }
    }

    /**
     * Ejecuta en un pool propio (en vez del {@code ForkJoinPool.commonPool()} por defecto), lo recomendable para tareas
     * que bloquean (E/S) porque así no se agotan los hilos compartidos.
     *
     * @return nombre del hilo que ejecutó la tarea
     */
    public static String threadNameInCustomPool() {
        AtomicInteger counter = new AtomicInteger();
        try (ExecutorService pool = Executors.newFixedThreadPool(2, r -> new Thread(r, "io-pool-" + counter.incrementAndGet()))) {
            return CompletableFuture.supplyAsync(() -> Thread.currentThread().getName(), pool).join();
        }
    }

    /** Ejecuta cada técnica y muestra el resultado. */
    public static void run() {
        LOGGER.info("supplyAsync + thenApply + thenCombine: " + pipeline());
        LOGGER.info("thenCompose (usuario -> pedidos): " + composeUserOrders(7));
        LOGGER.info("allOf (cuadrados en paralelo): " + allOfSquares(List.of(1, 2, 3, 4)));
        LOGGER.info("anyOf (la primera que termina): " + anyOfFastest());
        LOGGER.info("exceptionally sin error: " + recover(false) + " | con error: " + recover(true));
        LOGGER.info("handle sin error: " + handleBoth(false) + " | con error: " + handleBoth(true));
        LOGGER.info("completeOnTimeout: " + completeOnTimeout());
        LOGGER.info("orTimeout lanza TimeoutException: " + orTimeoutFails());
        LOGGER.info("Pool propio: " + threadNameInCustomPool());
    }
}
