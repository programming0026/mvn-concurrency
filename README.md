# Concurrencia en Java 21: Fork/Join, CompletableFuture, Executors y atómicos

Guía práctica de las herramientas de concurrencia de `java.util.concurrent`, con **Maven** y **Java 21**. Cada tema es una clase con métodos pequeños y comentados, que `Main` ejecuta mostrando el resultado, y que las pruebas verifican.

> Los **hilos virtuales** de Java 21 tienen su propio ejemplo: `mvn-virtual-threads`.

## Contenido

| Paquete | Qué muestra |
|---------|-------------|
| `threads` | `Thread`, `join`, `interrupt` (cancelación cooperativa), hilos demonio, `volatile` y la **condición de carrera** de un contador sin sincronizar frente a `synchronized` |
| `executors` | Pools con `try-with-resources` (Java 19+), `Callable` + `Future`, `invokeAll`, `invokeAny`, `get` con timeout y `cancel`, errores en `ExecutionException`, `scheduleAtFixedRate`, políticas de rechazo (`AbortPolicy`, `CallerRunsPolicy`) |
| `completable` | `supplyAsync`, `thenApply`, `thenCombine`, `thenCompose`, `allOf`, `anyOf`, `exceptionally`, `handle`, `completeOnTimeout`, `orTimeout` y pool propio |
| `forkjoin` | `RecursiveTask` (suma), `RecursiveAction` (cuadrados en el lugar y **merge sort paralelo**), `fork`/`join`/`invokeAll`, umbral y comparación con la versión secuencial |
| `atomic` | `AtomicInteger`, `LongAdder`, `accumulateAndGet`, `compareAndSet` (ejecutar algo una sola vez), `AtomicReference` con objetos inmutables y el problema **ABA** con `AtomicStampedReference` |
| `locks` | `ReentrantLock`, `tryLock` con timeout, `Condition`, `ReadWriteLock` (caché) y `StampedLock` con lectura optimista |
| `synchronizers` | `CountDownLatch` (pistola de salida), `CyclicBarrier` (fases), `Semaphore` (límite de concurrencia) |
| `collections` | `ConcurrentHashMap.merge`, `BlockingQueue` (productor-consumidor con "píldora de veneno") y `CopyOnWriteArrayList` frente al `ConcurrentModificationException` de `ArrayList` |

## Cuál usar

| Necesito... | Usa |
|-------------|-----|
| Ejecutar tareas sin crear hilos a mano | `ExecutorService` |
| Encadenar pasos asíncronos y combinar resultados sin bloquear | `CompletableFuture` |
| Dividir un cálculo CPU intensivo en partes | Fork/Join (o `parallelStream`, que lo usa por debajo) |
| Un contador o bandera compartida | `AtomicInteger` / `AtomicBoolean`; `LongAdder` si hay mucha contención |
| Proteger varios pasos como una unidad | `synchronized` o `ReentrantLock` |
| Muchas lecturas y pocas escrituras | `ReadWriteLock` / `StampedLock` / `CopyOnWriteArrayList` |
| Esperar a que ocurran N eventos | `CountDownLatch` |
| Limitar cuántos hilos usan un recurso | `Semaphore` |
| Pasar datos entre hilos | `BlockingQueue` |
| Muchísimas tareas que esperan E/S | Hilos virtuales (ver `mvn-virtual-threads`) |

## Estructura

```text
src/main/java/com/juniverse/concurrency/
├── Main.java                 # ejecuta todas las demostraciones
├── util/Concurrent.java      # runConcurrently, sleep, timeMillis
├── threads/ThreadsDemo.java
├── executors/ExecutorsDemo.java
├── completable/CompletableFutureDemo.java
├── forkjoin/ForkJoinDemo.java
├── atomic/AtomicDemo.java
├── locks/                    # LocksDemo, ReadWriteCache, StampedPoint
├── synchronizers/SynchronizersDemo.java
└── collections/ConcurrentCollectionsDemo.java
```

## Requisitos

- JDK 21 y Maven 3.9 o superior.

## Ejecutar

```bash
mvn clean package
java -jar target/mvn-concurrency-1.0.0-SNAPSHOT-launcher.jar
```

(también `mvn compile exec:java` si tienes el plugin `exec`). Los tiempos de Fork/Join y el contador sin sincronizar varían en cada ejecución y según tus núcleos.

## Pruebas

```bash
mvn test
```

18 pruebas JUnit 5 (`ConcurrencyTest`, agrupadas con `@Nested` por tema). Todas llevan `@Timeout(30)` para que un interbloqueo no cuelgue la compilación, y solo afirman cosas deterministas (por ejemplo, que el contador seguro es exacto y que el inseguro nunca supera el esperado, no que "falle").

## Reglas de oro

- **Comparte lo mínimo**: lo inmutable no necesita sincronización.
- Libera los locks siempre en `finally` y toma varios locks en el mismo orden para evitar interbloqueos.
- No ejecutes tareas bloqueantes en el `commonPool` (afecta a los streams paralelos y a `CompletableFuture`): pásales tu propio `Executor`.
- Cierra los pools (`try-with-resources` o `shutdown`); si no, la JVM puede no terminar.
- Respeta las interrupciones: restablece la bandera (`Thread.currentThread().interrupt()`) si no puedes propagar la excepción.
- Mide antes de paralelizar: para tareas pequeñas el coste de coordinar es mayor que la ganancia.

## Licencia

Código de ejemplo de uso libre con fines educativos.
