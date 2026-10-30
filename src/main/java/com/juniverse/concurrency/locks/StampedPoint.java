package com.juniverse.concurrency.locks;

import java.util.concurrent.locks.StampedLock;

/**
 * Punto 2D con {@link StampedLock}. La <b>lectura optimista</b> no bloquea: se lee, y después se comprueba con
 * {@code validate} que nadie escribió mientras tanto; si falló, se repite con un bloqueo de lectura normal.
 */
public class StampedPoint {

    private final StampedLock lock = new StampedLock();
    private double x;
    private double y;

    /**
     * Mueve el punto (escritura exclusiva).
     *
     * @param deltaX desplazamiento en x
     * @param deltaY desplazamiento en y
     */
    public void move(double deltaX, double deltaY) {
        long stamp = lock.writeLock();
        try {
            x += deltaX;
            y += deltaY;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Distancia al origen usando lectura optimista.
     *
     * @return distancia euclidiana
     */
    public double distanceFromOrigin() {
        long stamp = lock.tryOptimisticRead();
        double currentX = x;
        double currentY = y;
        if (!lock.validate(stamp)) {      // alguien escribió: se lee de nuevo con bloqueo
            stamp = lock.readLock();
            try {
                currentX = x;
                currentY = y;
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return Math.hypot(currentX, currentY);
    }
}
