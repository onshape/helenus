package net.helenus.support;

@FunctionalInterface
public interface CheckedRunnable {
    void run() throws Throwable;
}
