
package com.michael.zookeeper.zk_client.lock;

import java.util.concurrent.Semaphore;

public class BoundSemaphore {
    private Thread thread;
    private Semaphore semaphore;

    public BoundSemaphore(Thread thread, Semaphore semaphore) {
        this.thread = thread;
        this.semaphore = semaphore;
    }

    public Thread getThread() {
        return thread;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }
}
