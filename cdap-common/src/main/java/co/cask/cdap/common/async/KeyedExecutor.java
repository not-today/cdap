/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.async;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 * Wrapper around an ExecutorService that keeps track of submitted runnables by a key. Enforces that
 * only one runnable per key is ever running, and exposes methods to get the corresponding future for a key, and check
 * if there is a runnable for a given key.
 *
 * @param <K> type of key
 */
public class KeyedExecutor<K> {
  private final ExecutorService executorService;
  private final Map<K, Future<Void>> futures;
  private final Lock lock;

  public KeyedExecutor(ExecutorService executorService) {
    this.executorService = executorService;
    this.futures = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. If there is already a task
   * running for the key, the given task will not be executed, and the existing Future for the key will be returned.
   *
   * @param key the task key
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   */
  public Future<Void> submit(K key, Runnable task) {
    lock.lock();
    Future<Void> running = futures.get(key);
    if (running != null) {
      return running;
    }

    // latch prevents task from running before the future is placed in the map
    CountDownLatch latch = new CountDownLatch(1);
    Future<Void> future = executorService.submit(() -> {
      try {
        latch.await();
        task.run();
      } catch (InterruptedException e) {
        // interrupted while waiting for latch, run will be skipped
      }

      lock.lock();
      futures.remove(key);
      lock.unlock();
    }, null);

    futures.put(key, future);
    lock.unlock();

    latch.countDown();
    return future;
  }

  /**
   * Gets the Future for the specified key.
   *
   * @param key the key
   * @return the future for the key, or null if none exists
   */
  @Nullable
  public Future<Void> getFuture(K key) {
    lock.lock();
    Future<Void> future = futures.get(key);
    lock.unlock();
    return future;
  }

  /**
   * Returns whether there is a running task for the key
   *
   * @param key the key
   * @return whether there is a running task for the key
   */
  public boolean isRunning(K key) {
    lock.lock();
    boolean isRunning = futures.containsKey(key);
    lock.unlock();
    return isRunning;
  }

}
