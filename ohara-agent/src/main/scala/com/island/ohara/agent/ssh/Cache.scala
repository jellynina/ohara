/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.island.ohara.agent.ssh

import java.util.Objects
import java.util.concurrent.{ArrayBlockingQueue, Executors, TimeUnit}

import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.util.{CommonUtil, Releasable, ReleaseOnce}
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * a similar helper used to cache all cluster information for ssh collie.
  * We don't include google Guava since we don't need the complicated component.
  * @tparam T obj type
  */
trait Cache[T] extends Releasable {

  /**
    * get the cached value if it is not expired. Otherwise, it return the updated value
    * @return cached value or updated value
    */
  def get(): Future[T]

  /**
    * Overlook the cached value. Just return the latest value from updater.
    * @return latest value
    */
  def latest(): Future[T]

  /**
    * request to update cache.
    * @return true if the request is accepted
    */
  def requestUpdate(): Boolean
}

object Cache {
  private[this] val LOG = Logger(Cache.getClass)
  def builder[T](): Builder[T] = new Builder[T]

  /**
    * a do-nothing cache.
    * @param updater updater
    * @tparam T type of cached data
    * @return a fake and cheap cache.
    */
  def empty[T](updater: () => Future[T]): Cache[T] = new Cache[T] {
    override def get(): Future[T] = updater()

    override def latest(): Future[T] = updater()

    override def requestUpdate(): Boolean = true

    override def close(): Unit = {
      // do nothing
    }
  }

  class Builder[T] private[ssh] {
    private[this] var expiredTime: Duration = 3 seconds
    private[this] var updater: () => Future[T] = _
    private[this] var obj: Option[T] = None

    /**
      * the expired time of cached data.
      * @param expiredTime expired time
      * @return this builder
      */
    @Optional("default is 3 seconds")
    def expiredTime(expiredTime: Duration): Builder[T] = {
      this.expiredTime = Objects.requireNonNull(expiredTime)
      this
    }

    /**
      * the function to update cache.
      * @param f fetcher
      * @return this builder
      */
    def fetcher(f: () => T): Builder[T] = updater(() => Future.successful(f()))

    /**
      * the function to update cache.
      * @param updater updater
      * @return this builder
      */
    def updater(updater: () => Future[T]): Builder[T] = {
      this.updater = Objects.requireNonNull(updater)
      this
    }

    /**
      * the initial value of cache. THIS IS REQUIRED!!!
      * @param obj initial value
      * @return this builder
      */
    def default(obj: T): Builder[T] = {
      this.obj = Some(Objects.requireNonNull(obj))
      this
    }

    def build(): Cache[T] = new CacheImpl[T](
      defaultValue = obj.getOrElse(throw new NullPointerException("default value is required")),
      expiredTime = expiredTime,
      updater = updater
    )
  }

  private[this] class CacheImpl[T](defaultValue: T, expiredTime: Duration, updater: () => Future[T])
      extends ReleaseOnce
      with Cache[T] {
    // small queue prevent us from destroying remote nodes
    private[this] val requests = new ArrayBlockingQueue[Long](1)
    private[this] val lock = new Object()
    @volatile private[this] var obj: T = defaultValue
    @volatile private[this] var lastUpdate: Long = CommonUtil.current()
    private[this] val executor = {
      val exec = Executors.newSingleThreadExecutor()
      exec.execute(() =>
        try {
          while (!isClosed) try {
            // we ignore the returned value
            requests.poll(expiredTime.toMillis, TimeUnit.MILLISECONDS)
            // make all thread to wait updating of cache
            lastUpdate = -1
            // TODO: 30 seconds should be enough to fetch result ?
            obj = Await.result(updater(), 30 seconds)
            lastUpdate = CommonUtil.current()
          } catch {
            case e: InterruptedException =>
              LOG.info("we are closing this thread")
            case e: Throwable =>
              LOG.error("failed to update cache", e)
          } finally {
            lock.synchronized {
              lock.notifyAll()
            }
            requests.clear()
          }
        } finally LOG.info(s"cache thread is gone"))
      exec
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    override def get(): Future[T] = Future {
      var local = obj
      while (CommonUtil.current() > lastUpdate + expiredTime.toMillis) {
        // we may be stuck with this loop if thread is gone. Hence, we need this check!
        if (isClosed) throw new IllegalStateException("cache is closed!!!")
        lock.synchronized {
          // we all hate wait ...
          lock.wait(1 * 1000)
        }
        local = obj
      }
      if (isClosed) throw new IllegalStateException("cache is closed!!!")
      local
    }

    override def requestUpdate(): Boolean =
      if (isClosed) throw new IllegalStateException("cache is closed!!!") else requests.offer(CommonUtil.current())

    override protected def doClose(): Unit = {
      executor.shutdownNow()
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) throw new RuntimeException("failed to close clusters cache")
    }

    override def latest(): Future[T] =
      if (isClosed) throw new IllegalStateException("cache is closed!!!") else updater()
  }
}
