/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import com.twitter.algebird.Monoid
import com.twitter.bijection._
import com.twitter.bijection.netty.Implicits._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.Client
import com.twitter.storehaus.Store
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.redis.{RedisLongStore, RedisStore}
import com.twitter.summingbird.batch.BatchID
import org.jboss.netty.buffer.ChannelBuffer

/**
 * TODO: Delete when https://github.com/twitter/storehaus/pull/121 is
 * merged into Storehaus and Storehaus sees its next release. This
 * pull req will make it easier to create Memcache store instances.
 */
object Redis {
  val DEFAULT_TIMEOUT = 1.seconds

  def client = {
    Client("localhost:6379")
  }

  /**
   * Returns a function that encodes a key to a Memcache key string
   * given a unique namespace string.
   */

  def keyEncoder[T:Codec]:T => ChannelBuffer = { key =>
    val inj = Injection.connect[T, Array[Byte], ChannelBuffer]
    inj(key)
  }

  def store[K: Codec, V: Codec](keyPrefix: String): Store[K, V] = {
    implicit val valueToBuf = Injection.connect[V, Array[Byte], ChannelBuffer]
    RedisStore(client)
      .convert(keyEncoder[K])
  }

  def mergeable[K: Codec, V: Codec: Monoid](keyPrefix: String): MergeableStore[K, V] =
    MergeableStore.fromStore(store[K, V](keyPrefix))

}
