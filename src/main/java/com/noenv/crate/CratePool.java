/*
 * Copyright (C) 2026 Lukas Prettenthaler
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.noenv.crate;

import com.noenv.crate.codec.CrateQuery;
import com.noenv.crate.impl.CrateHttpConnection;
import com.noenv.crate.impl.CrateConnectionFactory;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.http.PoolOptions;
import io.vertx.core.json.JsonObject;

import java.util.concurrent.TimeUnit;

public class CratePool {

  private final CrateConnectionFactory factory;
  private final ContextInternal context;

  private CratePool(CrateConnectionFactory factory, ContextInternal context) {
    this.factory = factory;
    this.context = context;
  }

  public static CratePool pool(Vertx vertx, CrateConnectOptions connectOptions) {
    return pool(vertx, connectOptions, new PoolOptions().setHttp1MaxSize(12));
  }

  public static CratePool pool(Vertx vertx, CrateConnectOptions connectOptions, PoolOptions poolOptions) {
    ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
    CrateConnectionFactory factory = new CrateConnectionFactory(context, connectOptions, poolOptions);
    return new CratePool(factory, context);
  }

  public Observable<JsonObject> queryObservable(CrateQuery query) {
    return Observable.create(emitter ->
      factory.acquireConnection()
        .onSuccess(conn ->
          conn.sendQuery(context, query)
            .doOnTerminate(conn::close)
            .subscribe(emitter::onNext, emitter::onError, emitter::onComplete)
        )
        .onFailure(emitter::onError)
    );
  }

  public Future<Void> close() {
    return factory.shutdown(10L, TimeUnit.SECONDS);
  }
}
