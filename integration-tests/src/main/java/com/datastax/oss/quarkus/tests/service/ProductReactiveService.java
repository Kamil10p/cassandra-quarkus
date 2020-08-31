/*
 * Copyright DataStax, Inc.
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
 */
package com.datastax.oss.quarkus.tests.service;

import com.datastax.oss.quarkus.tests.dao.ProductReactiveDao;
import com.datastax.oss.quarkus.tests.entity.Product;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class ProductReactiveService {

  @Inject CompletionStage<ProductReactiveDao> daoCompletionStage;

  public Uni<Void> create(Product product) {
    try {
      // todo do convert:
      // CompletionStage<Uni<Void>> uniCompletionStage = daoCompletionStage.thenApply(dao ->
      // dao.create(product));
      // to Uni<Void> uniCompletionStage (same for rest of the methods)

      return daoCompletionStage.toCompletableFuture().get().create(product);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("msg");
    }
  }

  public Uni<Void> update(Product product) {
    try {
      return daoCompletionStage.toCompletableFuture().get().update(product);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("msg");
    }
  }

  public Uni<Void> delete(UUID productId) {
    try {
      return daoCompletionStage.toCompletableFuture().get().delete(productId);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("msg");
    }
  }

  public Uni<Product> findById(UUID productId) {
    try {
      return daoCompletionStage.toCompletableFuture().get().findById(productId);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("msg");
    }
  }

  public Multi<Product> findAll() {
    try {
      return daoCompletionStage.toCompletableFuture().get().findAll();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("msg");
    }
  }
}
