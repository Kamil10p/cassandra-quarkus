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

import com.datastax.oss.quarkus.tests.dao.ProductDao;
import com.datastax.oss.quarkus.tests.entity.Product;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class ProductService {

  @Inject CompletionStage<ProductDao> daoCompletionStage;

  public CompletionStage<Void> create(Product product) {
    return daoCompletionStage.thenCompose(dao -> dao.create(product));
  }

  public CompletionStage<Void> update(Product product) {
    return daoCompletionStage.thenCompose(dao -> dao.update(product));
  }

  public CompletionStage<Void> delete(UUID productId) {
    return daoCompletionStage.thenCompose(dao -> dao.delete(productId));
  }

  public CompletionStage<Product> findById(UUID productId) {
    return daoCompletionStage.thenCompose(dao -> dao.findById(productId));
  }

  public CompletionStage<List<Product>> findAll() {

    return daoCompletionStage.thenApply(
        dao -> {
          List<Product> products = new ArrayList<>();
          for (Product product : dao.findAll()) {
            products.add(product);
          }
          return products;
        });
  }
}
