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
package com.datastax.oss.quarkus.runtime.api.reactive;

import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveResultSet;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import io.smallrye.mutiny.Multi;

/**
 * A marker interface for {@code Multi<ReactiveRow>} results returned by {@link QuarkusCqlSession},
 * when continuous paging is used.
 *
 * <p>Note: continuous paging is only available for DataStax Enterprise (DSE) 5.1 and higher.
 *
 * @see QuarkusCqlSession#executeContinuouslyReactive(String)
 * @see QuarkusCqlSession#executeContinuouslyReactive(Statement)
 * @see ContinuousReactiveResultSet
 */
public interface MutinyContinuousReactiveResultSet
    extends Multi<ReactiveRow>, ContinuousReactiveResultSet, MutinyReactiveQueryMetadata {}
