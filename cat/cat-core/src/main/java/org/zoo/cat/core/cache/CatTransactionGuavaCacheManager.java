/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zoo.cat.core.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.utils.StringUtils;
import org.zoo.cat.core.coordinator.CatCoordinatorService;
import org.zoo.cat.core.helper.SpringBeanUtils;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * use google guava cache.
 *
 * @author dzc
 */
public final class CatTransactionGuavaCacheManager {

    private static final int MAX_COUNT = 1000000;

    private static final LoadingCache<String, CatTransaction> LOADING_CACHE =
            CacheBuilder.newBuilder().maximumWeight(MAX_COUNT)
                    .weigher((Weigher<String, CatTransaction>) (string, catTransaction) -> getSize())
                    .build(new CacheLoader<String, CatTransaction>() {
                        @Override
                        public CatTransaction load(final String key) {
                            return cacheCatTransaction(key);
                        }
                    });

    private static CatCoordinatorService coordinatorService = SpringBeanUtils.getInstance().getBean(CatCoordinatorService.class);

    private static final CatTransactionGuavaCacheManager TCC_TRANSACTION_CACHE_MANAGER = new CatTransactionGuavaCacheManager();

    private CatTransactionGuavaCacheManager() {

    }

    /**
     * CatTransactionCacheManager.
     *
     * @return CatTransactionCacheManager
     */
    public static CatTransactionGuavaCacheManager getInstance() {
        return TCC_TRANSACTION_CACHE_MANAGER;
    }

    private static int getSize() {
        return (int) LOADING_CACHE.size();
    }

    private static CatTransaction cacheCatTransaction(final String key) {
        return Optional.ofNullable(coordinatorService.findByTransId(key)).orElse(new CatTransaction());
    }

    /**
     * cache catTransaction.
     *
     * @param catTransaction {@linkplain CatTransaction}
     */
    public void cacheCatTransaction(final CatTransaction catTransaction) {
        LOADING_CACHE.put(catTransaction.getTransId(), catTransaction);
    }

    /**
     * acquire catTransaction.
     *
     * @param key this guava key.
     * @return {@linkplain CatTransaction}
     */
    public CatTransaction getCatTransaction(final String key) {
        try {
            return LOADING_CACHE.get(key);
        } catch (ExecutionException e) {
            return new CatTransaction();
        }
    }

    /**
     * remove guava cache by key.
     *
     * @param key guava cache key.
     */
    public void removeByKey(final String key) {
        if (StringUtils.isNoneBlank(key)) {
            LOADING_CACHE.invalidate(key);
        }
    }

}
