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

package org.zoo.cat.core.disruptor.publisher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.SmartApplicationListener;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.enums.EventTypeEnum;
import org.zoo.cat.core.concurrent.ConsistentHashSelector;
import org.zoo.cat.core.concurrent.SingletonExecutor;
import org.zoo.cat.core.coordinator.CatCoordinatorService;
import org.zoo.cat.core.disruptor.DisruptorProviderManage;
import org.zoo.cat.core.disruptor.event.CatTransactionEvent;
import org.zoo.cat.core.disruptor.handler.CatConsumerLogDataHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * event publisher.
 *
 * @author dzc
 */
@Component
public class CatTransactionEventPublisher implements SmartApplicationListener {

    private volatile AtomicBoolean isInit = new AtomicBoolean(false);

    private DisruptorProviderManage<CatTransactionEvent> disruptorProviderManage;

    private final CatCoordinatorService coordinatorService;

    private final CatConfig catConfig;

    @Autowired
    public CatTransactionEventPublisher(final CatCoordinatorService coordinatorService,
                                          final CatConfig catConfig) {
        this.coordinatorService = coordinatorService;
        this.catConfig = catConfig;
    }

    /**
     * disruptor start.
     *
     * @param bufferSize this is disruptor buffer size.
     * @param threadSize this is disruptor consumer thread size.
     */
    private void start(final int bufferSize, final int threadSize) {
        List<SingletonExecutor> selects = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {
            selects.add(new SingletonExecutor("cat-log-disruptor" + i));
        }
        ConsistentHashSelector selector = new ConsistentHashSelector(selects);
        disruptorProviderManage =
                new DisruptorProviderManage<>(
                        new CatConsumerLogDataHandler(selector, coordinatorService), 1, bufferSize);
        disruptorProviderManage.startup();
    }

    /**
     * publish disruptor event.
     *
     * @param catTransaction {@linkplain CatTransaction }
     * @param type             {@linkplain EventTypeEnum}
     */
    public void publishEvent(final CatTransaction catTransaction, final int type) {
        CatTransactionEvent event = new CatTransactionEvent();
        event.setType(type);
        event.setCatTransaction(catTransaction);
        disruptorProviderManage.getProvider().onData(event);
    }

    @Override
    public boolean supportsEventType(Class<? extends ApplicationEvent> aClass) {
        return aClass == ContextRefreshedEvent.class;
    }

    @Override
    public boolean supportsSourceType(Class<?> aClass) {
        return true;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (!isInit.compareAndSet(false, true)) {
            return;
        }
        start(catConfig.getBufferSize(), catConfig.getConsumerThreads());
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE - 1;
    }
}
