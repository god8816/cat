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

package org.zoo.cat.core.service.handler;

import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.SmartApplicationListener;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.enums.CatActionEnum;
import org.zoo.cat.core.concurrent.threadlocal.CatTransactionContextLocal;
import org.zoo.cat.core.disruptor.DisruptorProviderManage;
import org.zoo.cat.core.disruptor.handler.CatConsumerTransactionDataHandler;
import org.zoo.cat.core.service.CatTransactionHandler;
import org.zoo.cat.core.service.CatTransactionHandlerAlbum;
import org.zoo.cat.core.service.executor.CatTransactionExecutor;

/**
 * this is cat transaction starter.
 *
 * @author dzc
 */
@Component
public class StarterCatTransactionHandler implements CatTransactionHandler, SmartApplicationListener {

    private final CatTransactionExecutor catTransactionExecutor;

    private final CatConfig catConfig;

    private DisruptorProviderManage<CatTransactionHandlerAlbum> disruptorProviderManage;

    /**
     * Instantiates a new Starter cat transaction handler.
     *
     * @param catTransactionExecutor the cat transaction executor
     * @param catConfig              the cat config
     */
    @Autowired
    public StarterCatTransactionHandler(final CatTransactionExecutor catTransactionExecutor, final CatConfig catConfig) {
        this.catTransactionExecutor = catTransactionExecutor;
        this.catConfig = catConfig;
    }

    @Override
    public Object handler(final ProceedingJoinPoint point, final CatTransactionContext context) throws Throwable {
        Object returnValue;
        try {
            CatTransaction catTransaction = catTransactionExecutor.preTry(point);
            try {
                //execute try
                returnValue = point.proceed();
                catTransaction.setStatus(CatActionEnum.TRYING.getCode());
                catTransactionExecutor.updateStatus(catTransaction);
            } catch (Throwable throwable) {
                //if exception ,execute cancel
                final CatTransaction currentTransaction = catTransactionExecutor.getCurrentTransaction();
                disruptorProviderManage.getProvider().onData(() -> catTransactionExecutor.cancel(currentTransaction));
                throw throwable;
            }
            //execute confirm
            final CatTransaction currentTransaction = catTransactionExecutor.getCurrentTransaction();
            disruptorProviderManage.getProvider().onData(() -> catTransactionExecutor.confirm(currentTransaction));
        } finally {
            CatTransactionContextLocal.getInstance().remove();
            catTransactionExecutor.remove();
        }
        return returnValue;
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
        if (catConfig.getStarted()) {
            disruptorProviderManage = new DisruptorProviderManage<>(new CatConsumerTransactionDataHandler(),
                    catConfig.getAsyncThreads(),
                    DisruptorProviderManage.DEFAULT_SIZE);
            disruptorProviderManage.startup();
        }
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE - 2;
    }
}
