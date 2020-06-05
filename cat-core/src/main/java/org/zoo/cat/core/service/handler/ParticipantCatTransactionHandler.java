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
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.enums.CatActionEnum;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.utils.DefaultValueUtils;
import org.zoo.cat.core.cache.CatTransactionGuavaCacheManager;
import org.zoo.cat.core.concurrent.threadlocal.CatTransactionContextLocal;
import org.zoo.cat.core.service.CatTransactionHandler;
import org.zoo.cat.core.service.executor.CatTransactionExecutor;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * Participant Handler.
 *
 * @author dzc
 */
@Component
public class ParticipantCatTransactionHandler implements CatTransactionHandler {

    private final CatTransactionExecutor catTransactionExecutor;
    

    /**
     * Instantiates a new Participant cat transaction handler.
     *
     * @param catTransactionExecutor the cat transaction executor
     */
    @Autowired
    public ParticipantCatTransactionHandler(final CatTransactionExecutor catTransactionExecutor) {
        this.catTransactionExecutor = catTransactionExecutor;
    }


	@Override
    public Object handler(final ProceedingJoinPoint point, final CatTransactionContext context) throws Throwable {
     	CatTransaction catTransaction = null;
     	final CatTransaction currentTransaction;
        switch (CatActionEnum.acquireByCode(context.getAction())) {
            case TRYING:
                try {
                    catTransaction = catTransactionExecutor.preTryParticipant(context, point);
                    final Object proceed = point.proceed();
                    catTransaction.setStatus(CatActionEnum.TRYING.getCode());
                    //update log status to try
                    catTransactionExecutor.updateStatus(catTransaction);
                    return proceed;
                } catch (Throwable throwable) {
                    //if exception ,delete log.
                    catTransactionExecutor.deleteTransaction(catTransaction);
                    throw throwable;
                } finally {
                    CatTransactionContextLocal.getInstance().remove();
                }
            case CONFIRMING:
                currentTransaction = CatTransactionGuavaCacheManager
                        .getInstance().getCatTransaction(context.getTransId());
                return catTransactionExecutor.confirm(currentTransaction);
            case CANCELING:
                currentTransaction = CatTransactionGuavaCacheManager
                        .getInstance().getCatTransaction(context.getTransId());
                return catTransactionExecutor.cancel(currentTransaction);
            case NOTICEING:
            	    try {
            	     	catTransaction = catTransactionExecutor.preNoticeParticipant(context, point);
            	      	Long startTime = System.currentTimeMillis();
                    final Object proceed = point.proceed();
                    Long endTime = System.currentTimeMillis();
                    if(Objects.nonNull(catTransaction)&&catTransaction.getTimeoutMills()>0 && endTime-startTime>catTransaction.getTimeoutMills()) {
                      	throw new CatException("method "+catTransaction.getTargetMethod()+" timeout..");
                    }
                    return proceed;
                } catch (Throwable throwable) {
                    //if notice log.
                    throw throwable;
                } finally {
                    CatTransactionContextLocal.getInstance().remove();
                }
            default:
                break;
        }
        Method method = ((MethodSignature) (point.getSignature())).getMethod();
        return DefaultValueUtils.getDefaultValue(method.getReturnType());
    }

}
