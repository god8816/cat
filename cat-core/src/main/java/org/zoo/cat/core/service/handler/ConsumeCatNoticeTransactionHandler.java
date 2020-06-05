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

import java.util.Objects;

import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.core.service.CatTransactionHandler;
import org.zoo.cat.core.service.executor.CatTransactionExecutor;

/**
 * ConsumeCatNoticeTransactionHandler.
 *
 * @author dzc
 */
@Component
public class ConsumeCatNoticeTransactionHandler implements CatTransactionHandler {
	
	@Autowired
	private  CatTransactionExecutor catTransactionExecutor;

    @Override
    public Object handler(final ProceedingJoinPoint point, final CatTransactionContext context) throws Throwable {
     	CatTransaction catTransaction = catTransactionExecutor.preNoticeParticipant(context, point);
      	Long startTime = System.currentTimeMillis();
        final Object proceed = point.proceed();
        Long endTime = System.currentTimeMillis();
        if(Objects.nonNull(catTransaction)&&catTransaction.getTimeoutMills()>0 && endTime-startTime>catTransaction.getTimeoutMills()) {
          	throw new CatException("method "+catTransaction.getTargetMethod()+" timeout..");
        }
        return proceed;
    }
}
