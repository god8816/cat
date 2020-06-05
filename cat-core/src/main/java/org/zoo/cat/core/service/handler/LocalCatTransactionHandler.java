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
import org.zoo.cat.annotation.Cat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.bean.entity.CatInvocation;
import org.zoo.cat.common.bean.entity.CatParticipant;
import org.zoo.cat.common.enums.CatActionEnum;
import org.zoo.cat.common.utils.StringUtils;
import org.zoo.cat.core.service.CatTransactionHandler;
import org.zoo.cat.core.service.executor.CatTransactionExecutor;

import java.lang.reflect.Method;

/**
 * InlineCatTransactionHandler.
 * This is the method annotated by TCC within an actor.
 *
 * @author dzc
 */
@Component
public class LocalCatTransactionHandler implements CatTransactionHandler {

    private final CatTransactionExecutor catTransactionExecutor;

    /**
     * Instantiates a new Local cat transaction handler.
     *
     * @param catTransactionExecutor the cat transaction executor
     */
    @Autowired
    public LocalCatTransactionHandler(final CatTransactionExecutor catTransactionExecutor) {
        this.catTransactionExecutor = catTransactionExecutor;
    }

    @Override
    public Object handler(final ProceedingJoinPoint point, final CatTransactionContext context) throws Throwable {
        if (CatActionEnum.TRYING.getCode() == context.getAction()) {
            MethodSignature signature = (MethodSignature) point.getSignature();
            Method method = signature.getMethod();
            Class<?> clazz = point.getTarget().getClass();
            Object[] args = point.getArgs();
            final Cat cat = method.getAnnotation(Cat.class);
            CatInvocation confirmInvocation = null;
            String confirmMethodName = cat.confirmMethod();
            String cancelMethodName = cat.cancelMethod();
            if (StringUtils.isNoneBlank(confirmMethodName)) {
                confirmInvocation = new CatInvocation(clazz, confirmMethodName, method.getParameterTypes(), args);
            }
            CatInvocation cancelInvocation = null;
            if (StringUtils.isNoneBlank(cancelMethodName)) {
                cancelInvocation = new CatInvocation(clazz, cancelMethodName, method.getParameterTypes(), args);
            }
            final CatParticipant catParticipant = new CatParticipant(context.getTransId(),
                    confirmInvocation, cancelInvocation);
            catTransactionExecutor.registerByNested(context.getTransId(), catParticipant);
        }
        return point.proceed();
    }

}
