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

package org.zoo.cat.springcloud.feign;

import org.zoo.cat.annotation.Cat;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.bean.entity.CatInvocation;
import org.zoo.cat.common.bean.entity.CatParticipant;
import org.zoo.cat.common.enums.CatActionEnum;
import org.zoo.cat.common.enums.CatRoleEnum;
import org.zoo.cat.common.utils.StringUtils;
import org.zoo.cat.core.concurrent.threadlocal.CatTransactionContextLocal;
import org.zoo.cat.core.helper.SpringBeanUtils;
import org.zoo.cat.core.service.executor.CatTransactionExecutor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * CatFeignHandler.
 *
 * @author dzc
 */
public class CatFeignHandler implements InvocationHandler {

    private InvocationHandler delegate;

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if (Object.class.equals(method.getDeclaringClass())) {
            return method.invoke(this, args);
        } else {
            final Cat cat = method.getAnnotation(Cat.class);
            if (Objects.isNull(cat)) {
                return this.delegate.invoke(proxy, method, args);
            }
            try {
                final CatTransactionContext catTransactionContext = CatTransactionContextLocal.getInstance().get();
                if (Objects.nonNull(catTransactionContext)) {
                    if (catTransactionContext.getRole() == CatRoleEnum.LOCAL.getCode()) {
                        catTransactionContext.setRole(CatRoleEnum.INLINE.getCode());
                    }
                }
                final CatTransactionExecutor catTransactionExecutor =
                        SpringBeanUtils.getInstance().getBean(CatTransactionExecutor.class);
                final Object invoke = delegate.invoke(proxy, method, args);
                final CatParticipant catParticipant = buildParticipant(cat, method, args, catTransactionContext);
                if (catTransactionContext.getRole() == CatRoleEnum.INLINE.getCode()) {
                    catTransactionExecutor.registerByNested(catTransactionContext.getTransId(),
                            catParticipant);
                } else {
                    catTransactionExecutor.enlistParticipant(catParticipant);
                }
                return invoke;
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                throw throwable;
            }
        }
    }

    private CatParticipant buildParticipant(final Cat cat, final Method method, final Object[] args,
                                              final CatTransactionContext catTransactionContext) {
        if (Objects.isNull(catTransactionContext)
                || (CatActionEnum.TRYING.getCode() != catTransactionContext.getAction())) {
            return null;
        }
        String confirmMethodName = cat.confirmMethod();
        if (StringUtils.isBlank(confirmMethodName)) {
            confirmMethodName = method.getName();
        }
        String cancelMethodName = cat.cancelMethod();
        if (StringUtils.isBlank(cancelMethodName)) {
            cancelMethodName = method.getName();
        }
        final Class<?> declaringClass = method.getDeclaringClass();
        CatInvocation confirmInvocation = new CatInvocation(declaringClass, confirmMethodName, method.getParameterTypes(), args);
        CatInvocation cancelInvocation = new CatInvocation(declaringClass, cancelMethodName, method.getParameterTypes(), args);
        return new CatParticipant(catTransactionContext.getTransId(), confirmInvocation, cancelInvocation);
    }

    void setDelegate(InvocationHandler delegate) {
        this.delegate = delegate;
    }

}
