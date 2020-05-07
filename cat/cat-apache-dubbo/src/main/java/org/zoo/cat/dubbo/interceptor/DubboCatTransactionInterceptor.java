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

package org.zoo.cat.dubbo.interceptor;

import org.apache.dubbo.rpc.RpcContext;
import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.core.concurrent.threadlocal.CatTransactionContextLocal;
import org.zoo.cat.core.interceptor.CatTransactionInterceptor;
import org.zoo.cat.core.mediator.RpcMediator;
import org.zoo.cat.core.service.CatTransactionAspectService;

import java.util.Objects;

/**
 * The DubboCatTransactionInterceptor.
 *
 * @author dzc
 */
@Component
public class DubboCatTransactionInterceptor implements CatTransactionInterceptor {

    private final CatTransactionAspectService catTransactionAspectService;

    @Autowired
    public DubboCatTransactionInterceptor(final CatTransactionAspectService catTransactionAspectService) {
        this.catTransactionAspectService = catTransactionAspectService;
    }

    @Override
    public Object interceptor(final ProceedingJoinPoint pjp) throws Throwable {
        CatTransactionContext catTransactionContext =
                RpcMediator.getInstance().acquire(RpcContext.getContext()::getAttachment);
        if (Objects.isNull(catTransactionContext)) {
            catTransactionContext = CatTransactionContextLocal.getInstance().get();
        }
        return catTransactionAspectService.invoke(catTransactionContext, pjp);
    }
}
