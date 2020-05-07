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

package org.zoo.cat.core.service.impl;

import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.core.helper.SpringBeanUtils;
import org.zoo.cat.core.service.CatTransactionAspectService;
import org.zoo.cat.core.service.CatTransactionFactoryService;
import org.zoo.cat.core.service.CatTransactionHandler;

/**
 * CatTransactionAspectServiceImpl.
 *
 * @author dzc
 */
@Service("catTransactionAspectService")
@SuppressWarnings("unchecked")
public class CatTransactionAspectServiceImpl implements CatTransactionAspectService {

    private final CatTransactionFactoryService catTransactionFactoryService;
    


    /**
     * Instantiates a new Cat transaction aspect service.
     *
     * @param catTransactionFactoryService the cat transaction factory service
     */
    @Autowired
    public CatTransactionAspectServiceImpl(final CatTransactionFactoryService catTransactionFactoryService) {
        this.catTransactionFactoryService = catTransactionFactoryService;
    }

    /**
     * cat transaction aspect.
     *
     * @param catTransactionContext {@linkplain  CatTransactionContext}
     * @param point                   {@linkplain ProceedingJoinPoint}
     * @return object  return value
     * @throws Throwable exception
     */
    @Override
    public Object invoke(final CatTransactionContext catTransactionContext, final ProceedingJoinPoint point) throws Throwable {
        final Class clazz = catTransactionFactoryService.factoryOf(point,catTransactionContext);
        final CatTransactionHandler txTransactionHandler =
                (CatTransactionHandler) SpringBeanUtils.getInstance().getBean(clazz);
        return txTransactionHandler.handler(point, catTransactionContext);
    }
}
