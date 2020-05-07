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
import org.aspectj.lang.reflect.MethodSignature;
import org.zoo.cat.annotation.Cat;
import org.zoo.cat.annotation.TransTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.bean.variate.CatDegradation;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.enums.CatRoleEnum;
import org.zoo.cat.core.service.CatTransactionFactoryService;
import org.zoo.cat.core.service.handler.ConsumeCatNoticeTransactionHandler;
import org.zoo.cat.core.service.handler.ConsumeCatTransactionHandler;
import org.zoo.cat.core.service.handler.LocalCatTransactionHandler;
import org.zoo.cat.core.service.handler.ParticipantCatTransactionHandler;
import org.zoo.cat.core.service.handler.StarterCatTransactionHandler;
import org.zoo.cat.core.service.handler.StarterNoticeTransactionHandler;
import org.zoo.cat.core.utils.JoinPointUtils;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * CatTransactionFactoryServiceImpl.
 *
 * @author dzc
 */
@SuppressWarnings("rawtypes")
@Service("catTransactionFactoryService")
public class CatTransactionFactoryServiceImpl implements CatTransactionFactoryService  {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CatTransactionFactoryServiceImpl.class);
    
    @Autowired
    private CatConfig catConfig;
    
    /**
     * acquired CatTransactionHandler.
     *
     * @param context {@linkplain CatTransactionContext}
     * @return Class
     */
    @Override
    public Class factoryOf(final ProceedingJoinPoint point,final CatTransactionContext context) {
        MethodSignature signature = (MethodSignature) point.getSignature();
        Method method = JoinPointUtils.getMethod(point);
        Class<?> declaringClass =  signature.getMethod().getDeclaringClass();
        
        final Cat cat = method.getAnnotation(Cat.class);
        final TransTypeEnum pattern = cat.pattern();
        if(Objects.isNull(pattern)) { 
         	LOGGER.error("事务补偿模式必须在TCC,SAGA,CC,NOTICE中选择"); 
         	return ConsumeCatTransactionHandler.class;
        }
        
        //判断正向消息补偿模式
        if(cat.pattern().getCode()==TransTypeEnum.NOTICE.getCode()) {
        	  if(CatDegradation.isStartDegradation(catConfig, declaringClass.getName(), method.getName())) {
               	if (Objects.isNull(context)) { 
                    return StarterNoticeTransactionHandler.class;
                } else {
                	    //1.0 spring cloud调用
                    if (context.getRole() == CatRoleEnum.SPRING_CLOUD.getCode()) {
                        context.setRole(CatRoleEnum.START.getCode());
                        return ConsumeCatNoticeTransactionHandler.class;
                    }
                    //2.0 dubbo调用
                    if (context.getRole() == CatRoleEnum.LOCAL.getCode()) {
                    	    return LocalCatTransactionHandler.class;
                    }else if (context.getRole() == CatRoleEnum.START.getCode()
                            || context.getRole() == CatRoleEnum.INLINE.getCode()) {
                     	return ParticipantCatTransactionHandler.class;
                    }
                    return ConsumeCatNoticeTransactionHandler.class;
                } 
        	  }else {
        		   return ConsumeCatTransactionHandler.class;
        	  }
        }else {
            if (Objects.isNull(context)) {
                return StarterCatTransactionHandler.class;
            } else {
                //why this code?  because spring cloud invoke has proxy.
                if (context.getRole() == CatRoleEnum.SPRING_CLOUD.getCode()) {
                    context.setRole(CatRoleEnum.START.getCode());
                    return ConsumeCatTransactionHandler.class;
                }
                // if context not null and role is inline  is ParticipantCatTransactionHandler.
                if (context.getRole() == CatRoleEnum.LOCAL.getCode()) {
                    return LocalCatTransactionHandler.class;
                } else if (context.getRole() == CatRoleEnum.START.getCode()
                        || context.getRole() == CatRoleEnum.INLINE.getCode()) {
                    return ParticipantCatTransactionHandler.class;
                }
                return ConsumeCatTransactionHandler.class;
            }
        }
    }
}
