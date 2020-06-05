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
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

/**
 * The type Cat feign bean post processor.
 *
 * @author dzc
 */
public class CatFeignBeanPostProcessor implements BeanPostProcessor {

    @Override
    public Object postProcessBeforeInitialization(final Object bean, final String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
        if (!Proxy.isProxyClass(bean.getClass())) {
            return bean;
        }
        InvocationHandler handler = Proxy.getInvocationHandler(bean);

        final Method[] methods = ReflectionUtils.getAllDeclaredMethods(bean.getClass());

        for (Method method : methods) {
            Cat cat = AnnotationUtils.findAnnotation(method, Cat.class);
            if (Objects.nonNull(cat)) {
                CatFeignHandler catFeignHandler = new CatFeignHandler();
                catFeignHandler.setDelegate(handler);
                Class<?> clazz = bean.getClass();
                Class<?>[] interfaces = clazz.getInterfaces();
                ClassLoader loader = clazz.getClassLoader();
                return Proxy.newProxyInstance(loader, interfaces, catFeignHandler);
            }
        }
        return bean;
    }

}


