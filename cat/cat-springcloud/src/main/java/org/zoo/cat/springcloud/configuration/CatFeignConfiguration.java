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

package org.zoo.cat.springcloud.configuration;

import feign.RequestInterceptor;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zoo.cat.springcloud.feign.CatFeignBeanPostProcessor;
import org.zoo.cat.springcloud.feign.CatFeignInterceptor;
import org.zoo.cat.springcloud.hystrix.CatHystrixConcurrencyStrategyRegister;
import org.zoo.cat.springcloud.hystrix.CatHystrixConcurrencyStrategyRegisterService;

/**
 * The type Cat spring cloud configuration.
 *
 * @author dzc
 */
@Configuration
public class CatFeignConfiguration {

    /**
     * Cat rest template interceptor request interceptor.
     *
     * @return the request interceptor
     */
    @Bean
    @Qualifier("catFeignInterceptor")
    public RequestInterceptor catFeignInterceptor() {
        return new CatFeignInterceptor();
    }

    /**
     * Feign post processor cat feign bean post processor.
     *
     * @return the cat feign bean post processor
     */
    @Bean
    public CatFeignBeanPostProcessor feignPostProcessor() {
        return new CatFeignBeanPostProcessor();
    }

    /**
     * Hystrix concurrency strategy hystrix concurrency strategy.
     *
     * @return the hystrix concurrency strategy
     */
    @Bean
    @ConditionalOnProperty(name = "feign.hystrix.enabled")
    @ConditionalOnMissingBean(value = CatHystrixConcurrencyStrategyRegisterService.class)
    public CatHystrixConcurrencyStrategyRegisterService hystrixConcurrencyStrategy() {
        return new CatHystrixConcurrencyStrategyRegister();
    }
}
