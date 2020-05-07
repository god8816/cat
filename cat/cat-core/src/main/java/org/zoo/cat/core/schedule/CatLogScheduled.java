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

package org.zoo.cat.core.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.SmartApplicationListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.core.concurrent.threadpool.CatThreadFactory;
import org.zoo.cat.core.helper.SpringBeanUtils;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * EN: the type cat log delete scheduled.
 * CN: 补偿日志自动删除
 * @author dzc
 */
@Component
public class CatLogScheduled implements SmartApplicationListener {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CatLogScheduled.class);

    private final CatConfig catConfig;

    private volatile AtomicBoolean isInit = new AtomicBoolean(false);

    private ScheduledExecutorService scheduledExecutorService;

    private CatCoordinatorRepository catCoordinatorRepository;


    @Autowired(required = false)
    public CatLogScheduled(final CatConfig catConfig) {
        this.catConfig = catConfig;
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
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
    public void onApplicationEvent(@NonNull final ApplicationEvent event) {
        if (!isInit.compareAndSet(false, true)) {
            return;
        }
        catCoordinatorRepository = SpringBeanUtils.getInstance().getBean(CatCoordinatorRepository.class);
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(1,
                        CatThreadFactory.create("cat-notice-safe-second-scheduled", true));
        selfRecovery();
    }

    /**
     * if have some exception by schedule execute cat transaction log.
     */
    private void selfRecovery() {
        scheduledExecutorService
                .scheduleWithFixedDelay(() -> {
                        try {
		                    	 if(Objects.nonNull(catConfig.getCatLogConfig().getScheduledLogDelay())) {
		                           catCoordinatorRepository.removeLogsByDelay(acquireSecondsData());
		                           LOGGER.info("cat remove logs end:", acquireSecondsData());
		                     }
                        
                        } catch (Exception e) {
                            LOGGER.error("cat remove logs scheduled  is error:", e);
                        } 
                }, catConfig.getScheduledInitDelay(), catConfig.getCatLogConfig().getScheduledLogDelay(), TimeUnit.SECONDS);

    }

    /**
     * seconds acquire time
     * */
    private Date acquireSecondsData() {
        return new Date(LocalDateTime.now().atZone(ZoneId.systemDefault())
                .toInstant().toEpochMilli() - (catConfig.getCatLogConfig().getScheduledLogDelay() * 1000));
    }
}
