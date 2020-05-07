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

import org.zoo.cat.annotation.TransTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.SmartApplicationListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.enums.CatActionEnum;
import org.zoo.cat.common.enums.CatRoleEnum;
import org.zoo.cat.common.utils.CollectionUtils;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.core.concurrent.threadpool.CatThreadFactory;
import org.zoo.cat.core.helper.SpringBeanUtils;
import org.zoo.cat.core.service.recovery.CatTransactionRecoveryService;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * EN:The type Cat transaction self recovery scheduled.
 * CN:定时补偿失败的请求
 * @author dzc
 */
@Component
public class CatTransactionSelfRecoveryScheduled implements SmartApplicationListener {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CatTransactionSelfRecoveryScheduled.class);

    private final CatConfig catConfig;

    private volatile AtomicBoolean isInit = new AtomicBoolean(false);

    private ScheduledExecutorService scheduledExecutorService;

    private CatCoordinatorRepository catCoordinatorRepository;

    private CatTransactionRecoveryService catTransactionRecoveryService;

    @Autowired(required = false)
    public CatTransactionSelfRecoveryScheduled(final CatConfig catConfig) {
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
                        CatThreadFactory.create("cat-transaction-self-recovery", true));
        catTransactionRecoveryService = new CatTransactionRecoveryService(catCoordinatorRepository);
        selfRecovery();
    }

    /**
     * if have some exception by schedule execute cat transaction log.
     */
    private void selfRecovery() {
        scheduledExecutorService
                .scheduleWithFixedDelay(() -> {
                    LogUtil.debug(LOGGER, "self recovery execute delayTime:{}", catConfig::getScheduledDelay);
                    try {
                        final List<CatTransaction> catTransactions = catCoordinatorRepository.listAllByDelay(acquireData());
                        if (CollectionUtils.isEmpty(catTransactions)) {
                            return;
                        }
                        for (CatTransaction catTransaction : catTransactions) {
                            // if the try is not completed, no compensation will be provided (to prevent various exceptions in the try phase)
                            if (catTransaction.getRole() == CatRoleEnum.PROVIDER.getCode()
                                    && catTransaction.getStatus() == CatActionEnum.PRE_TRY.getCode()) {
                                catCoordinatorRepository.remove(catTransaction.getTransId());
                                continue;
                            }
                            
                            if (catTransaction.getRetriedCount() >= catTransaction.getRetryMax()) {
                                LogUtil.debug(LOGGER, "This transaction exceeds the maximum number of retries and no retries will occur：{}", () -> catTransaction);
                                continue;
                            }
                            if (Objects.equals(catTransaction.getPattern(), TransTypeEnum.CC.getCode())
                                    && catTransaction.getStatus() == CatActionEnum.TRYING.getCode()) {
                                continue;
                            }
                            // if the transaction role is the provider, and the number of retries in the scope class cannot be executed, only by the initiator
                            if (catTransaction.getRole() == CatRoleEnum.PROVIDER.getCode()
                                    && (catTransaction.getCreateTime().getTime()
                                    + catConfig.getRecoverDelayTime() * catConfig.getLoadFactor() * 1000
                                    > System.currentTimeMillis())) {
                                continue;
                            }
                            catTransaction.setRetriedCount(catTransaction.getRetriedCount() + 1);
                            final int rows = catCoordinatorRepository.update(catTransaction);
                            // determine that rows>0 is executed to prevent concurrency when the business side is in cluster mode
                            if (rows > 0) {
                                if (catTransaction.getStatus() == CatActionEnum.TRYING.getCode()
                                        || catTransaction.getStatus() == CatActionEnum.PRE_TRY.getCode()
                                        || catTransaction.getStatus() == CatActionEnum.CANCELING.getCode()) {
                                    catTransactionRecoveryService.cancel(catTransaction);
                                } else if (catTransaction.getStatus() == CatActionEnum.CONFIRMING.getCode()) {
                                    catTransactionRecoveryService.confirm(catTransaction);
                                } else if (catTransaction.getStatus() == CatActionEnum.NOTICEING.getCode()) {
                                    catTransactionRecoveryService.notice(catTransaction);
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("cat scheduled transaction log is error:", e);
                    } 
                }, catConfig.getScheduledInitDelay(), catConfig.getScheduledDelay(), TimeUnit.SECONDS);

    }

    private Date acquireData() {
        return new Date(LocalDateTime.now().atZone(ZoneId.systemDefault())
                .toInstant().toEpochMilli() - (catConfig.getRecoverDelayTime() * 1000));
    }


}
