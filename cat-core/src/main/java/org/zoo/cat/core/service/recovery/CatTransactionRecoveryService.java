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

package org.zoo.cat.core.service.recovery;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoo.cat.common.bean.entity.CatParticipant;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.enums.CatActionEnum;
import org.zoo.cat.common.utils.CollectionUtils;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.core.concurrent.threadlocal.CatTransactionContextLocal;
import org.zoo.cat.core.reflect.CatReflector;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

import java.util.List;

/**
 * The type Cat transaction recovery service.
 *
 * @author dzc
 */
public class CatTransactionRecoveryService {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CatTransactionRecoveryService.class);

    private CatCoordinatorRepository catCoordinatorRepository;

    /**
     * Instantiates a new Cat transaction recovery service.
     *
     * @param catCoordinatorRepository the cat coordinator repository
     */
    public CatTransactionRecoveryService(final CatCoordinatorRepository catCoordinatorRepository) {
        this.catCoordinatorRepository = catCoordinatorRepository;
    }

    /**
     * Cancel.
     *
     * @param catTransaction the cat transaction
     */
    public void cancel(final CatTransaction catTransaction) {
        final List<CatParticipant> catParticipants = catTransaction.getCatParticipants();
        List<CatParticipant> failList = Lists.newArrayListWithCapacity(catParticipants.size());
        boolean success = true;
        if (CollectionUtils.isNotEmpty(catParticipants)) {
            for (CatParticipant catParticipant : catParticipants) {
                try {
                    CatReflector.executor(catParticipant.getTransId(),
                            CatActionEnum.CANCELING,
                            catParticipant.getCancelCatInvocation());
                } catch (Exception e) {
                    LogUtil.error(LOGGER, "execute cancel exception:{}", () -> e);
                    success = false;
                    failList.add(catParticipant);
                } finally {
                    CatTransactionContextLocal.getInstance().remove();
                }
            }
            executeHandler(success, catTransaction, failList);
        }

    }

    /**
     * Confirm.
     *
     * @param catTransaction the cat transaction
     */
    public void confirm(final CatTransaction catTransaction) {
        final List<CatParticipant> catParticipants = catTransaction.getCatParticipants();
        List<CatParticipant> failList = Lists.newArrayListWithCapacity(catParticipants.size());
        boolean success = true;
        if (CollectionUtils.isNotEmpty(catParticipants)) {
            for (CatParticipant catParticipant : catParticipants) {
                try {
                    CatReflector.executor(catParticipant.getTransId(),
                            CatActionEnum.CONFIRMING,
                            catParticipant.getConfirmCatInvocation());
                } catch (Exception e) {
                    LogUtil.error(LOGGER, "execute confirm exception:{}", () -> e);
                    success = false;
                    failList.add(catParticipant);
                } finally {
                    CatTransactionContextLocal.getInstance().remove();
                }
            }
            executeHandler(success, catTransaction, failList);
        }
    }
    
    
    /**
     * notice.
     * @param catTransaction the cat transaction
     */
    public void notice(final CatTransaction catTransaction) {
        final List<CatParticipant> catParticipants = catTransaction.getCatParticipants();
        List<CatParticipant> failList = Lists.newArrayListWithCapacity(catParticipants.size());
        boolean success = true;
        if (CollectionUtils.isNotEmpty(catParticipants)) {
            for (CatParticipant catParticipant : catParticipants) {
                try {
                    CatReflector.executor(catParticipant.getTransId(),
                            CatActionEnum.NOTICEING,
                            catParticipant.getNoticeCatInvocation());
                } catch (Exception e) {
                    LogUtil.error(LOGGER, "execute notice exception:{}", () -> e);
                    success = false;
                    failList.add(catParticipant);
                } finally {
                    CatTransactionContextLocal.getInstance().remove();
                }
            }
            executeHandler(success, catTransaction, failList);
        }
    }

    private void executeHandler(final boolean success, final CatTransaction currentTransaction, final List<CatParticipant> failList) {
        if (success) {
            deleteTransaction(currentTransaction.getTransId());
        } else {
            currentTransaction.setCatParticipants(failList);
            catCoordinatorRepository.updateParticipant(currentTransaction);
        }
    }

    private void deleteTransaction(final String transId) {
        catCoordinatorRepository.remove(transId);
    }
}
