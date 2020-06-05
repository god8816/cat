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

package org.zoo.cat.core.coordinator;

import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.enums.CatActionEnum;

/**
 * this is save transaction log service.
 * @author dzc
 */
public interface CatCoordinatorService {

    /**
     * init cat config.
     *
     * @param catConfig {@linkplain CatConfig}
     * @throws Exception exception
     */
    void start(CatConfig catConfig) throws Exception;

    /**
     * save tccTransaction.
     *
     * @param catTransaction {@linkplain CatTransaction }
     * @return id
     */
    String save(CatTransaction catTransaction);

    /**
     * find by transId.
     *
     * @param transId  transId
     * @return {@linkplain CatTransaction }
     */
    CatTransaction findByTransId(String transId);

    /**
     * remove transaction.
     *
     * @param id  transaction pk.
     * @return true success
     */
    boolean remove(String id);

    /**
     * update.
     * @param catTransaction {@linkplain CatTransaction }
     */
    void update(CatTransaction catTransaction);

    /**
     * update TccTransaction .
     * this is only update Participant field.
     * @param catTransaction  {@linkplain CatTransaction }
     * @return rows
     */
    int updateParticipant(CatTransaction catTransaction);

    /**
     * update TccTransaction status.
     * @param id  pk.
     * @param status   {@linkplain CatActionEnum}
     * @return rows
     */
    int updateStatus(String id, Integer status);

}
