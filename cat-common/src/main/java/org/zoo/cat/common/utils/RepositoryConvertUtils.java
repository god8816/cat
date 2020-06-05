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

package org.zoo.cat.common.utils;

import org.zoo.cat.common.bean.adapter.CoordinatorRepositoryAdapter;
import org.zoo.cat.common.bean.entity.CatParticipant;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.common.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * RepositoryConvertUtils.
 *
 * @author dzc
 */
public class RepositoryConvertUtils {

    /**
     * Convert byte [ ].
     *
     * @param catTransaction   the tcc transaction
     * @param objectSerializer the object serializer
     * @return the byte [ ]
     * @throws CatException the tcc exception
     */
    public static byte[] convert(final CatTransaction catTransaction, final ObjectSerializer objectSerializer) throws CatException {
        CoordinatorRepositoryAdapter adapter = new CoordinatorRepositoryAdapter();
        adapter.setTransId(catTransaction.getTransId());
        adapter.setLastTime(catTransaction.getLastTime());
        adapter.setCreateTime(catTransaction.getCreateTime());
        adapter.setRetriedCount(catTransaction.getRetriedCount());
        adapter.setStatus(catTransaction.getStatus());
        adapter.setTargetClass(catTransaction.getTargetClass());
        adapter.setTargetMethod(catTransaction.getTargetMethod());
        adapter.setPattern(catTransaction.getPattern());
        adapter.setRole(catTransaction.getRole());
        adapter.setVersion(catTransaction.getVersion());
        if (CollectionUtils.isNotEmpty(catTransaction.getCatParticipants())) {
            final CatParticipant catParticipant = catTransaction.getCatParticipants().get(0);
            adapter.setConfirmMethod(catParticipant.getConfirmCatInvocation().getMethodName());
            adapter.setCancelMethod(catParticipant.getCancelCatInvocation().getMethodName());
        }
        adapter.setContents(objectSerializer.serialize(catTransaction.getCatParticipants()));
        return objectSerializer.serialize(adapter);
    }

    /**
     * Transform bean tcc transaction.
     *
     * @param contents         the contents
     * @param objectSerializer the object serializer
     * @return the tcc transaction
     * @throws CatException the tcc exception
     */
    @SuppressWarnings("unchecked")
    public static CatTransaction transformBean(final byte[] contents, final ObjectSerializer objectSerializer) throws CatException {
        CatTransaction catTransaction = new CatTransaction();
        final CoordinatorRepositoryAdapter adapter = objectSerializer.deSerialize(contents, CoordinatorRepositoryAdapter.class);
        List<CatParticipant> catParticipants = objectSerializer.deSerialize(adapter.getContents(), ArrayList.class);
        catTransaction.setLastTime(adapter.getLastTime());
        catTransaction.setRetriedCount(adapter.getRetriedCount());
        catTransaction.setCreateTime(adapter.getCreateTime());
        catTransaction.setTransId(adapter.getTransId());
        catTransaction.setStatus(adapter.getStatus());
        catTransaction.setCatParticipants(catParticipants);
        catTransaction.setRole(adapter.getRole());
        catTransaction.setPattern(adapter.getPattern());
        catTransaction.setTargetClass(adapter.getTargetClass());
        catTransaction.setTargetMethod(adapter.getTargetMethod());
        catTransaction.setVersion(adapter.getVersion());
        return catTransaction;
    }

}
