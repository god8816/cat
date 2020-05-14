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

package org.zoo.cat.core.spi.repository;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.result.UpdateResult;
import org.zoo.cat.annotation.CatSPI;
import org.zoo.cat.common.bean.adapter.MongoAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoClientFactoryBean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.zoo.cat.common.bean.entity.CatNoticeSafe;
import org.zoo.cat.common.bean.entity.CatParticipant;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.config.CatMongoConfig;
import org.zoo.cat.common.enums.RepositorySupportEnum;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.exception.CatRuntimeException;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.common.utils.AssertUtils;
import org.zoo.cat.common.utils.CollectionUtils;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.common.utils.RepositoryPathUtils;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * mongo impl.
 *
 * @author dzc
 */
@CatSPI("mongodb")
public class MongoCoordinatorRepository implements CatCoordinatorRepository {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoCoordinatorRepository.class);

    private ObjectSerializer objectSerializer;

    private MongoTemplate template;

    private String collectionName;

    @Override
    public int create(final CatTransaction catTransaction) {
        try {
            MongoAdapter mongoBean = new MongoAdapter();
            mongoBean.setTransId(catTransaction.getTransId());
            mongoBean.setCreateTime(catTransaction.getCreateTime());
            mongoBean.setLastTime(catTransaction.getLastTime());
            mongoBean.setRetriedCount(catTransaction.getRetriedCount());
            mongoBean.setStatus(catTransaction.getStatus());
            mongoBean.setRole(catTransaction.getRole());
            mongoBean.setPattern(catTransaction.getPattern());
            mongoBean.setTargetClass(catTransaction.getTargetClass());
            mongoBean.setTargetMethod(catTransaction.getTargetMethod());
            mongoBean.setConfirmMethod(catTransaction.getConfirmMethod());
            mongoBean.setCancelMethod(catTransaction.getCancelMethod());
            final byte[] cache = objectSerializer.serialize(catTransaction.getCatParticipants());
            mongoBean.setContents(cache);
            template.save(mongoBean, collectionName);
        } catch (CatException e) {
            e.printStackTrace();
        }
        return ROWS;
    }

    @Override
    public int remove(final String id) {
        AssertUtils.notNull(id);
        Query query = new Query();
        query.addCriteria(new Criteria("transId").is(id));
        template.remove(query, collectionName);
        return ROWS;
    }

    @Override
    public int update(final CatTransaction catTransaction) throws CatRuntimeException {
        Query query = new Query();
        query.addCriteria(new Criteria("transId").is(catTransaction.getTransId()));
        Update update = new Update();
        update.set("lastTime", new Date());
        update.set("retriedCount", catTransaction.getRetriedCount());
        update.set("version", catTransaction.getVersion() + 1);
        try {
            if (CollectionUtils.isNotEmpty(catTransaction.getCatParticipants())) {
                update.set("contents", objectSerializer.serialize(catTransaction.getCatParticipants()));
            }
        } catch (CatException e) {
            e.printStackTrace();
        }
        final UpdateResult updateResult = template.updateFirst(query, update, MongoAdapter.class, collectionName);

        if (updateResult.getModifiedCount() <= 0) {
            throw new CatRuntimeException("update data exception!");
        }
        return ROWS;
    }

    @Override
    public int updateParticipant(final CatTransaction catTransaction) {
        Query query = new Query();
        query.addCriteria(new Criteria("transId").is(catTransaction.getTransId()));
        Update update = new Update();
        try {
            update.set("contents", objectSerializer.serialize(catTransaction.getCatParticipants()));
        } catch (CatException e) {
            e.printStackTrace();
        }
        final UpdateResult updateResult = template.updateFirst(query, update, MongoAdapter.class, collectionName);
        if (updateResult.getModifiedCount() <= 0) {
            throw new CatRuntimeException("update data exception!");
        }
        return ROWS;
    }

    @Override
    public int updateStatus(final String id, final Integer status) {
        Query query = new Query();
        query.addCriteria(new Criteria("transId").is(id));
        Update update = new Update();
        update.set("status", status);
        final UpdateResult updateResult = template.updateFirst(query, update, MongoAdapter.class, collectionName);
        if (updateResult.getModifiedCount() <= 0) {
            throw new CatRuntimeException("update data exception!");
        }
        return ROWS;
    }

    @Override
    public CatTransaction findById(final String id) {
        Query query = new Query();
        query.addCriteria(new Criteria("transId").is(id));
        MongoAdapter cache = template.findOne(query, MongoAdapter.class, collectionName);
        return buildByCache(Objects.requireNonNull(cache));
    }

    @SuppressWarnings("unchecked")
    private CatTransaction buildByCache(final MongoAdapter cache) {
        try {
            CatTransaction catTransaction = new CatTransaction();
            catTransaction.setTransId(cache.getTransId());
            catTransaction.setCreateTime(cache.getCreateTime());
            catTransaction.setLastTime(cache.getLastTime());
            catTransaction.setRetriedCount(cache.getRetriedCount());
            catTransaction.setVersion(cache.getVersion());
            catTransaction.setStatus(cache.getStatus());
            catTransaction.setRole(cache.getRole());
            catTransaction.setPattern(cache.getPattern());
            catTransaction.setTargetClass(cache.getTargetClass());
            catTransaction.setTargetMethod(cache.getTargetMethod());
            List<CatParticipant> catParticipants = (List<CatParticipant>) objectSerializer.deSerialize(cache.getContents(), CopyOnWriteArrayList.class);
            catTransaction.setCatParticipants(catParticipants);
            return catTransaction;
        } catch (CatException e) {
            LogUtil.error(LOGGER, "mongodb deSerialize exception:{}", e::getLocalizedMessage);
            return null;
        }

    }

    @Override
    public List<CatTransaction> listAll() {
        final List<MongoAdapter> resultList = template.findAll(MongoAdapter.class, collectionName);
        if (CollectionUtils.isNotEmpty(resultList)) {
            return resultList.stream().map(this::buildByCache).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public List<CatTransaction> listAllByDelay(final Date date) {
        Query query = new Query();
        query.addCriteria(Criteria.where("lastTime").lt(date));
        final List<MongoAdapter> mongoBeans =
                template.find(query, MongoAdapter.class, collectionName);
        if (CollectionUtils.isNotEmpty(mongoBeans)) {
            return mongoBeans.stream().map(this::buildByCache).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public void init(final String modelName,final String appName, final CatConfig catConfig) {
        collectionName = RepositoryPathUtils.buildMongoTableName(modelName);
        final CatMongoConfig catMongoConfig = catConfig.getCatMongoConfig();
        MongoClientFactoryBean clientFactoryBean = buildMongoClientFactoryBean(catMongoConfig);
        try {
            clientFactoryBean.afterPropertiesSet();
            template = new MongoTemplate(Objects.requireNonNull(clientFactoryBean.getObject()), catMongoConfig.getMongoDbName());
        } catch (Exception e) {
            LogUtil.error(LOGGER, "mongo init error please check you config:{}", e::getMessage);
            throw new CatRuntimeException(e);
        }
    }

    private MongoClientFactoryBean buildMongoClientFactoryBean(final CatMongoConfig catMongoConfig) {
        MongoClientFactoryBean clientFactoryBean = new MongoClientFactoryBean();
        MongoCredential credential = MongoCredential.createScramSha1Credential(catMongoConfig.getMongoUserName(),
                catMongoConfig.getMongoDbName(),
                catMongoConfig.getMongoUserPwd().toCharArray());
        clientFactoryBean.setCredentials(new MongoCredential[]{credential});

        List<String> urls = Lists.newArrayList(Splitter.on(",").trimResults().split(catMongoConfig.getMongoDbUrl()));
        ServerAddress[] sds = new ServerAddress[urls.size()];
        for (int i = 0; i < sds.length; i++) {
            List<String> adds = Lists.newArrayList(Splitter.on(":").trimResults().split(urls.get(i)));
            InetSocketAddress address = new InetSocketAddress(adds.get(0), Integer.parseInt(adds.get(1)));
            sds[i] = new ServerAddress(address);
        }
        clientFactoryBean.setReplicaSetSeeds(sds);
        return clientFactoryBean;
    }

    @Override
    public String getScheme() {
        return RepositorySupportEnum.MONGODB.getSupport();
    }

    @Override
    public void setSerializer(final ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
    }

	@Override
	public List<CatNoticeSafe> countLogsByDelay(Date acquireData,String timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int removeLogsByDelay(Date acquireSecondsData) {
		// TODO Auto-generated method stub
		return 0;
	}
}
