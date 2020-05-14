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
import org.zoo.cat.annotation.CatSPI;
import org.zoo.cat.common.bean.adapter.CoordinatorRepositoryAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoo.cat.common.bean.entity.CatNoticeSafe;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.config.CatRedisConfig;
import org.zoo.cat.common.enums.RepositorySupportEnum;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.exception.CatRuntimeException;
import org.zoo.cat.common.jedis.JedisClient;
import org.zoo.cat.common.jedis.JedisClientCluster;
import org.zoo.cat.common.jedis.JedisClientSentinel;
import org.zoo.cat.common.jedis.JedisClientSingle;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.common.utils.RepositoryConvertUtils;
import org.zoo.cat.common.utils.RepositoryPathUtils;
import org.zoo.cat.common.utils.StringUtils;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * redis impl.
 *
 * @author dzc
 */
@CatSPI("redis")
public class RedisCoordinatorRepository implements CatCoordinatorRepository {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCoordinatorRepository.class);

    private ObjectSerializer objectSerializer;

    private JedisClient jedisClient;

    private String keyPrefix;

    @Override
    public int create(final CatTransaction catTransaction) {
        try {
            final String redisKey = RepositoryPathUtils.buildRedisKey(keyPrefix, catTransaction.getTransId());
            jedisClient.set(redisKey, RepositoryConvertUtils.convert(catTransaction, objectSerializer));
            return ROWS;
        } catch (Exception e) {
            throw new CatRuntimeException(e);
        }
    }

    @Override
    public int remove(final String id) {
        try {
            final String redisKey = RepositoryPathUtils.buildRedisKey(keyPrefix, id);
            return jedisClient.del(redisKey).intValue();
        } catch (Exception e) {
            throw new CatRuntimeException(e);
        }
    }

    @Override
    public int update(final CatTransaction catTransaction) throws CatRuntimeException {
        try {
            final String redisKey = RepositoryPathUtils.buildRedisKey(keyPrefix, catTransaction.getTransId());
            catTransaction.setVersion(catTransaction.getVersion() + 1);
            catTransaction.setLastTime(new Date());
            catTransaction.setRetriedCount(catTransaction.getRetriedCount());
            jedisClient.set(redisKey, RepositoryConvertUtils.convert(catTransaction, objectSerializer));
            return ROWS;
        } catch (Exception e) {
            throw new CatRuntimeException(e);
        }
    }

    @Override
    public int updateParticipant(final CatTransaction catTransaction) {
        try {
            final String redisKey = RepositoryPathUtils.buildRedisKey(keyPrefix, catTransaction.getTransId());
            byte[] contents = jedisClient.get(redisKey.getBytes());
            CoordinatorRepositoryAdapter adapter = objectSerializer.deSerialize(contents, CoordinatorRepositoryAdapter.class);
            adapter.setContents(objectSerializer.serialize(catTransaction.getCatParticipants()));
            jedisClient.set(redisKey, objectSerializer.serialize(adapter));
        } catch (CatException e) {
            e.printStackTrace();
            return FAIL_ROWS;
        }
        return ROWS;
    }

    @Override
    public int updateStatus(final String id, final Integer status) {
        try {
            final String redisKey = RepositoryPathUtils.buildRedisKey(keyPrefix, id);
            byte[] contents = jedisClient.get(redisKey.getBytes());
            if (contents != null) {
                CoordinatorRepositoryAdapter adapter = objectSerializer.deSerialize(contents, CoordinatorRepositoryAdapter.class);
                adapter.setStatus(status);
                jedisClient.set(redisKey, objectSerializer.serialize(adapter));
            }
        } catch (CatException e) {
            e.printStackTrace();
            return FAIL_ROWS;
        }
        return ROWS;
    }

    @Override
    public CatTransaction findById(final String id) {
        try {
            final String redisKey = RepositoryPathUtils.buildRedisKey(keyPrefix, id);
            byte[] contents = jedisClient.get(redisKey.getBytes());
            return RepositoryConvertUtils.transformBean(contents, objectSerializer);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public List<CatTransaction> listAll() {
        try {
            List<CatTransaction> transactions = Lists.newArrayList();
            Set<byte[]> keys = jedisClient.keys((keyPrefix + "*").getBytes());
            for (final byte[] key : keys) {
                byte[] contents = jedisClient.get(key);
                if (contents != null) {
                    transactions.add(RepositoryConvertUtils.transformBean(contents, objectSerializer));
                }
            }
            return transactions;
        } catch (Exception e) {
            throw new CatRuntimeException(e);
        }
    }

    @Override
    public List<CatTransaction> listAllByDelay(final Date date) {
        final List<CatTransaction> catTransactions = listAll();
        return catTransactions.stream()
                .filter(tccTransaction -> tccTransaction.getLastTime().compareTo(date) < 0)
                .collect(Collectors.toList());
    }

    @Override
    public void init(final String modelName, final String appName,final CatConfig catConfig) {
        keyPrefix = RepositoryPathUtils.buildRedisKeyPrefix(modelName);
        final CatRedisConfig catRedisConfig = catConfig.getCatRedisConfig();
        try {
            buildJedisPool(catRedisConfig);
        } catch (Exception e) {
            LogUtil.error(LOGGER, "redis init error please check you config:{}", e::getMessage);
            throw new CatRuntimeException(e);
        }
    }

    @Override
    public String getScheme() {
        return RepositorySupportEnum.REDIS.getSupport();
    }

    @Override
    public void setSerializer(final ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
    }

    private void buildJedisPool(final CatRedisConfig catRedisConfig) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(catRedisConfig.getMaxIdle());
        config.setMinIdle(catRedisConfig.getMinIdle());
        config.setMaxTotal(catRedisConfig.getMaxTotal());
        config.setMaxWaitMillis(catRedisConfig.getMaxWaitMillis());
        config.setTestOnBorrow(catRedisConfig.getTestOnBorrow());
        config.setTestOnReturn(catRedisConfig.getTestOnReturn());
        config.setTestWhileIdle(catRedisConfig.getTestWhileIdle());
        config.setMinEvictableIdleTimeMillis(catRedisConfig.getMinEvictableIdleTimeMillis());
        config.setSoftMinEvictableIdleTimeMillis(catRedisConfig.getSoftMinEvictableIdleTimeMillis());
        config.setTimeBetweenEvictionRunsMillis(catRedisConfig.getTimeBetweenEvictionRunsMillis());
        config.setNumTestsPerEvictionRun(catRedisConfig.getNumTestsPerEvictionRun());
        JedisPool jedisPool;
        if (catRedisConfig.getCluster()) {
            LogUtil.info(LOGGER, () -> "build redis cluster ............");
            final String clusterUrl = catRedisConfig.getClusterUrl();
            final Set<HostAndPort> hostAndPorts =
                    Lists.newArrayList(Splitter.on(";")
                            .split(clusterUrl))
                            .stream()
                            .map(HostAndPort::parseString).collect(Collectors.toSet());
            JedisCluster jedisCluster = new JedisCluster(hostAndPorts, config);
            jedisClient = new JedisClientCluster(jedisCluster);
        } else if (catRedisConfig.getSentinel()) {
            LogUtil.info(LOGGER, () -> "build redis sentinel ............");
            final String sentinelUrl = catRedisConfig.getSentinelUrl();
            final Set<String> hostAndPorts =
                    new HashSet<>(Lists.newArrayList(Splitter.on(";").split(sentinelUrl)));
            JedisSentinelPool pool =
                    new JedisSentinelPool(catRedisConfig.getMasterName(), hostAndPorts,
                            config, catRedisConfig.getTimeOut(), catRedisConfig.getPassword());
            jedisClient = new JedisClientSentinel(pool);
        } else {
            if (StringUtils.isNoneBlank(catRedisConfig.getPassword())) {
                jedisPool = new JedisPool(config, catRedisConfig.getHostName(), catRedisConfig.getPort(), catRedisConfig.getTimeOut(), catRedisConfig.getPassword());
            } else {
                jedisPool = new JedisPool(config, catRedisConfig.getHostName(), catRedisConfig.getPort(), catRedisConfig.getTimeOut());
            }
            jedisClient = new JedisClientSingle(jedisPool);
        }
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
