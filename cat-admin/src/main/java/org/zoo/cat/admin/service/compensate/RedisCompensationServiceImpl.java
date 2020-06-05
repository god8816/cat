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

package org.zoo.cat.admin.service.compensate;

import com.google.common.collect.Sets;

import org.zoo.cat.common.bean.adapter.CoordinatorRepositoryAdapter;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.jedis.JedisClient;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.common.utils.DateUtils;
import org.zoo.cat.common.utils.RepositoryPathUtils;
import lombok.RequiredArgsConstructor;
import org.zoo.cat.common.utils.CollectionUtils;
import org.zoo.cat.common.utils.StringUtils;
import org.zoo.cat.admin.helper.ConvertHelper;
import org.zoo.cat.admin.helper.PageHelper;
import org.zoo.cat.admin.page.CommonPager;
import org.zoo.cat.admin.query.CompensationQuery;
import org.zoo.cat.admin.service.CompensationService;
import org.zoo.cat.admin.vo.CatCompensationVO;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * redis impl.
 * @author dzc
 */
@SuppressWarnings("unchecked")
@RequiredArgsConstructor
public class RedisCompensationServiceImpl implements CompensationService {

    private final JedisClient jedisClient;

    private final ObjectSerializer objectSerializer;

    @Override
    public CommonPager<CatCompensationVO> listByPage(final CompensationQuery query) {
        CommonPager<CatCompensationVO> commonPager = new CommonPager<>();
        final String redisKeyPrefix = RepositoryPathUtils.buildRedisKeyPrefix(query.getApplicationName());
        final int currentPage = query.getPageParameter().getCurrentPage();
        final int pageSize = query.getPageParameter().getPageSize();
        int start = (currentPage - 1) * pageSize;
        Set<byte[]> keys;
        List<CatCompensationVO> voList;
        int totalCount;
        //如果只查 重试条件的
        if (StringUtils.isBlank(query.getTransId()) && Objects.nonNull(query.getRetry())) {
            keys = jedisClient.keys((redisKeyPrefix + "*").getBytes());
            final List<CatCompensationVO> all = findAll(keys);
            final List<CatCompensationVO> collect =
                    all.stream()
                            .filter(vo -> vo.getRetriedCount() < query.getRetry())
                            .collect(Collectors.toList());
            totalCount = collect.size();
            voList = collect.stream().skip(start).limit(pageSize).collect(Collectors.toList());
        } else if (StringUtils.isNoneBlank(query.getTransId()) && Objects.isNull(query.getRetry())) {
            keys = Sets.newHashSet(String.join(":", redisKeyPrefix, query.getTransId()).getBytes());
            totalCount = keys.size();
            voList = findAll(keys);
        } else if (StringUtils.isNoneBlank(query.getTransId()) && Objects.nonNull(query.getRetry())) {
            keys = Sets.newHashSet(String.join(":", redisKeyPrefix, query.getTransId()).getBytes());
            totalCount = keys.size();
            voList = findAll(keys)
                    .stream()
                    .filter(vo -> vo.getRetriedCount() < query.getRetry())
                    .collect(Collectors.toList());
        } else {
            keys = jedisClient.keys((redisKeyPrefix + "*").getBytes());
            if (keys.size() <= 0 || keys.size() < start) {
                return commonPager;
            }
            totalCount = keys.size();
            voList = findByPage(keys, start, pageSize);
        }
        if (keys.size() <= 0 || keys.size() < start) {
            return commonPager;
        }
        commonPager.setPage(PageHelper.buildPage(query.getPageParameter(), totalCount));
        commonPager.setDataList(voList);
        return commonPager;
    }

    private CatCompensationVO buildVOByKey(final byte[] key) {
        final byte[] bytes = jedisClient.get(key);
        try {
            final CoordinatorRepositoryAdapter adapter =
                    objectSerializer.deSerialize(bytes, CoordinatorRepositoryAdapter.class);
            return ConvertHelper.buildVO(adapter);
        } catch (CatException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Boolean batchRemove(final List<String> ids, final String applicationName) {
        if (CollectionUtils.isEmpty(ids) || StringUtils.isBlank(applicationName)) {
            return Boolean.FALSE;
        }
        String keyPrefix = RepositoryPathUtils.buildRedisKeyPrefix(applicationName);
        final String[] keys = ids.stream()
                .map(id -> RepositoryPathUtils.buildRedisKey(keyPrefix, id)).toArray(String[]::new);
        jedisClient.del(keys);
        return Boolean.TRUE;
    }

    @Override
    public Boolean updateRetry(final String id, final Integer retry, final String appName) {
        if (StringUtils.isBlank(id) || StringUtils.isBlank(appName) || Objects.isNull(retry)) {
            return Boolean.FALSE;
        }
        String keyPrefix = RepositoryPathUtils.buildRedisKeyPrefix(appName);
        final String key = RepositoryPathUtils.buildRedisKey(keyPrefix, id);
        final byte[] bytes = jedisClient.get(key.getBytes());
        try {
            final CoordinatorRepositoryAdapter adapter =
                    objectSerializer.deSerialize(bytes, CoordinatorRepositoryAdapter.class);
            adapter.setRetriedCount(retry);
            adapter.setLastTime(DateUtils.getDateYYYY());
            jedisClient.set(key, objectSerializer.serialize(adapter));
            return Boolean.TRUE;
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    private List<CatCompensationVO> findAll(final Set<byte[]> keys) {
        return keys.parallelStream()
                .map(this::buildVOByKey)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<CatCompensationVO> findByPage(final Set<byte[]> keys, final int start, final int pageSize) {
        return keys.parallelStream()
                .skip(start)
                .limit(pageSize)
                .map(this::buildVOByKey)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

}
