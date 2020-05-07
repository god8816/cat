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

import org.zoo.cat.common.utils.CollectionUtils;
import org.zoo.cat.common.utils.StringUtils;
import org.zoo.cat.admin.helper.ConvertHelper;
import org.zoo.cat.admin.helper.PageHelper;
import org.zoo.cat.admin.page.CommonPager;
import org.zoo.cat.admin.page.PageParameter;
import org.zoo.cat.admin.query.CompensationQuery;
import org.zoo.cat.admin.service.CompensationService;
import org.zoo.cat.admin.vo.CatCompensationVO;
import org.zoo.cat.common.bean.adapter.CoordinatorRepositoryAdapter;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.common.utils.DateUtils;
import org.zoo.cat.common.utils.FileUtils;
import org.zoo.cat.common.utils.RepositoryPathUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * file impl.
 *
 * @author dzc
 */
@SuppressWarnings("all")
public class FileCompensationServiceImpl implements CompensationService {

    private final ObjectSerializer objectSerializer;

    public FileCompensationServiceImpl(final ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
    }

    @Override
    public CommonPager<CatCompensationVO> listByPage(final CompensationQuery query) {
        final String filePath = RepositoryPathUtils.buildFilePath(query.getApplicationName());
        final PageParameter pageParameter = query.getPageParameter();
        final int currentPage = pageParameter.getCurrentPage();
        final int pageSize = pageParameter.getPageSize();
        int start = (currentPage - 1) * pageSize;
        CommonPager<CatCompensationVO> voCommonPager = new CommonPager<>();
        File path;
        File[] files;
        int totalCount;
        List<CatCompensationVO> voList;
        //如果只查 重试条件的
        if (StringUtils.isBlank(query.getTransId()) && Objects.nonNull(query.getRetry())) {
            path = new File(filePath);
            files = path.listFiles();
            final List<CatCompensationVO> all = findAll(files);
            if (CollectionUtils.isNotEmpty(all)) {
                final List<CatCompensationVO> collect =
                        all.stream().filter(Objects::nonNull)
                                .filter(vo -> vo.getRetriedCount() < query.getRetry())
                                .collect(Collectors.toList());
                totalCount = collect.size();
                voList = collect.stream().skip(start).limit(pageSize).collect(Collectors.toList());
            } else {
                totalCount = 0;
                voList = null;
            }
        } else if (StringUtils.isNoneBlank(query.getTransId()) && Objects.isNull(query.getRetry())) {
            final String fullFileName = RepositoryPathUtils.getFullFileName(filePath, query.getTransId());
            final File file = new File(fullFileName);
            files = new File[]{file};
            totalCount = files.length;
            voList = findAll(files);
        } else if (StringUtils.isNoneBlank(query.getTransId()) && Objects.nonNull(query.getRetry())) {
            final String fullFileName = RepositoryPathUtils.getFullFileName(filePath, query.getTransId());
            final File file = new File(fullFileName);
            files = new File[]{file};
            totalCount = files.length;
            voList = findAll(files)
                    .stream().filter(Objects::nonNull)
                    .filter(vo -> vo.getRetriedCount() < query.getRetry())
                    .collect(Collectors.toList());
        } else {
            path = new File(filePath);
            files = path.listFiles();
            totalCount = Objects.requireNonNull(files).length;
            voList = findByPage(files, start, pageSize);
        }
        voCommonPager.setPage(PageHelper.buildPage(query.getPageParameter(), totalCount));
        voCommonPager.setDataList(voList);
        return voCommonPager;
    }

    @Override
    public Boolean batchRemove(final List<String> ids, final String applicationName) {
        if (CollectionUtils.isEmpty(ids) || StringUtils.isBlank(applicationName)) {
            return Boolean.FALSE;
        }
        final String filePath = RepositoryPathUtils.buildFilePath(applicationName);
        ids.stream().map(id ->
                new File(RepositoryPathUtils.getFullFileName(filePath, id)))
                .forEach(File::delete);
        return Boolean.TRUE;
    }

    @Override
    public Boolean updateRetry(final String id, final Integer retry, final String applicationName) {
        if (StringUtils.isBlank(id)
                || StringUtils.isBlank(applicationName)
                || Objects.isNull(retry)) {
            return false;
        }
        final String filePath = RepositoryPathUtils.buildFilePath(applicationName);
        final String fullFileName = RepositoryPathUtils.getFullFileName(filePath, id);
        final File file = new File(fullFileName);
        final CoordinatorRepositoryAdapter adapter = readRecover(file);
        if (Objects.nonNull(adapter)) {
            try {
                adapter.setLastTime(DateUtils.getDateYYYY());
            } catch (Exception e) {
                e.printStackTrace();
            }
            adapter.setRetriedCount(retry);
            try {
                FileUtils.writeFile(fullFileName, objectSerializer.serialize(adapter));
            } catch (CatException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
        return false;
    }

    private CoordinatorRepositoryAdapter readRecover(final File file) {
        try {
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] content = new byte[(int) file.length()];
                fis.read(content);
                return objectSerializer.deSerialize(content, CoordinatorRepositoryAdapter.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private CatCompensationVO readTransaction(final File file) {
        try {
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] content = new byte[(int) file.length()];
                fis.read(content);
                final CoordinatorRepositoryAdapter adapter =
                        objectSerializer.deSerialize(content, CoordinatorRepositoryAdapter.class);
                return ConvertHelper.buildVO(adapter);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private List<CatCompensationVO> findAll(final File[] files) {
        if (files != null && files.length > 0) {
            return Arrays.stream(files)
                    .map(this::readTransaction)
                    .collect(Collectors.toList());
        }
        return null;
    }

    private List<CatCompensationVO> findByPage(final File[] files, final int start, final int pageSize) {
        if (files != null && files.length > 0) {
            return Arrays.stream(files).skip(start).limit(pageSize)
                    .map(this::readTransaction)
                    .collect(Collectors.toList());
        }
        return null;
    }

}
