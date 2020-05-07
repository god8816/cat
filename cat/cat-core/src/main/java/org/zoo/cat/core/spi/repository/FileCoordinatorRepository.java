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

import com.google.common.collect.Lists;
import org.zoo.cat.annotation.CatSPI;
import org.zoo.cat.common.bean.adapter.CoordinatorRepositoryAdapter;
import org.zoo.cat.common.bean.entity.CatNoticeSafe;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.enums.RepositorySupportEnum;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.exception.CatRuntimeException;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.common.utils.FileUtils;
import org.zoo.cat.common.utils.RepositoryConvertUtils;
import org.zoo.cat.common.utils.RepositoryPathUtils;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * file impl.
 * @author dzc
 */
@SuppressWarnings("all")
@CatSPI("file")
public class FileCoordinatorRepository implements CatCoordinatorRepository {

    private static volatile boolean initialized;

    private String filePath;

    private ObjectSerializer serializer;

    @Override
    public void setSerializer(final ObjectSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public int create(final CatTransaction catTransaction) {

        writeFile(catTransaction);
        return 1;
    }

    @Override
    public int remove(final String id) {
        String fullFileName = RepositoryPathUtils.getFullFileName(filePath, id);
        File file = new File(fullFileName);
        if (file.exists()) {
            file.delete();
        }
        return ROWS;
    }

    @Override
    public int update(final CatTransaction catTransaction) throws CatRuntimeException {
        catTransaction.setLastTime(new Date());
        catTransaction.setVersion(catTransaction.getVersion() + 1);
        catTransaction.setRetriedCount(catTransaction.getRetriedCount() + 1);
        try {
            writeFile(catTransaction);
        } catch (Exception e) {
            throw new CatRuntimeException("update data exception!");
        }
        return 1;
    }

    @Override
    public int updateParticipant(final CatTransaction catTransaction) {
        try {
            final String fullFileName = RepositoryPathUtils.getFullFileName(filePath, catTransaction.getTransId());
            final File file = new File(fullFileName);
            final CoordinatorRepositoryAdapter adapter = readAdapter(file);
            if (Objects.nonNull(adapter)) {
                adapter.setContents(serializer.serialize(catTransaction.getCatParticipants()));
            }
            FileUtils.writeFile(fullFileName, serializer.serialize(adapter));
        } catch (Exception e) {
            throw new CatRuntimeException("update data exception!");
        }
        return ROWS;
    }

    @Override
    public int updateStatus(final String id, final Integer status) {
        try {
            final String fullFileName = RepositoryPathUtils.getFullFileName(filePath, id);
            final File file = new File(fullFileName);
            final CoordinatorRepositoryAdapter adapter = readAdapter(file);
            if (Objects.nonNull(adapter)) {
                adapter.setStatus(status);
            }
            FileUtils.writeFile(fullFileName, serializer.serialize(adapter));
        } catch (Exception e) {
            throw new CatRuntimeException("update data exception!");
        }
        return ROWS;
    }

    @Override
    public CatTransaction findById(final String id) {
        String fullFileName = RepositoryPathUtils.getFullFileName(filePath, id);
        File file = new File(fullFileName);
        try {
            return readTransaction(file);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<CatTransaction> listAll() {
        List<CatTransaction> transactionRecoverList = Lists.newArrayList();
        File path = new File(filePath);
        File[] files = path.listFiles();
        if (files != null && files.length > 0) {
            for (File file : files) {
                try {
                    CatTransaction transaction = readTransaction(file);
                    transactionRecoverList.add(transaction);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return transactionRecoverList;
    }

    @Override
    public List<CatTransaction> listAllByDelay(final Date date) {
        final List<CatTransaction> catTransactions = listAll();
        return catTransactions.stream()
                .filter(tccTransaction -> tccTransaction.getLastTime().compareTo(date) < 0)
                .collect(Collectors.toList());
    }

    @Override
    public void init(final String modelName, final CatConfig catConfig) {
        filePath = RepositoryPathUtils.buildFilePath(modelName);
        File file = new File(filePath);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.mkdirs();
        }
    }

    @Override
    public String getScheme() {
        return RepositorySupportEnum.FILE.getSupport();
    }

    private void writeFile(final CatTransaction catTransaction) {
        makeDir();
        String fileName = RepositoryPathUtils.getFullFileName(filePath, catTransaction.getTransId());
        try {
            FileUtils.writeFile(fileName, RepositoryConvertUtils.convert(catTransaction, serializer));
        } catch (CatException e) {
            e.printStackTrace();
        }
    }

    private CatTransaction readTransaction(final File file) throws Exception {
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] content = new byte[(int) file.length()];
            fis.read(content);
            return RepositoryConvertUtils.transformBean(content, serializer);
        }
    }

    private CoordinatorRepositoryAdapter readAdapter(final File file) throws Exception {
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] content = new byte[(int) file.length()];
            fis.read(content);
            return serializer.deSerialize(content, CoordinatorRepositoryAdapter.class);
        }
    }

    private void makeDir() {
        if (!initialized) {
            synchronized (FileCoordinatorRepository.class) {
                if (!initialized) {
                    File rootPathFile = new File(filePath);
                    if (!rootPathFile.exists()) {

                        boolean result = rootPathFile.mkdir();

                        if (!result) {
                            throw new CatRuntimeException("cannot create root path, the path to create is:" + filePath);
                        }
                        initialized = true;
                    } else if (!rootPathFile.isDirectory()) {
                        throw new CatRuntimeException("rootPath is not directory");
                    }
                }
            }
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
