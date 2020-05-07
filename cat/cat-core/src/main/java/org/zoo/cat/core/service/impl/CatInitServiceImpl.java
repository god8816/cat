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

package org.zoo.cat.core.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.common.utils.extension.ExtensionLoader;
import org.zoo.cat.core.coordinator.CatCoordinatorService;
import org.zoo.cat.core.helper.SpringBeanUtils;
import org.zoo.cat.core.logo.CatLogo;
import org.zoo.cat.core.service.CatInitService;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

/**
 * cat init service.
 *
 * @author dzc
 */
@Service("catInitService")
public class CatInitServiceImpl implements CatInitService {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CatInitServiceImpl.class);

    private final CatCoordinatorService catCoordinatorService;

    /**
     * Instantiates a new Cat init service.
     *
     * @param catCoordinatorService the cat coordinator service
     */
    @Autowired
    public CatInitServiceImpl(final CatCoordinatorService catCoordinatorService) {
        this.catCoordinatorService = catCoordinatorService;
    }

    /**
     * cat initialization.
     *
     * @param catConfig {@linkplain CatConfig}
     */
    @Override
    public void initialization(final CatConfig catConfig) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> LOGGER.info("cat shutdown now")));
        try {
            loadSpiSupport(catConfig);
            catCoordinatorService.start(catConfig);
        } catch (Exception ex) {
            LogUtil.error(LOGGER, " cat init exception:{}", ex::getMessage);
            System.exit(1);
        }
        new CatLogo().logo();
    }

    /**
     * load spi.
     *
     * @param catConfig {@linkplain CatConfig}
     */
    private void loadSpiSupport(final CatConfig catConfig) {
        //spi serialize
        final ObjectSerializer serializer = ExtensionLoader.getExtensionLoader(ObjectSerializer.class)
                .getActivateExtension(catConfig.getSerializer());

        //spi repository
        final CatCoordinatorRepository repository = ExtensionLoader.getExtensionLoader(CatCoordinatorRepository.class)
                .getActivateExtension(catConfig.getRepositorySupport());

        repository.setSerializer(serializer);

        SpringBeanUtils.getInstance().registerBean(CatCoordinatorRepository.class.getName(), repository);
    }
}
