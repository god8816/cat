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

package org.zoo.cat.common.config;

import org.zoo.cat.common.enums.RepositorySupportEnum;
import org.zoo.cat.common.enums.SerializeEnum;

import lombok.Data;

/**
 * cat config.
 *
 * @author dzc
 */
@Data
public class CatConfig {

    /**
     * EN：Resource suffix this parameter please fill in about is the transaction store path.
     * If it's a table store this is a table suffix, it's stored the same way.
     * If this parameter is not filled in, the applicationName of the application is retrieved by default
     * CN: 补偿日志入库名称
     */
    private String repositorySuffix;

    /**
     * this is map db concurrencyScale.
     */
    private Integer concurrencyScale = 512;

    /**
     * EN：log serializer.{@linkplain SerializeEnum}
     * CN：默认序列化
     */
    private String serializer = "kryo";

    /**
     * EN：scheduledPool Thread size.
     * CN：scheduledPool线程大小
     */
    private int scheduledThreadMax = Runtime.getRuntime().availableProcessors() << 1;

    /**
     * EN：scheduledPool scheduledDelay unit SECONDS.
     * CN：定时器执行时间间隔
     */
    private int scheduledDelay = 60;

    /**
     * EN:scheduledPool scheduledInitDelay unit SECONDS.
     * CN:定时器延时执行时间
     */
    private int scheduledInitDelay = 120;

    /**
     * EN: retry max.
     * CN: 最大重试次数
     */
    private int retryMax = 3;

    /**
     * recoverDelayTime Unit seconds
     * (note that this time represents how many seconds after the local transaction was created before execution).
     */
    private int recoverDelayTime = 60;

    /**
     * Parameters when participants perform their own recovery.
     * 1.such as RPC calls time out
     * 2.such as the starter down machine
     */
    private int loadFactor = 2;

    /**
     * EN:repositorySupport.{@linkplain RepositorySupportEnum}
     * CN:选择模式db，redis，zk，mongo，file
     */
    private String repositorySupport = "db";

    /**
     * EN:disruptor bufferSize.
     * CN:
     */
    private int bufferSize = 4096 * 2 * 2;

    /**
     * EN：this is disruptor consumerThreads.
     * CN：
     */
    private int consumerThreads = Runtime.getRuntime().availableProcessors() << 1;

    /**
     * EN：this is cat async execute cancel or confirm or notice thread size.
     * CN： cancel or confirm or notice执行线程池
     */
    private int asyncThreads = Runtime.getRuntime().availableProcessors() << 1;

    /**
     * EN：when start this set true  actor set false.
     * CN：框架开关
     */
    private Boolean started = true;
    
    /**
     * EN：cat safe config.
     * CN：消息通知自动降级
     */
    private CatNoticeSafeConfig catNoticeSafeConfig = new CatNoticeSafeConfig();
    
    /**
     * EN: Cat log config
     * CN: Cat 日志自动清理配置
     */
    private CatLogConfig catLogConfig = new CatLogConfig();

    /**
     * EN：db config.
     * CN：数据库配置
     */
    private CatDbConfig catDbConfig;

    /**
     * EN：mongo config.
     * CN：mongo 配置
     */
    private CatMongoConfig catMongoConfig;

    /**
     * EN：redis config.
     * CN：redis配置
     */
    private CatRedisConfig catRedisConfig;

    /**
     * EN：zookeeper config.
     * CN：zk配置
     */
    private CatZookeeperConfig catZookeeperConfig;

    /**
     * EN：file config.
     * CN：文件配置
     */
    private CatFileConfig catFileConfig;

}
