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

import lombok.Data;

/**
 * EN：The CatNoticeSafeConfig.
 * CN：Cat 消息通知自动降级
 * @author dzc
 */
@Data
public class CatNoticeSafeConfig {
	
    /**
     * EN：
     * CN：定时器执行时间间隔
     */
	private Integer noticeScheduledDelay=3;

    /**
     * EN：
     * CN：当每秒补偿日志超过N条关闭补偿
     */
    private Integer timesSecond;

    /**
     * EN：
     * CN：当每分钟补偿日志超过N条关闭补偿
     */
    private Integer timesMinute;
    
    
    /**
     * EN：
     * CN：当每小时补偿日志超过N条关闭补偿
     */
    private Integer timesHour;

}
