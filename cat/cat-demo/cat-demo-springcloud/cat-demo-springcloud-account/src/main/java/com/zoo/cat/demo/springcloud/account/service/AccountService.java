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

package com.zoo.cat.demo.springcloud.account.service;

import org.zoo.cat.annotation.Cat;
import org.zoo.cat.annotation.TransTypeEnum;

import com.zoo.cat.demo.springcloud.account.dto.AccountDTO;
import com.zoo.cat.demo.springcloud.account.entity.AccountDO;

/**
 * AccountService.f
 *
 * @author dzc
 */
public interface AccountService {

    /**
     * 扣款支付.
     *
     * @param accountDTO 参数dto
     * @return true
     */
    @Cat(pattern = TransTypeEnum.TCC)
    boolean payment(AccountDTO accountDTO);

    /**
     * 获取用户账户信息.
     *
     * @param userId 用户id
     * @return AccountDO
     */
    @Cat(retryMax=10,pattern = TransTypeEnum.NOTICE)
    AccountDO findByUserId(String userId);
}
