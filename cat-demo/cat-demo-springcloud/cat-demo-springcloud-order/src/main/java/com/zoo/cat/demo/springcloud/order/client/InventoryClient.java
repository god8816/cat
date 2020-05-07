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

package com.zoo.cat.demo.springcloud.order.client;

import org.zoo.cat.annotation.Cat;
import org.zoo.cat.annotation.TransTypeEnum;

import com.zoo.cat.demo.springcloud.order.dto.InventoryDTO;

import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * The interface Inventory client.
 *
 * @author dzc
 */
@FeignClient(value = "inventory-service")
public interface InventoryClient {

    /**
     * 库存扣减.
     *
     * @param inventoryDTO 实体对象
     * @return true 成功
     */
    @RequestMapping("/inventory/decrease")
    @Cat(pattern = TransTypeEnum.TCC)
    Boolean decrease(@RequestBody InventoryDTO inventoryDTO);


    /**
     * 获取商品库存.
     *
     * @param productId 商品id
     * @return InventoryDO integer
     */
    @RequestMapping("/inventory/findByProductId")
    Integer findByProductId(@RequestParam("productId") String productId);


    /**
     * 模拟库存扣减异常.
     *
     * @param inventoryDTO 实体对象
     * @return true 成功
     */
    @Cat(pattern = TransTypeEnum.TCC)
    @RequestMapping("/inventory/mockWithTryException")
    Boolean mockWithTryException(@RequestBody InventoryDTO inventoryDTO);


    /**
     * 模拟库存扣减超时.
     *
     * @param inventoryDTO 实体对象
     * @return true 成功
     */
    @Cat(pattern = TransTypeEnum.TCC)
    @RequestMapping("/inventory/mockWithTryTimeout")
    Boolean mockWithTryTimeout(@RequestBody InventoryDTO inventoryDTO);
}
