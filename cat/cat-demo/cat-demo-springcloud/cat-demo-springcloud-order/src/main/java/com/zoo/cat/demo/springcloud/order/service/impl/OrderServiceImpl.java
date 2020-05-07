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

package com.zoo.cat.demo.springcloud.order.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zoo.cat.annotation.Cat;
import org.zoo.cat.annotation.TransTypeEnum;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.utils.IdWorkerUtils;

import com.zoo.cat.demo.springcloud.order.entity.Order;
import com.zoo.cat.demo.springcloud.order.enums.OrderStatusEnum;
import com.zoo.cat.demo.springcloud.order.mapper.OrderMapper;
import com.zoo.cat.demo.springcloud.order.service.OrderService;
import com.zoo.cat.demo.springcloud.order.service.PaymentService;

import java.math.BigDecimal;
import java.util.Date;


/**
 * @author dzc
 */
@Service("orderService")
@SuppressWarnings("all")
public class OrderServiceImpl implements OrderService {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderServiceImpl.class);

    private final OrderMapper orderMapper;

    private final PaymentService paymentService;

    @Autowired(required = false)
    public OrderServiceImpl(OrderMapper orderMapper, PaymentService paymentService) {
        this.orderMapper = orderMapper;
        this.paymentService = paymentService;
    }

    @Override
    public String orderPay(Integer count, BigDecimal amount) {
        final Order order = buildOrder(count, amount);
        final int rows = orderMapper.save(order);

        if (rows > 0) {
            paymentService.makePayment(order);
        }
        return "success";
    }

    /**
     * 模拟在订单支付操作中，库存在try阶段中的库存异常
     *
     * @param count  购买数量
     * @param amount 支付金额
     * @return string
     */
    @Override
    public String mockInventoryWithTryException(Integer count, BigDecimal amount) {
        final Order order = buildOrder(count, amount);
        final int rows = orderMapper.save(order);

        if (rows > 0) {
            paymentService.mockPaymentInventoryWithTryException(order);
        }


        return "success";
    }

    /**
     * 模拟在订单支付操作中，库存在try阶段中的timeout
     *
     * @param count  购买数量
     * @param amount 支付金额
     * @return string
     */
    @Override
    public String mockInventoryWithTryTimeout(Integer count, BigDecimal amount) {
        final Order order = buildOrder(count, amount);
        final int rows = orderMapper.save(order);

        if (rows > 0) {
            paymentService.mockPaymentInventoryWithTryTimeout(order);
        }


        return "success";
    }


    @Override
    public void updateOrderStatus(Order order) {
        orderMapper.update(order);
    }

    private Order buildOrder(Integer count, BigDecimal amount) {
        LOGGER.debug("构建订单对象");
        Order order = new Order();
        order.setCreateTime(new Date());
        order.setNumber(IdWorkerUtils.getInstance().buildPartNumber());
        //demo中的表里只有商品id为 1的数据
        order.setProductId("1");
        order.setStatus(OrderStatusEnum.NOT_PAY.getCode());
        order.setTotalAmount(amount);
        order.setCount(count);
        //demo中 表里面存的用户id为10000
        order.setUserId("10000");
        return order;
    }
    
    private Order buildOrder(Integer orderId) {
        LOGGER.debug("构建订单对象");
        Order order = new Order();
        order.setId(orderId);
        order.setCreateTime(new Date());
        order.setNumber(IdWorkerUtils.getInstance().buildPartNumber());
        //demo中的表里只有商品id为2的数据
        order.setProductId("2");
        order.setStatus(OrderStatusEnum.NOT_PAY.getCode());
        order.setTotalAmount(new BigDecimal(100));
        order.setCount(100);
        //demo中 表里面存的用户id为20000
        order.setUserId("20000");
        return order;
    }

	@Override
	@Cat(retryMax=10,timeoutMills=2000,pattern = TransTypeEnum.NOTICE)
	public String createOrderTimeOut(Integer orderId) {
	    final Order order = buildOrder(orderId);
	    //等待5秒模拟超时场景
	    try {
			Thread.sleep(5*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	    
	    final int rows = orderMapper.save(order);
	    if(rows>0)
		   return "success";
	    else
	    	   return "false";
	}

	@Override
	@Cat(retryMax=10,pattern = TransTypeEnum.NOTICE)
	public String createOrderException(Integer orderId) {
		 final Order order = buildOrder(orderId);
		 //模拟抛出异常
		 if(true) {
			 throw new CatException("模拟抛出异常"); 
		 }
		 final int rows = orderMapper.save(order);
	     if(rows>0)
		    return "success";
	     else
	    	    return "false";
	}
}
