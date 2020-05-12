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

package org.zoo.cat.core.service.executor;

import com.google.common.collect.Lists;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.zoo.cat.annotation.Cat;
import org.zoo.cat.annotation.TransTypeEnum;
import org.zoo.cat.common.utils.CollectionUtils;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.bean.entity.CatInvocation;
import org.zoo.cat.common.bean.entity.CatParticipant;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.enums.EventTypeEnum;
import org.zoo.cat.common.enums.CatActionEnum;
import org.zoo.cat.common.enums.CatRoleEnum;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.exception.CatRuntimeException;
import org.zoo.cat.core.cache.CatTransactionGuavaCacheManager;
import org.zoo.cat.core.concurrent.threadlocal.CatTransactionContextLocal;
import org.zoo.cat.core.disruptor.publisher.CatTransactionEventPublisher;
import org.zoo.cat.core.reflect.CatReflector;
import org.zoo.cat.core.utils.JoinPointUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * this is cat transaction manager.
 *
 * @author dzc
 */
@Component
public class CatTransactionExecutor {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CatTransactionExecutor.class);

    /**
     * transaction save threadLocal.
     */
    private static final ThreadLocal<CatTransaction> CURRENT = new ThreadLocal<>();

    private final CatTransactionEventPublisher catTransactionEventPublisher;

    /**
     * Instantiates a new Cat transaction executor.
     *
     * @param catTransactionEventPublisher the cat transaction event publisher
     */
    @Autowired
    public CatTransactionExecutor(final CatTransactionEventPublisher catTransactionEventPublisher) {
        this.catTransactionEventPublisher = catTransactionEventPublisher;
    }

    /**
     * transaction preTry.
     *
     * @param point cut point.
     * @return TccTransaction cat transaction
     */
    public CatTransaction preTry(final ProceedingJoinPoint point) {
        LogUtil.debug(LOGGER, () -> "......cat transaction starter....");
        //build tccTransaction
        final CatTransaction catTransaction = buildCatTransaction(point, CatRoleEnum.START.getCode(), null);
        //save tccTransaction in threadLocal
        CURRENT.set(catTransaction);
        //publishEvent
        catTransactionEventPublisher.publishEvent(catTransaction, EventTypeEnum.SAVE.getCode());
        //set TccTransactionContext this context transfer remote
        CatTransactionContext context = new CatTransactionContext();
        //set action is try
        context.setAction(CatActionEnum.TRYING.getCode());
        context.setTransId(catTransaction.getTransId());
        context.setRole(CatRoleEnum.START.getCode());
        CatTransactionContextLocal.getInstance().set(context);
        return catTransaction;
    }
    
    /**
     * transaction preTry.
     *
     * @param point cut point.
     * @return TccTransaction cat transaction
     */
    public CatTransaction preTryNotice(final ProceedingJoinPoint point) {
        LogUtil.debug(LOGGER, () -> "......cat transaction starter....");
        //build noticeTransaction
        final CatTransaction catTransaction = buildCatTransaction(point, CatRoleEnum.START.getCode(), null);
        //save noticeTransaction in threadLocal
        CURRENT.set(catTransaction);
        //publishEvent
        catTransactionEventPublisher.publishEvent(catTransaction, EventTypeEnum.SAVE.getCode());
        //set TccTransactionContext this context transfer remote
        CatTransactionContext context = new CatTransactionContext();
        //set action is notice
        context.setAction(CatActionEnum.NOTICEING.getCode());
        context.setTransId(catTransaction.getTransId());
        context.setRole(CatRoleEnum.START.getCode());
        CatTransactionContextLocal.getInstance().set(context);
        return catTransaction;
    }

    /**
     * this is Participant transaction preTry.
     *
     * @param context transaction context.
     * @param point   cut point
     * @return TccTransaction cat transaction
     */
    public CatTransaction preTryParticipant(final CatTransactionContext context, final ProceedingJoinPoint point) {
        LogUtil.debug(LOGGER, "participant cat transaction start..：{}", context::toString);
        final CatTransaction catTransaction = buildCatTransaction(point, CatRoleEnum.PROVIDER.getCode(), context.getTransId());
        //cache by guava
        CatTransactionGuavaCacheManager.getInstance().cacheCatTransaction(catTransaction);
        //publishEvent
        catTransactionEventPublisher.publishEvent(catTransaction, EventTypeEnum.SAVE.getCode());
        //Nested transaction support
        context.setRole(CatRoleEnum.LOCAL.getCode());
        CatTransactionContextLocal.getInstance().set(context);
        return catTransaction;
    }
    

    /**
     * Call the confirm method and basically if the initiator calls here call the remote or the original method
     * However, the context sets the call confirm
     * The remote service calls the confirm method.
     *
     * @param currentTransaction {@linkplain CatTransaction}
     * @return the object
     * @throws CatRuntimeException ex
     */
    public Object confirm(final CatTransaction currentTransaction) throws CatRuntimeException {
        LogUtil.debug(LOGGER, () -> "cat transaction confirm .......！start");
        if (Objects.isNull(currentTransaction) || CollectionUtils.isEmpty(currentTransaction.getCatParticipants())) {
            return null;
        }
        currentTransaction.setStatus(CatActionEnum.CONFIRMING.getCode());
        updateStatus(currentTransaction);
        final List<CatParticipant> catParticipants = currentTransaction.getCatParticipants();
        boolean success = true;
        if (CollectionUtils.isNotEmpty(catParticipants)) {
            List<CatParticipant> failList = Lists.newArrayListWithCapacity(catParticipants.size());
            List<Object> results = Lists.newArrayListWithCapacity(catParticipants.size());
            for (CatParticipant catParticipant : catParticipants) {
                try {
                    final Object result = CatReflector.executor(catParticipant.getTransId(),
                            CatActionEnum.CONFIRMING,
                            catParticipant.getConfirmCatInvocation());
                    results.add(result);
                } catch (Exception e) {
                    LogUtil.error(LOGGER, "execute confirm :{}", () -> e);
                    success = false;
                    failList.add(catParticipant);
                } finally {
                    CatTransactionContextLocal.getInstance().remove();
                }
            }
            executeHandler(success, currentTransaction, failList);
            return results.get(0);
        }
        return null;
    }

    /**
     * cancel transaction.
     *
     * @param currentTransaction {@linkplain CatTransaction}
     * @return the object
     */
    public Object cancel(final CatTransaction currentTransaction) {
        LogUtil.debug(LOGGER, () -> "tcc cancel ...........start!");
        if (Objects.isNull(currentTransaction) || CollectionUtils.isEmpty(currentTransaction.getCatParticipants())) {
            return null;
        }
        //if cc pattern，can not execute cancel
        if (currentTransaction.getStatus() == CatActionEnum.TRYING.getCode()
                && Objects.equals(currentTransaction.getPattern(), TransTypeEnum.CC.getCode())) {
            deleteTransaction(currentTransaction);
            return null;
        }
        currentTransaction.setStatus(CatActionEnum.CANCELING.getCode());
        //update cancel
        updateStatus(currentTransaction);
        final List<CatParticipant> catParticipants = filterPoint(currentTransaction);
        boolean success = true;
        if (CollectionUtils.isNotEmpty(catParticipants)) {
            List<CatParticipant> failList = Lists.newArrayListWithCapacity(catParticipants.size());
            List<Object> results = Lists.newArrayListWithCapacity(catParticipants.size());
            for (CatParticipant catParticipant : catParticipants) {
                try {
                    final Object result = CatReflector.executor(catParticipant.getTransId(),
                            CatActionEnum.CANCELING,
                            catParticipant.getCancelCatInvocation());
                    results.add(result);
                } catch (Exception e) {
                    LogUtil.error(LOGGER, "execute cancel ex:{}", () -> e);
                    success = false;
                    failList.add(catParticipant);
                } finally {
                    CatTransactionContextLocal.getInstance().remove();
                }
            }
            executeHandler(success, currentTransaction, failList);
            return results.get(0);
        }
        return null;
    }
    
    
    /**
     * transaction preNotice.
     *
     * @param point cut point.
     * @return TccTransaction cat transaction
     */
    public CatTransaction preNotice(final ProceedingJoinPoint point) {
        LogUtil.debug(LOGGER, () -> "......cat transaction starter....");
        //build tccTransaction
        final CatTransaction catTransaction = buildCatTransaction(point, CatRoleEnum.START.getCode(), null);
        //save tccTransaction in threadLocal
        CURRENT.set(catTransaction);
        //publishEvent
        catTransactionEventPublisher.publishEvent(catTransaction, EventTypeEnum.SAVE.getCode());
        //set TccTransactionContext this context transfer remote
        CatTransactionContext context = new CatTransactionContext();
        //set action is try
        context.setAction(CatActionEnum.NOTICEING.getCode());
        context.setTransId(catTransaction.getTransId());
        context.setRole(CatRoleEnum.START.getCode());
        CatTransactionContextLocal.getInstance().set(context);
        return catTransaction;
    }
    
    /**
     * this is Participant transaction preNoticeParticipant.
     *
     * @param context transaction context.
     * @param point   cut point
     * @return CatTransaction cat transaction
     */
    public CatTransaction preNoticeParticipant(final CatTransactionContext context, final ProceedingJoinPoint point) {
        LogUtil.debug(LOGGER, "participant cat transaction start..：{}", context::toString);
        final CatTransaction catTransaction = buildCatTransaction(point, CatRoleEnum.PROVIDER.getCode(), context.getTransId());
        //cache by guava
        CatTransactionGuavaCacheManager.getInstance().cacheCatTransaction(catTransaction);
        //Nested transaction support
        context.setRole(CatRoleEnum.LOCAL.getCode());
        CatTransactionContextLocal.getInstance().set(context);
        return catTransaction;
    }


    /**
     * update transaction status by disruptor.
     *
     * @param catTransaction {@linkplain CatTransaction}
     */
    public void updateStatus(final CatTransaction catTransaction) {
        catTransactionEventPublisher.publishEvent(catTransaction, EventTypeEnum.UPDATE_STATUS.getCode());
    }

    /**
     * delete transaction by disruptor.
     *
     * @param catTransaction {@linkplain CatTransaction}
     */
    public void deleteTransaction(final CatTransaction catTransaction) {
        catTransactionEventPublisher.publishEvent(catTransaction, EventTypeEnum.DELETE.getCode());
    }

    /**
     * update Participant in transaction by disruptor.
     *
     * @param catTransaction {@linkplain CatTransaction}
     */
    public void updateParticipant(final CatTransaction catTransaction) {
        catTransactionEventPublisher.publishEvent(catTransaction, EventTypeEnum.UPDATE_PARTICIPANT.getCode());
    }

    /**
     * acquired by threadLocal.
     *
     * @return {@linkplain CatTransaction}
     */
    public CatTransaction getCurrentTransaction() {
        return CURRENT.get();
    }


    /**
     * clean threadLocal help gc.
     */
    public void remove() {
        CURRENT.remove();
    }

    /**
     * add participant.
     *
     * @param catParticipant {@linkplain CatParticipant}
     */
    public void enlistParticipant(final CatParticipant catParticipant) {
        if (Objects.isNull(catParticipant)) {
            return;
        }
        Optional.ofNullable(getCurrentTransaction())
                .ifPresent(c -> {
                    c.registerParticipant(catParticipant);
                    updateParticipant(c);
                });
    }

    /**
     * when nested transaction add participant.
     *
     * @param transId          key
     * @param catParticipant {@linkplain CatParticipant}
     */
    public void registerByNested(final String transId, final CatParticipant catParticipant) {
        if (Objects.isNull(catParticipant)
                || Objects.isNull(catParticipant.getCancelCatInvocation())
                || Objects.isNull(catParticipant.getConfirmCatInvocation())) {
            return;
        }
        final CatTransaction catTransaction =
                CatTransactionGuavaCacheManager.getInstance().getCatTransaction(transId);
        Optional.ofNullable(catTransaction)
                .ifPresent(transaction -> {
                    transaction.registerParticipant(catParticipant);
                    updateParticipant(transaction);
                });
    }

    public void executeHandler(final boolean success, final CatTransaction currentTransaction, final List<CatParticipant> failList) {
        CatTransactionGuavaCacheManager.getInstance().removeByKey(currentTransaction.getTransId());
        if (success) {
            deleteTransaction(currentTransaction);
        } else {
            currentTransaction.setCatParticipants(failList);
            updateParticipant(currentTransaction);
            throw new CatRuntimeException(failList.toString());
        }
    }

    private List<CatParticipant> filterPoint(final CatTransaction currentTransaction) {
        final List<CatParticipant> catParticipants = currentTransaction.getCatParticipants();
        if (CollectionUtils.isNotEmpty(catParticipants)) {
            if ( 
            		(currentTransaction.getStatus() == CatActionEnum.TRYING.getCode() 
            		 || currentTransaction.getStatus() == CatActionEnum.NOTICEING.getCode())
                    && currentTransaction.getRole() == CatRoleEnum.START.getCode()) {
                return catParticipants.stream()
                        .limit(catParticipants.size())
                        .filter(Objects::nonNull).collect(Collectors.toList());
            }
        }
        return catParticipants;
    }

    private CatTransaction buildCatTransaction(final ProceedingJoinPoint point, final int role, final String transId) {
        CatTransaction catTransaction;
        if (StringUtils.isNoneBlank(transId)) {
            catTransaction = new CatTransaction(transId);
        } else {
            catTransaction = new CatTransaction();
        }
        catTransaction.setStatus(CatActionEnum.PRE_TRY.getCode());
        catTransaction.setRole(role);
        Method method = JoinPointUtils.getMethod(point);
        Class<?> clazz = point.getTarget().getClass();
        Object[] args = point.getArgs();
    
   
    	    final Cat cat = method.getAnnotation(Cat.class);
        final TransTypeEnum pattern = cat.pattern();
        if(Objects.isNull(pattern)) {
         	LOGGER.error("事务补偿模式必须在TCC,SAGA,CC,NOTICE中选择"); 
        }
        catTransaction.setTargetClass(clazz.getName());
        catTransaction.setTargetMethod(method.getName());
        catTransaction.setPattern(pattern.getCode());
        catTransaction.setRetryMax(cat.retryMax());
        catTransaction.setTransType(cat.pattern().getDesc());
        catTransaction.setTimeoutMills(cat.timeoutMills());
        
        String targetMethod = method.getName(); 
        String confirmMethodName = cat.confirmMethod();
        String cancelMethodName = cat.cancelMethod();
        
        //判断是否是通知模式
        if(cat.pattern().getCode()==TransTypeEnum.NOTICE.getCode()) {
            CatInvocation noticeInvocation = null;
            if (StringUtils.isNoneBlank(targetMethod)) {
                catTransaction.setTargetMethod(targetMethod);
                noticeInvocation = new CatInvocation(clazz, targetMethod, method.getParameterTypes(), args);
            }
            final CatParticipant catParticipant = new CatParticipant(catTransaction.getTransId(), noticeInvocation);
            catTransaction.registerParticipant(catParticipant);
            catTransaction.setStatus(CatActionEnum.NOTICEING.getCode());
            catTransaction.setRole(role);
            return catTransaction;
        }else {
          	CatInvocation confirmInvocation = null;
	        CatInvocation cancelInvocation = null;
	        if (StringUtils.isNoneBlank(confirmMethodName)) {
	             catTransaction.setConfirmMethod(confirmMethodName);
	             confirmInvocation = new CatInvocation(clazz, confirmMethodName, method.getParameterTypes(), args);
	        }
	        if (StringUtils.isNoneBlank(cancelMethodName)) {
	             catTransaction.setCancelMethod(cancelMethodName);
	             cancelInvocation = new CatInvocation(clazz, cancelMethodName, method.getParameterTypes(), args);
	        }
	        final CatParticipant catParticipant = new CatParticipant(catTransaction.getTransId(), confirmInvocation, cancelInvocation);
	        catTransaction.registerParticipant(catParticipant);
	        return catTransaction;
        }
    }

    /**
     * EN: notice transaction. 
     * CN：消息通知事务补偿
     * @param currentTransaction {@linkplain CatTransaction}
     * @return the object
     */
	public Object notice(final CatTransaction currentTransaction) {
		    LogUtil.debug(LOGGER, () -> "notice compensate...........start!");
	        if (Objects.isNull(currentTransaction) || CollectionUtils.isEmpty(currentTransaction.getCatParticipants())) {
	            return null;
	        }
	        currentTransaction.setStatus(CatActionEnum.NOTICEING.getCode());
	        updateStatus(currentTransaction);
	        final List<CatParticipant> catParticipants = filterPoint(currentTransaction);
	        boolean success = true;
	        if (CollectionUtils.isNotEmpty(catParticipants)) {
	            List<CatParticipant> failList = Lists.newArrayListWithCapacity(catParticipants.size());
	            List<Object> results = Lists.newArrayListWithCapacity(catParticipants.size());
	            for (CatParticipant catParticipant : catParticipants) {
	                try {
	                  	Long startTime = System.currentTimeMillis();
	                    final Object result = CatReflector.executor(catParticipant.getTransId(),
	                            CatActionEnum.NOTICEING,
	                            catParticipant.getNoticeCatInvocation());
	                    Long endTime = System.currentTimeMillis();
	                    if(currentTransaction.getTimeoutMills()>0 && endTime-startTime>currentTransaction.getTimeoutMills()) {
	                      	throw new CatException("method "+currentTransaction.getTargetMethod()+" timeout..");
	                    }
	                    results.add(result);
	                } catch (Exception e) {
	                    LogUtil.error(LOGGER, "execute notice ex:{}", () -> e);
	                    success = false;
	                    failList.add(catParticipant);
	                } finally {
	                    CatTransactionContextLocal.getInstance().remove();
	                }
	            }
	            //删除补偿
	            executeHandler(success, currentTransaction, failList);
	            return results.get(0);
	        }
	        return null;
	}

}
