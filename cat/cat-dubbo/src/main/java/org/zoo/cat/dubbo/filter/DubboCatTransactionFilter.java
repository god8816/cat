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

package org.zoo.cat.dubbo.filter;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import org.zoo.cat.annotation.Cat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.bean.entity.CatInvocation;
import org.zoo.cat.common.bean.entity.CatParticipant;
import org.zoo.cat.common.enums.CatActionEnum;
import org.zoo.cat.common.enums.CatRoleEnum;
import org.zoo.cat.common.exception.CatRuntimeException;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.common.utils.StringUtils;
import org.zoo.cat.core.concurrent.threadlocal.CatTransactionContextLocal;
import org.zoo.cat.core.mediator.RpcMediator;
import org.zoo.cat.core.service.executor.CatTransactionExecutor;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * impl dubbo filter.
 *
 * @author xiaoyu
 */
@Activate(group = {Constants.SERVER_KEY, Constants.CONSUMER})
@SuppressWarnings("all")
public class DubboCatTransactionFilter implements Filter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DubboCatTransactionFilter.class);

    private CatTransactionExecutor catTransactionExecutor;

    /**
     * this is init by dubbo spi
     * set catTransactionExecutor.
     *
     * @param catTransactionExecutor {@linkplain CatTransactionExecutor }
     */
    public void setCatTransactionExecutor(final CatTransactionExecutor catTransactionExecutor) {
        this.catTransactionExecutor = catTransactionExecutor;
    }

    @Override
    public Result invoke(final Invoker<?> invoker, final Invocation invocation) throws RpcException {
        String methodName = invocation.getMethodName();
        Class clazz = invoker.getInterface();
        Class[] args = invocation.getParameterTypes();
        final Object[] arguments = invocation.getArguments();
        Method method = null;
        Cat cat = null;
        try {
            converterParamsClass(args, arguments);
            method = clazz.getMethod(methodName, args);
            cat = method.getAnnotation(Cat.class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
            LogUtil.error(LOGGER, "cat find method error {} ", ex::getMessage);
        }
        if (Objects.nonNull(cat)) {
            try {
                final CatTransactionContext catTransactionContext = CatTransactionContextLocal.getInstance().get();
                if (Objects.nonNull(catTransactionContext)) {
                    RpcMediator.getInstance().transmit(RpcContext.getContext()::setAttachment, catTransactionContext);
                    final Result result = invoker.invoke(invocation);
                    //if result has not exception
                    if (!result.hasException()) {
                        final CatParticipant catParticipant = buildParticipant(catTransactionContext, cat, method, clazz, arguments, args);
                        if (catTransactionContext.getRole() == CatRoleEnum.INLINE.getCode()) {
                            catTransactionExecutor.registerByNested(catTransactionContext.getTransId(),
                                    catParticipant);
                        } else {
                            catTransactionExecutor.enlistParticipant(catParticipant);
                        }
                    } else {
                        throw new CatRuntimeException("rpc invoke exception{}", result.getException());
                    }
                    return result;
                }
                return invoker.invoke(invocation);
            } catch (RpcException e) {
                e.printStackTrace();
                throw e;
            }
        } else {
            return invoker.invoke(invocation);
        }
    }

    @SuppressWarnings("unchecked")
    private CatParticipant buildParticipant(final CatTransactionContext catTransactionContext,
                                              final Cat cat,
                                              final Method method, final Class clazz,
                                              final Object[] arguments, final Class... args) throws CatRuntimeException {

        if (Objects.isNull(catTransactionContext)
                || (CatActionEnum.TRYING.getCode() != catTransactionContext.getAction())) {
            return null;
        }
        //获取协调方法
        String confirmMethodName = cat.confirmMethod();
        if (StringUtils.isBlank(confirmMethodName)) {
            confirmMethodName = method.getName();
        }
        String cancelMethodName = cat.cancelMethod();
        if (StringUtils.isBlank(cancelMethodName)) {
            cancelMethodName = method.getName();
        }
        CatInvocation confirmInvocation = new CatInvocation(clazz, confirmMethodName, args, arguments);
        CatInvocation cancelInvocation = new CatInvocation(clazz, cancelMethodName, args, arguments);
        //封装调用点
        return new CatParticipant(catTransactionContext.getTransId(), confirmInvocation, cancelInvocation);
    }

    private void converterParamsClass(final Class[] args, final Object[] arguments) {
        if (arguments == null || arguments.length < 1) {
            return;
        }
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] != null) {
                args[i] = arguments[i].getClass();
            }
        }
    }
}
