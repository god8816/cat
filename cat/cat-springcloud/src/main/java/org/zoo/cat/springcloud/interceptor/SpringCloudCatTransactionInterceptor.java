package org.zoo.cat.springcloud.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.zoo.cat.common.bean.context.CatTransactionContext;
import org.zoo.cat.common.enums.CatRoleEnum;
import org.zoo.cat.common.utils.LogUtil;
import org.zoo.cat.core.concurrent.threadlocal.CatTransactionContextLocal;
import org.zoo.cat.core.interceptor.CatTransactionInterceptor;
import org.zoo.cat.core.mediator.RpcMediator;
import org.zoo.cat.core.service.CatTransactionAspectService;

import javax.servlet.http.HttpServletRequest;
import java.util.Objects;


/**
 * SpringCloudCatTransactionInterceptor.
 *
 * @author dzc
 */
@Component
public class SpringCloudCatTransactionInterceptor implements CatTransactionInterceptor {

    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SpringCloudCatTransactionInterceptor.class);

    private final CatTransactionAspectService catTransactionAspectService;

    @Autowired
    public SpringCloudCatTransactionInterceptor(final CatTransactionAspectService catTransactionAspectService) {
        this.catTransactionAspectService = catTransactionAspectService;
    }

    @Override
    public Object interceptor(final ProceedingJoinPoint pjp) throws Throwable {
        CatTransactionContext catTransactionContext = CatTransactionContextLocal.getInstance().get();
        if (Objects.nonNull(catTransactionContext)) {
            if (CatRoleEnum.START.getCode() == catTransactionContext.getRole()) {
                catTransactionContext.setRole(CatRoleEnum.SPRING_CLOUD.getCode());
            }
        } else {
            try {
                final RequestAttributes requestAttributes = RequestContextHolder.currentRequestAttributes();
                catTransactionContext = RpcMediator.getInstance().acquire(key -> {
                    HttpServletRequest request = ((ServletRequestAttributes) requestAttributes).getRequest();
                    return request.getHeader(key);
                });
            } catch (IllegalStateException ex) {
                LogUtil.warn(LOGGER, () -> "can not acquire request info:" + ex.getLocalizedMessage());
            }
        }
        return catTransactionAspectService.invoke(catTransactionContext, pjp);
    }

}
