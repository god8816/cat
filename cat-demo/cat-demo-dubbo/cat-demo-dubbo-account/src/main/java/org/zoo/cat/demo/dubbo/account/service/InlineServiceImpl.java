package org.zoo.cat.demo.dubbo.account.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.zoo.cat.annotation.Cat;
import org.zoo.cat.annotation.TransTypeEnum;
import org.zoo.cat.demo.dubbo.account.api.service.InlineService;

/**
 * InlineServiceImpl.
 *
 * @author dzc
 */
@Service("inlineService")
public class InlineServiceImpl implements InlineService {


    private static final Logger LOGGER = LoggerFactory.getLogger(InlineServiceImpl.class);

    @Override
    @Cat(pattern = TransTypeEnum.TCC,confirmMethod = "inLineConfirm", cancelMethod = "inLineCancel")
    public void testInline() {
        LOGGER.info("===========执行inline try 方法==============");
    }

    public void inLineConfirm() {
        LOGGER.info("===========执行inlineConfirm 方法==============");
    }


    public void inLineCancel() {
        LOGGER.info("===========执行inLineCancel 方法==============");
    }

}
