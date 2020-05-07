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

package org.zoo.cat.common.bean.variate;


import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoo.cat.common.bean.entity.CatNoticeSafe;
import org.zoo.cat.common.config.CatConfig;


/**
 * EN：CatDegradation .
 * CN：系统降级判断
 * @author dzc
 */
public class CatDegradation implements Serializable {

    private static final long serialVersionUID = -5108578223428529356L;
    
    /**
     * logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CatDegradation.class);
    

    public static volatile List<CatNoticeSafe>  catNoticeSafe;
    
    
    /**
     * EN: is start degradation.
     * CN: 是否开启降级
     */
    public static Boolean isStartDegradation(CatConfig catConfig,String className,String methodName) {
    	    if(catConfig.getStarted()==false) {
    	    	  return false;
    	    }else {
        	    Integer timesSecond = catConfig.getCatNoticeSafeConfig().getTimesSecond();
        	    Integer timesMinute = catConfig.getCatNoticeSafeConfig().getTimesMinute();
        	    Integer timesHour = catConfig.getCatNoticeSafeConfig().getTimesHour();
        	    
        	    if(Objects.nonNull(catNoticeSafe)) {
        	    	   for (CatNoticeSafe cns : catNoticeSafe) {
        	    		   
				   if(TimeUnit.SECONDS.name().equals(cns.getTimeUnit())
						   && className.equals(cns.getTargetClass()) 
						      && methodName.equals(cns.getTargetMethod())
						         && cns.getNum() >= timesSecond) {
					                 LOGGER.info("notice close by seconds degradation");
					                 return false;
				   }	
				   
				   if(TimeUnit.MINUTES.name().equals(cns.getTimeUnit())
						   && className.equals(cns.getTargetClass()) 
						      && methodName.equals(cns.getTargetMethod())
						         && cns.getNum() >= timesMinute) {
					                 LOGGER.info("notice close by minutes degradation");
					                 return false;
				   }	
				   
				   if(TimeUnit.HOURS.name().equals(cns.getTimeUnit())
						   && className.equals(cns.getTargetClass()) 
						      && methodName.equals(cns.getTargetMethod())
						         && cns.getNum() >= timesHour) {
					                 LOGGER.info("notice close by hours degradation");
					                 return false;
				   }	
			   }
        	    }
    	    }
		return true;
    }
    
}
