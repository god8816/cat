package org.zoo.cat.core.concurrent.threadlocal;

import java.util.Objects;

import org.zoo.cat.common.bean.context.CatTransactionContext;

/**
 * transactionContext in threadLocal.
 * @author dzc
 */
public class RootContext {

	  private RootContext() {

	  }
	  
	  
     /**
      * Gets transId.
      *
      * @return the transId
      */
     public static String getTransId() {
     	CatTransactionContext catTransactionContext = CatTransactionContextLocal.getInstance().get();
     	if(Objects.nonNull(catTransactionContext)) {
     		  String transId = catTransactionContext.getTransId();
     	      return transId;
     	}else {
     		  return null;
     	}
    }
}
