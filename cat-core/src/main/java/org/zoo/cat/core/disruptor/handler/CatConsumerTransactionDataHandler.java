package org.zoo.cat.core.disruptor.handler;

import org.zoo.cat.core.disruptor.AbstractDisruptorConsumerExecutor;
import org.zoo.cat.core.disruptor.DisruptorConsumerFactory;
import org.zoo.cat.core.service.CatTransactionHandlerAlbum;

/**
 * CatTransactionHandler.
 * About the processing of a rotation function.
 *
 * @author chenbin sixh
 */
public class CatConsumerTransactionDataHandler extends AbstractDisruptorConsumerExecutor<CatTransactionHandlerAlbum> implements DisruptorConsumerFactory {


    @Override
    public String fixName() {
        return "CatConsumerTransactionDataHandler";
    }

    @Override
    public AbstractDisruptorConsumerExecutor create() {
        return this;
    }

    @Override
    public void executor(final CatTransactionHandlerAlbum data) {
        data.run();
    }
}

