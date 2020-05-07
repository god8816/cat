package org.zoo.cat.core.disruptor.handler;

import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.enums.EventTypeEnum;
import org.zoo.cat.core.concurrent.ConsistentHashSelector;
import org.zoo.cat.core.coordinator.CatCoordinatorService;
import org.zoo.cat.core.disruptor.AbstractDisruptorConsumerExecutor;
import org.zoo.cat.core.disruptor.DisruptorConsumerFactory;
import org.zoo.cat.core.disruptor.event.CatTransactionEvent;

/**
 * this is disruptor consumer.
 *
 * @author dzc
 */
public class CatConsumerLogDataHandler extends AbstractDisruptorConsumerExecutor<CatTransactionEvent> implements DisruptorConsumerFactory {

    private ConsistentHashSelector executor;

    private final CatCoordinatorService coordinatorService;

    public CatConsumerLogDataHandler(final ConsistentHashSelector executor, final CatCoordinatorService coordinatorService) {
        this.executor = executor;
        this.coordinatorService = coordinatorService;
    }

    @Override
    public String fixName() {
        return "CatConsumerDataHandler";
    }

    @Override
    public AbstractDisruptorConsumerExecutor create() {
        return this;
    }

    @Override
    public void executor(final CatTransactionEvent event) {
        String transId = event.getCatTransaction().getTransId();
        executor.select(transId).execute(() -> {
            EventTypeEnum eventTypeEnum = EventTypeEnum.buildByCode(event.getType());
            switch (eventTypeEnum) {
                case SAVE:
                    coordinatorService.save(event.getCatTransaction());
                    break;
                case DELETE:
                    coordinatorService.remove(event.getCatTransaction().getTransId());
                    break;
                case UPDATE_STATUS:
                    final CatTransaction catTransaction = event.getCatTransaction();
                    coordinatorService.updateStatus(catTransaction.getTransId(), catTransaction.getStatus());
                    break;
                case UPDATE_PARTICIPANT:
                    coordinatorService.updateParticipant(event.getCatTransaction());
                    break;
                default:
                    break;
            }
            event.clear();
        });
    }
}
