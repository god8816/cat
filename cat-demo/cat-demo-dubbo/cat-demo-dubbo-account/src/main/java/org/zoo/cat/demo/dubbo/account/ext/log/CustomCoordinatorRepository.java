package org.zoo.cat.demo.dubbo.account.ext.log;


import org.zoo.cat.annotation.CatSPI;
import org.zoo.cat.common.bean.entity.CatNoticeSafe;
import org.zoo.cat.common.bean.entity.CatTransaction;
import org.zoo.cat.common.config.CatConfig;
import org.zoo.cat.common.serializer.ObjectSerializer;
import org.zoo.cat.core.spi.CatCoordinatorRepository;

import java.util.Date;
import java.util.List;

/**
 * @author dzc
 */
@CatSPI("custom")
public class CustomCoordinatorRepository implements CatCoordinatorRepository {

    private ObjectSerializer serializer;

    @Override
    public int create(CatTransaction catTransaction) {
        return 0;
    }

    @Override
    public int remove(String id) {
        return 0;
    }

    @Override
    public int update(CatTransaction catTransaction) {
        return 0;
    }

    @Override
    public int updateParticipant(CatTransaction catTransaction) {
        return 0;
    }

    @Override
    public int updateStatus(String id, Integer status) {
        return 0;
    }

    @Override
    public CatTransaction findById(String id) {
        return null;
    }

    @Override
    public List<CatTransaction> listAll() {
        return null;
    }

    @Override
    public List<CatTransaction> listAllByDelay(Date date) {
        return null;
    }

    @Override
    public void init(String modelName, CatConfig catConfig) {
        System.out.println("executor customer CustomCoordinatorRepository");
    }

    @Override
    public String getScheme() {
        return null;
    }

    @Override
    public void setSerializer(ObjectSerializer objectSerializer) {
        this.serializer = objectSerializer;
    }

	@Override
	public List<CatNoticeSafe> countLogsByDelay(Date acquireData, String timeUnit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int removeLogsByDelay(Date acquireSecondsData) {
		// TODO Auto-generated method stub
		return 0;
	}
}
