package org.zoo.cat.demo.dubbo.account.ext.serializer;


import org.zoo.cat.annotation.CatSPI;
import org.zoo.cat.common.exception.CatException;
import org.zoo.cat.common.serializer.ObjectSerializer;

/**
 * @author dzc
 */
@CatSPI("custom")
public class CustomSerializer implements ObjectSerializer {

    @Override
    public byte[] serialize(Object obj) throws CatException {
        return new byte[0];
    }

    @Override
    public <T> T deSerialize(byte[] param, Class<T> clazz) throws CatException {
        return null;
    }
}
