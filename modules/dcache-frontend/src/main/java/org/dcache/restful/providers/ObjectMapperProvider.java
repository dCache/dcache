package org.dcache.restful.providers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * Fine-tuning how JSON we be presented by redefine the default Jackson behaviour.
 **/
@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper>
{
    private final static Module PNFSID_SERIALIZER = createPnfsIdSerializer();
    private final ObjectMapper defaultObjectMapper = createDefaultMapper();
    private final ObjectMapper listObjectMapper = createListObjectMapper();

    private static ObjectMapper createListObjectMapper()
    {
        return new ObjectMapper()
                .registerModule(PNFSID_SERIALIZER)
                .disable(SerializationFeature.WRITE_NULL_MAP_VALUES)
                .enable(DeserializationFeature.UNWRAP_ROOT_VALUE)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }

    private static ObjectMapper createDefaultMapper()
    {
        return new ObjectMapper()
                .registerModule(PNFSID_SERIALIZER)
                .enable(SerializationFeature.INDENT_OUTPUT)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private static Module createPnfsIdSerializer()
    {
        Version version = new Version(1, 0, 0,
                                      null, null, null);
        SimpleModule pnfsIdModule = new SimpleModule("PnfsIdModule", version);
        pnfsIdModule.addSerializer(new PnfsidSerializer());
        return pnfsIdModule;
    }

    @Override
    public ObjectMapper getContext(Class<?> type)
    {
        if (type == java.util.ArrayList.class) {
            return listObjectMapper;
        } else {
            return defaultObjectMapper;
        }
    }
}
