package org.dcache.restful.providers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * Fine-tuning how JSON we be presented by redefine the default Jackson behaviour.
 **/
@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper>
{
    private final ObjectMapper defaultObjectMapper = createDefaultMapper();
    private final ObjectMapper listObjectMapper = createListObjectMapper();

    private static ObjectMapper createListObjectMapper()
    {
        return new ObjectMapper()
                .disable(SerializationFeature.WRITE_NULL_MAP_VALUES)
                .enable(DeserializationFeature.UNWRAP_ROOT_VALUE)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }

    private static ObjectMapper createDefaultMapper()
    {
        return new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
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
