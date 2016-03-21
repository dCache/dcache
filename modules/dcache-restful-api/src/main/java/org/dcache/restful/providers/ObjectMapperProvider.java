package org.dcache.restful.providers;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;


/**
 * Fine-tuning how JSON we be presented by redefine the default Jackson behaviour.
 **/


@Provider
@Produces(MediaType.APPLICATION_JSON)
public class ObjectMapperProvider implements ContextResolver<ObjectMapper>
{
    private final ObjectMapper defaultObjectMapper;
    private final ObjectMapper combinedObjectMapper;

    public ObjectMapperProvider()
    {
        defaultObjectMapper = createDefaultMapper();
        combinedObjectMapper = createCombinedObjectMapper();
    }

    private static ObjectMapper createCombinedObjectMapper()
    {
        return new ObjectMapper()
                .configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false)
                .configure(SerializationFeature.WRAP_ROOT_VALUE, true)
                .configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT)

                .setAnnotationIntrospector(createJaxbJacksonAnnotationIntrospector());
    }

    private static ObjectMapper createDefaultMapper()
    {
        final ObjectMapper result = new ObjectMapper();
        result.enable(SerializationFeature.INDENT_OUTPUT);
        result.enable(SerializationFeature.WRITE_NULL_MAP_VALUES);
        result.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return result;
    }

    private static AnnotationIntrospector createJaxbJacksonAnnotationIntrospector()
    {
        final AnnotationIntrospector jaxbIntrospector = new JaxbAnnotationIntrospector(TypeFactory.defaultInstance());
        final AnnotationIntrospector jacksonIntrospector = new JacksonAnnotationIntrospector();

        return AnnotationIntrospector.pair(jacksonIntrospector, jaxbIntrospector);
    }

    @Override
    public ObjectMapper getContext(Class<?> type)
    {
        if (type == java.util.ArrayList.class) {
            return combinedObjectMapper;
        } else {
            return defaultObjectMapper;
        }
    }
}
