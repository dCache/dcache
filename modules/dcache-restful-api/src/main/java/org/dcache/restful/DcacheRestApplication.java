package org.dcache.restful;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.dcache.restful.qos.QosManagement;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.message.filtering.EntityFilteringFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;

import org.dcache.restful.filters.ResponseHeaderFilter;
import org.dcache.restful.providers.ObjectMapperProvider;
import org.dcache.restful.resources.namespace.FileResources;

/**
 *
 */
public class DcacheRestApplication extends ResourceConfig
{
    public DcacheRestApplication()
    {
        packages("org.dcache.restful",
                "org.glassfish.jersey.jackson;",
                "com.fasterxml.jackson.jaxrs.json");

        //register application resources controller
        register(FileResources.class);
        register(QosManagement.class);


        //register filters
        register(ResponseHeaderFilter.class);

        //register features/provider
        register(ObjectMapperProvider.class);
        register(JacksonJsonProvider.class);
        register(JacksonJaxbJsonProvider.class);
        register(EntityFilteringFeature.class);

        /**
         * Jersey framework has a built-in functionality to easily enable content encoding.
         * Uncomment the line below to activate this default built-in functionality
        */
        EncodingFilter.enableFor(this, GZipEncoder.class);

    }
}
