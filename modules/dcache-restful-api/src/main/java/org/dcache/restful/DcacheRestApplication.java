package org.dcache.restful;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.message.filtering.EntityFilteringFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;

import org.dcache.restful.filters.ResponseHeaderFilter;
import org.dcache.restful.providers.ErrorResponseProvider;
import org.dcache.restful.providers.ObjectMapperProvider;
import org.dcache.restful.qos.QosManagement;
import org.dcache.restful.resources.alarms.AlarmsResources;
import org.dcache.restful.resources.billing.BillingResources;
import org.dcache.restful.resources.cells.CellInfoResources;
import org.dcache.restful.resources.namespace.FileResources;
import org.dcache.restful.resources.restores.RestoreResources;
import org.dcache.restful.resources.transfers.TransferResources;

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
        register(TransferResources.class);
        register(BillingResources.class);
        register(CellInfoResources.class);
        register(RestoreResources.class);
        register(AlarmsResources.class);

        //register filters
        register(ResponseHeaderFilter.class);

        //register features/provider
        register(ObjectMapperProvider.class);
        register(JacksonJsonProvider.class);
        register(JacksonJaxbJsonProvider.class);
        register(EntityFilteringFeature.class);
        register(ErrorResponseProvider.class);

        /**
         * Jersey framework has a built-in functionality to easily enable content encoding.
         * Uncomment the line below to activate this default built-in functionality
        */
        EncodingFilter.enableFor(this, GZipEncoder.class);

    }
}
