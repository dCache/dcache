package org.dcache.restful;

import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.dcache.restful.interceptors.LoggingInterceptor;
import org.dcache.restful.providers.ErrorResponseProvider;
import org.dcache.restful.providers.ObjectMapperProvider;
import org.dcache.restful.qos.QosManagement;
import org.dcache.restful.resources.EventResources;
import org.dcache.restful.resources.alarms.AlarmsResources;
import org.dcache.restful.resources.billing.BillingResources;
import org.dcache.restful.resources.cells.CellInfoResources;
import org.dcache.restful.resources.doors.DoorsResources;
import org.dcache.restful.resources.namespace.FileResources;
import org.dcache.restful.resources.pool.PoolGroupInfoResources;
import org.dcache.restful.resources.pool.PoolInfoResources;
import org.dcache.restful.resources.restores.RestoreResources;
import org.dcache.restful.resources.selection.LinkResources;
import org.dcache.restful.resources.selection.PartitionResources;
import org.dcache.restful.resources.selection.PoolPreferenceResources;
import org.dcache.restful.resources.selection.UnitResources;
import org.dcache.restful.resources.space.SpaceManagerResources;
import org.dcache.restful.resources.transfers.TransferResources;
import org.dcache.restful.util.RequestUser;
import org.glassfish.jersey.message.filtering.EntityFilteringFeature;
import org.glassfish.jersey.server.ResourceConfig;

public class DcacheRestApplication extends ResourceConfig {

    public DcacheRestApplication() {
        packages("org.dcache.restful",
              "org.glassfish.jersey.jackson");

        //register application resources controller
        register(FileResources.class);
        register(QosManagement.class);
        register(TransferResources.class);
        register(BillingResources.class);
        register(CellInfoResources.class);
        register(RestoreResources.class);
        register(AlarmsResources.class);
        register(PoolInfoResources.class);
        register(PoolGroupInfoResources.class);
        register(LinkResources.class);
        register(UnitResources.class);
        register(PartitionResources.class);
        register(PoolPreferenceResources.class);
        register(SpaceManagerResources.class);
        register(ApiListingResource.class);
        register(SwaggerSerializers.class);
        register(EventResources.class);
        register(DoorsResources.class);

        //register filters
        register(RequestUser.class);

        //register features/provider
        register(ObjectMapperProvider.class);
        register(EntityFilteringFeature.class);
        register(ErrorResponseProvider.class);

        register(LoggingInterceptor.class);

        /*
         * The Jersey support for content-encoding (in particular, compression)
         * is currently broken for SSE.  There is no (easy) work-around, so
         * support for compression must not be enabled until this is resolved.
         * For details, see:
         *
         *     https://github.com/eclipse-ee4j/jersey/issues/3809
         *
         * EncodingFilter.enableFor(this, GZipEncoder.class);
         */
    }
}
