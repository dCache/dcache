package org.dcache.webapi;

import org.dcache.util.Version;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/*
* This class registers itself as a get resource via the @GET annotation.
* The @Path("/") and @Path("/info") annotations identify the URI path for which the Info class will serve requests.
* The @Produces annotation indicates that the getIt() method returns JSON object.
* The base URL is based on your application name, the servlet and the URL pattern from
* is defined the webdav.xml configuration file (where you need to register Jersey as the
* servlet dispatcher for REST requests).
*
* In this file you need to specifies your ServeletContainer and the package containing the Info.class:
 <bean class="org.glassfish.jersey.servlet.ServletContainer">
  <constructor-arg name="resourceConfig">
   <bean class="org.glassfish.jersey.server.ResourceConfig">
    <property name="properties">
       <map>
         <entry key="jersey.config.server.provider.packages"
              value="org.dcache.webapi"/>
       </map>
     /property>
   </bean>
  </constructor-arg>
 </bean>.

* The ServletContainer itself is added to ServletContextHandler.
 <bean class="org.eclipse.jetty.servlet.ServletContextHandler">
    <property name="contextPath" value="/api"/>
* Context path sepcifies the path to access the service:
http://localhost:2880/api/v1/info.
v1 is defined in the  servletMappings property of ServletContextHandler:
<property name="servletMappings">
   <list>
       <bean class="org.eclipse.jetty.servlet.ServletMapping">
               property name="servletName" value="rest"/>
                    <property name="pathSpecs">
                        <list>
                           <value>/v1/*</value>
                        </list>
                    </property>
        </bean>
    </list>
</property>.
* If you need to specify the ServletContext, e.g when you need to use CellStub class
* to get file attributes info,  the attributes property should be added to
* ServletContextHandler:
 <property name="attributes">
        <bean class="org.dcache.util.jetty.ImmutableAttributesMap">
            <constructor-arg>
                <map>
                    <entry key="#{ T(org.dcache.webapi.RestResponce).FS}"
                           value-ref="pnfs-stub"/>
                </map>
            </constructor-arg>
        </bean>
    </property>.
*
* The corresponding method in the RestResponce.class looks like this:
    .
    .
    .
    @Context
    ServletContext ctx;

    public final static String FS = "org.dcache.webapi";
    .
    .
    @GET
    @Path("/stats/{fileid}")
    @Produces("text/plain")
    public FileAttributes getFileStatus(@PathParam("fileid") String fileid) throws IOException, CacheException,
                                                                    ClassNotFoundException, IllegalAccessException,
                                                                    InstantiationException {
        CellStub nameCellStub = (CellStub) (ctx.getAttribute(FS));
        PnfsHandler pnfs = new PnfsHandler(nameCellStub);
        Set<FileAttribute> attrs = EnumSet.of(
                FileAttribute.MODE,
                FileAttribute.OWNER,
                FileAttribute.OWNER_GROUP,
                FileAttribute.PNFSID,
                FileAttribute.TYPE,
                FileAttribute.MODIFICATION_TIME,
                FileAttribute.CREATION_TIME);
        FileAttributes fa = pnfs.getFileAttributes(new PnfsId(fileid), attrs);
        return fa;
    }
* the url path to access the service is
* http://localhost:2880/api/v1/stats/000000000000000000000000000000000000.
* */
@Path("/")
public class Info {

    /* @Path("/info")  will map the resource to the URL "/info".
    * @Produces(MediaType.APPLICATION_JSON) will return the Json object
    * containing the following information about Dcache version:
    * {
    *     "version": "2.15.0-SNAPSHOT",
    *      "build": "3e9c052+dirty+sahakya",
    *      "buildTime": "2015-12-07T17:16:50Z",
    *      "branch": "Restfull/jersey"
    *    }
    * */

    @GET
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public Version getDcahceVersion() {
        return Version.of(Info.class);
    }

}
