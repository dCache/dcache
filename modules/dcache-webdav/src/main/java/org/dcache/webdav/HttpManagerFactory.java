package org.dcache.webdav;

import com.google.common.collect.ImmutableMap;
import io.milton.config.HttpManagerBuilder;
import io.milton.http.Auth;
import io.milton.http.AuthenticationService;
import io.milton.http.HandlerHelper;
import io.milton.http.HttpManager;
import io.milton.http.Response;
import io.milton.http.Response.Status;
import io.milton.http.http11.DefaultHttp11ResponseHandler;
import io.milton.http.webdav.DefaultWebDavResponseHandler;
import io.milton.http.webdav.PropFindXmlGenerator;
import io.milton.http.webdav.WebDavResponseHandler;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import java.util.Date;

import org.dcache.webdav.federation.FederationResponseHandler;

import static com.google.common.base.Preconditions.checkNotNull;

public class HttpManagerFactory extends HttpManagerBuilder implements FactoryBean
{
    private ReloadableTemplate _template;
    private ImmutableMap<String,String> _templateConfig;
    private String _staticContentPath;
    private PathMapper _pathMapper;

    @Override
    public Object getObject() throws Exception
    {
        DcacheResponseHandler dcacheResponseHandler = new DcacheResponseHandler();
        dcacheResponseHandler.setPathMapper(_pathMapper);
        WebDavResponseHandler handler = new FederationResponseHandler(dcacheResponseHandler);
        setWebdavResponseHandler(handler);

        init();

        // Late initialization of DcacheResponseHandler because AuthenticationService and other collaborators
        // have to be created first.
        dcacheResponseHandler.setAuthenticationService(getAuthenticationService());
        dcacheResponseHandler.setWrapped(
            new DefaultWebDavResponseHandler(getHttp11ResponseHandler(), getResourceTypeHelper(),
                                             getPropFindXmlGenerator()));
        dcacheResponseHandler.setReloadableTemplate(_template);
        dcacheResponseHandler.setTemplateConfig(_templateConfig);
        dcacheResponseHandler.setStaticContentPath(_staticContentPath);
        dcacheResponseHandler.setBuffering(getBuffering());

        return buildHttpManager();
    }

    /* The following hack allows injection of custom objects part way through init */
    @Override
    protected void buildResourceTypeHelper() {
        super.buildResourceTypeHelper();

        if (handlerHelper == null) {
            handlerHelper = new HandlerHelper(authenticationService);
            showLog("handlerHelper", handlerHelper);
        }

        if (propFindXmlGenerator == null) {
            propFindXmlGenerator = new PropFindXmlGenerator(valueWriters);
            showLog("propFindXmlGenerator", propFindXmlGenerator);
        }

        if (http11ResponseHandler == null) {
            DefaultHttp11ResponseHandler rh = createDefaultHttp11ResponseHandler(authenticationService);
            rh.setCacheControlHelper(cacheControlHelper);
            rh.setBuffering(buffering);
            http11ResponseHandler = rh;
            showLog("http11ResponseHandler", http11ResponseHandler);
        }

        if (webdavResponseHandler == null) {
            webdavResponseHandler = new DefaultWebDavResponseHandler(http11ResponseHandler, resourceTypeHelper, propFindXmlGenerator);
        }
        outerWebdavResponseHandler = webdavResponseHandler;

        if (resourceHandlerHelper == null) {
            resourceHandlerHelper = new DcacheResourceHandlerHelper(handlerHelper,
                    urlAdapter, outerWebdavResponseHandler, authenticationService);
            showLog("resourceHandlerHelper", resourceHandlerHelper);
        }
    }

    @Override
    protected DefaultHttp11ResponseHandler createDefaultHttp11ResponseHandler(AuthenticationService authenticationService)
    {
        // Subclass DefaultHttp11ResponseHandler to avoid adding a "Server" response header.
        return new DefaultHttp11ResponseHandler(authenticationService,
                eTagGenerator, contentGenerator) {
            @Override
            protected void setRespondCommonHeaders(Response response,
                    io.milton.resource.Resource resource, Status status, Auth auth)
            {
                response.setStatus(status);
                // The next line is omitted to avoid setting the Server header
                // response.setNonStandardHeader("Server", "milton.io-" + miltonVerson);
                response.setDateHeader(new Date());
                response.setNonStandardHeader("Accept-Ranges", "bytes");
                String etag = eTagGenerator.generateEtag(resource);
                if (etag != null) {
                        response.setEtag(etag);
                }
            }
        };
    }

    @Override
    public Class<?> getObjectType()
    {
        return HttpManager.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }

    @Required
    public void setPathMapper(PathMapper mapper)
    {
        _pathMapper = checkNotNull(mapper);
    }

    /**
     * Sets the resource containing the StringTemplateGroup for
     * directory listing.
     */
    @Required
    public void setTemplate(ReloadableTemplate template)
    {
        _template = template;
    }

    @Required
    public void setTemplateConfig(ImmutableMap<String,String> config)
    {
        _templateConfig = config;
    }

    /**
     * The static content path is the path under which the service
     * exports the static content. This typically contains stylesheets
     * and image files.
     */
    public void setStaticContentPath(String path)
    {
        _staticContentPath = path;
    }
}
