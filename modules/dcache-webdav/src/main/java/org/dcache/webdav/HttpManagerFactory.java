package org.dcache.webdav;

import io.milton.config.HttpManagerBuilder;
import io.milton.http.CompressingResponseHandler;
import io.milton.http.HttpManager;
import io.milton.http.http11.DefaultHttp11ResponseHandler;
import io.milton.http.http11.auth.LoginResponseHandler;
import io.milton.http.webdav.DefaultWebDavResponseHandler;
import io.milton.http.webdav.PropFindXmlGenerator;
import io.milton.http.webdav.WebDavResponseHandler;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.io.Resource;

import java.io.IOException;

public class HttpManagerFactory extends HttpManagerBuilder implements FactoryBean
{
    private Resource _templateResource;
    private String _staticContentPath;

    @Override
    public Object getObject() throws Exception
    {
        DcacheResponseHandler dcacheResponseHandler = new DcacheResponseHandler();

        // A number of collaborators must be created and injected manually because injecting a custom
        // WebDavResponseHandler suppresses the creation of the collaborators too. This essentially duplicates
        // similar code in HttpManagerBuilder.
        WebDavResponseHandler webdavResponseHandler = dcacheResponseHandler;
        if (isEnableCompression()) {
            webdavResponseHandler = new CompressingResponseHandler(webdavResponseHandler);
        }
        if (isEnableFormAuth()) {
            if (getLoginResponseHandler() == null) {
                LoginResponseHandler loginResponseHandler =
                    new LoginResponseHandler(webdavResponseHandler,
                                             getMainResourceFactory(),
                                             getLoginPageTypeHandler());
                loginResponseHandler.setExcludePaths(getLoginPageExcludePaths());
                loginResponseHandler.setLoginPage(getLoginPage());
                webdavResponseHandler = loginResponseHandler;
            }
        }
        setWebdavResponseHandler(webdavResponseHandler);

        if (getPropFindXmlGenerator() == null) {
            setPropFindXmlGenerator(new PropFindXmlGenerator(getValueWriters()));
        }

        init();

        // Cannot create the following collaborators until init was called because init creates
        // the AuthenticationService.
        if (getHttp11ResponseHandler() == null) {
            DefaultHttp11ResponseHandler rh = new DefaultHttp11ResponseHandler(getAuthenticationService(),
                                                                               geteTagGenerator());
            rh.setContentGenerator(getContentGenerator());
            rh.setCacheControlHelper(getCacheControlHelper());
            setHttp11ResponseHandler(rh);
        }

        // Late initialization of DcacheResponseHandler because AuthenticationService and other collaborators
        // have to be created first.
        dcacheResponseHandler.setAuthenticationService(getAuthenticationService());
        dcacheResponseHandler.setWrapped(
            new DefaultWebDavResponseHandler(getHttp11ResponseHandler(), getResourceTypeHelper(),
                                             getPropFindXmlGenerator()));
        dcacheResponseHandler.setTemplateResource(_templateResource);
        dcacheResponseHandler.setStaticContentPath(_staticContentPath);
        dcacheResponseHandler.setBuffering(getBuffering());

        return buildHttpManager();
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

    /**
     * Sets the resource containing the StringTemplateGroup for
     * directory listing.
     */
    public void setTemplateResource(org.springframework.core.io.Resource resource)
        throws IOException
    {
        _templateResource = resource;
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
