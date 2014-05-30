package org.dcache.webdav;


import com.google.common.io.ByteStreams;
import io.milton.http.Auth;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.resource.GetableResource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author leonie
 */
public class OwnCloudResource
    implements GetableResource
{
    private static final String _statusphpContent = "{\"installed\":\"true\",\"version\":\"6.0.2.2\",\"versionstring\":\"6.0.2\",\"edition\":\"\"}";

    @Override
    public Object authenticate(String user, String password)
    {
        return user;
    }

    @Override
    public boolean authorise(Request request, Request.Method method, Auth auth)
    {
        return true;
    }

    @Override
    public Date getModifiedDate()
    {
        return new Date();
    }

    @Override
    public String getRealm()
    {
        return "dCache";
    }

    @Override
    public String getName()
    {
        return "status.php";
    }

    @Override
    public String getUniqueId()
    {
        return "ownCloudStatusPhp";
    }

    @Override
    public Long getMaxAgeSeconds(Auth auth)
    {
        return null;
    }

    @Override
    public String getContentType(String accepts)
    {
        return "text/html";
    }

    @Override
    public Long getContentLength()
    {
        return (long) _statusphpContent.length();
    }

    @Override
    public String checkRedirect(Request request)
    {
        return null;
    }

    @Override
    public void sendContent(OutputStream out, Range range, Map<String,String> params, String contentType)
        throws IOException, NotAuthorizedException
    {
        InputStream inputStream = new ByteArrayInputStream(_statusphpContent.getBytes());

        ByteStreams.copy(inputStream, out);
        out.flush();
    }
}
