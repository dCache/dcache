package org.dcache.webdav;

import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;
import io.milton.http.Auth;
import io.milton.http.Filter;
import io.milton.http.FilterChain;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.servlet.ServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.NoSuchElementException;

import org.dcache.auth.Subjects;

/**
 * Simple request logging for WebDAV door. Interim solution until we
 * switch to a better logging framework with direct support for Jetty.
 */
public class LoggingFilter implements Filter
{
    private final Logger _log = LoggerFactory.getLogger(LoggingFilter.class);

    private static final String X509_CERTIFICATE_ATTRIBUTE =
        "javax.servlet.request.X509Certificate";

    @Override
    public void process(final FilterChain filterChain,
                        final Request request,
                        final Response response)
    {
        try {
            filterChain.process(request, response);

            Response.Status status = response.getStatus();
            if (status != null) {
                _log.debug("{} {} {} {} {}",
                          request.getFromAddress(),
                          request.getMethod(),
                          request.getAbsolutePath(),
                          getUser(request),
                          status.code);
            } else {
                _log.debug("{} {} {} {}",
                          request.getFromAddress(),
                          request.getMethod(),
                          request.getAbsolutePath(),
                          getUser(request));
            }
        } catch (RuntimeException e) {
            _log.warn(String.format("%s %s %s %s",
                                    request.getFromAddress(),
                                    request.getMethod(),
                                    request.getAbsolutePath(),
                                    getUser(request)),
                                    e);
            throw e;
        }
    }

     private String getUser(Request request)
     {
         StringBuilder sb = new StringBuilder();

         String certificateName = getCertificateName();
         String subjectName = getSubjectName(request);

         sb.append("[");
         if(certificateName.isEmpty() && subjectName.isEmpty()) {
             sb.append("ANONYMOUS");
         } else {
             sb.append(certificateName);

             if(!certificateName.isEmpty() && !subjectName.isEmpty()) {
                 sb.append("; ");
             }

             sb.append(subjectName);
         }
         sb.append("]");

         return sb.toString();
    }

    private String getCertificateName()
    {
        HttpServletRequest servletRequest = ServletRequest.getRequest();

        Object object =
            servletRequest.getAttribute(X509_CERTIFICATE_ATTRIBUTE);

        if (object instanceof X509Certificate[]) {
            X509Certificate[] chain = (X509Certificate[]) object;

            if (chain.length >= 1) {
                return chain[0].getSubjectX500Principal().getName();
            }
        }

        return "";
    }

    private String getSubjectName(Request request)
    {
        Auth auth = request.getAuthorization();

        if(auth == null) {
            return "";
        }

        Subject subject = (Subject) auth.getTag();

        if(subject == null) {
            return "";
        }

        if(subject.equals(Subjects.NOBODY)) {
            return "NOBODY";
        }

        if(subject.equals(Subjects.ROOT)) {
            return "ROOT";
        }

        String uid;
        try {
            uid = Long.toString(Subjects.getUid(subject));
        } catch(NoSuchElementException e) {
            uid="unknown";
        }

        List<Long> gids = Longs.asList(Subjects.getGids(subject));

        return "uid=" + uid + ", gid={" + Joiner.on(",").join(gids)+"}";
    }
}
