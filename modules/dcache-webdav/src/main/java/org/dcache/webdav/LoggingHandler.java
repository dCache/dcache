package org.dcache.webdav;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.dcache.http.AbstractLoggingHandler;
import org.dcache.util.NetLoggerBuilder;
import org.dcache.webdav.macaroons.MacaroonRequestHandler;
import org.dcache.webdav.transfer.CopyFilter;

import static org.dcache.webdav.DcacheResourceFactory.TRANSACTION_ATTRIBUTE;
import static org.dcache.util.NetLoggerBuilder.Level.WARN;

/**
 * WebDAV door specific logging.
 */
public class LoggingHandler extends AbstractLoggingHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger("org.dcache.access.webdav");

    private static String getTransaction(HttpServletRequest request)
    {
        Object object = request.getAttribute(TRANSACTION_ATTRIBUTE);
        return object == null ? null : String.valueOf(object);
    }

    @Override
    protected Logger accessLogger()
    {
        return LOGGER;
    }

    @Override
    protected String requestEventName()
    {
        return "org.dcache.webdav.request";
    }

    @Override
    protected NetLoggerBuilder.Level logLevel(HttpServletRequest request,
            HttpServletResponse response)
    {
        NetLoggerBuilder.Level level = super.logLevel(request, response);

        if (level.ordinal() > WARN.ordinal() && CopyFilter.getTpcError(request) != null) {
            level = WARN;
        }

        return level;
    }

    @Override
    protected void describeOperation(NetLoggerBuilder log,
            HttpServletRequest request, HttpServletResponse response)
    {
        super.describeOperation(log, request, response);

        log.add("transaction", getTransaction(request));
        log.add("request.macaroon-request", MacaroonRequestHandler.getMacaroonRequest(request));
        log.add("response.macaroon-id", MacaroonRequestHandler.getMacaroonId(request));
        log.add("tpc.credential", CopyFilter.getTpcCredential(request));
        log.add("tpc.error", CopyFilter.getTpcError(request));
        log.add("tpc.require-checksum", CopyFilter.getTpcRequireChecksumVerification(request));
        log.add("tpc.source", CopyFilter.getTpcSource(request));
        log.add("tpc.destination", CopyFilter.getTpcDestination(request));
    }
}
