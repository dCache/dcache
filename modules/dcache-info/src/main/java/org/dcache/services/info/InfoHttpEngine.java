package org.dcache.services.info;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterables.find;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.services.info.serialisation.JsonSerialiser;
import org.dcache.services.info.serialisation.PrettyPrintTextSerialiser;
import org.dcache.services.info.serialisation.XmlSerialiser;
import org.dcache.util.Args;
import org.dcache.vehicles.InfoGetSerialisedDataMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides support for querying the info cell via the admin web-interface.  It
 * implements the HttpResponseEngine to handle requests at a particular point (a specific alias).
 * <p>
 * Users may query the complete tree, or select a subtree by specifying the path.
 * <p>
 * It supports several serialisers from which the user may chose, either by specifying  the query
 * parameter 'format', by specifying the HTTP Accept header.  XML is the default if neither
 * indicates which serialiser to use.
 * <p>
 * The implementation caches serialised data for one second.  This is a safety feature to reducing
 * the impact on info of pathologically broken clients that make many requests per second.
 */
public class InfoHttpEngine implements HttpResponseEngine, CellMessageSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpResponseEngine.class);

    private static final List<String> ENTIRE_TREE = new ArrayList<>();

    private final SerialisationHandler xmlSerialiser =
          new SerialisationHandler(XmlSerialiser.NAME, "text/xml");

    private final SerialisationHandler jsonSerialiser =
          new SerialisationHandler(JsonSerialiser.NAME, "text/json");

    private final SerialisationHandler prettyPrintSerialiser =
          new SerialisationHandler(PrettyPrintTextSerialiser.NAME, "text/x-ascii-art");

    private final Map<String, SerialisationHandler> mimetypeToSerialiser =
          ImmutableMap.<String, SerialisationHandler>builder().
                put("application/xml", xmlSerialiser).
                put("text/xml", xmlSerialiser).
                put("application/json", jsonSerialiser).
                put("text/x-ascii-art", prettyPrintSerialiser).
                build();

    private final Map<String, SerialisationHandler> queryParameterToSerialiser =
          ImmutableMap.<String, SerialisationHandler>builder().
                put("xml", xmlSerialiser).
                put("json", jsonSerialiser).
                put("pretty", prettyPrintSerialiser).
                build();
    private final String _infoCellName;

    private CellStub _info;

    /**
     * httpd-side class for each info-side serialiser.
     */
    private class SerialisationHandler {

        private final String _name;
        private final String _mimeType;

        LoadingCache<List<String>, String> resultCache = CacheBuilder.newBuilder()
              .maximumSize(10)
              .expireAfterWrite(1, TimeUnit.SECONDS)
              .build(new CacheLoader<List<String>, String>() {
                  @Override
                  public String load(List<String> path)
                        throws InterruptedException, CacheException, NoRouteToCellException {
                      InfoGetSerialisedDataMessage message =
                            (path == ENTIRE_TREE) ? new InfoGetSerialisedDataMessage(_name)
                                  : new InfoGetSerialisedDataMessage(path, _name);
                      message = _info.sendAndWait(message);
                      return message.getSerialisedData();
                  }
              });

        public SerialisationHandler(String name, String mimeType) {
            _name = name;
            _mimeType = mimeType;
        }

        public void handleRequest(HttpRequest request) throws HttpException {
            String[] urlItems = request.getRequestTokens();
            OutputStream out = request.getOutputStream();

            List<String> path = urlItems.length == 1 ? ENTIRE_TREE :
                  Arrays.asList(urlItems).subList(1, urlItems.length);

            try {
                byte[] raw = resultCache.get(path).getBytes(UTF_8);
                request.printHttpHeader(raw.length);
                request.setContentType(this._mimeType);
                out.write(raw);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TimeoutCacheException) {
                    throw new HttpException(503, "The info cell took too " +
                          "long to reply, suspect trouble (" +
                          cause.getMessage() + ")");
                }
                if (cause instanceof NoRouteToCellException) {
                    throw new HttpException(503, "Unable to locate the info cell");
                }
                if (cause instanceof CacheException) {
                    throw new HttpException(500, "Error when requesting " +
                          "info from info cell. (" + cause.getMessage() + ")");
                }
                if (cause instanceof InterruptedException) {
                    throw new HttpException(503, "Received interrupt " +
                          "whilst processing data. Please try again later.");
                }
                throwIfUnchecked(cause);
                throw new RuntimeException(cause);
            } catch (IOException e) {
                LOGGER.error("Failed to send response: {}", e.getMessage());
            }
        }
    }


    /**
     * The constructor simply creates a new nucleus for us to use when sending messages.
     */
    public InfoHttpEngine(String[] args) {
        _infoCellName = new Args(args).getOption("cell");
        checkArgument(_infoCellName != null,
              "-cell option is required for InfoHttpEngine handler.");
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _info = new CellStub(endpoint, new CellPath(_infoCellName), 4000, MILLISECONDS);
    }

    /**
     * Handle a request for data.  This either returns the cached contents (if still valid), or
     * queries the info cell for information.
     */
    @Override
    public void queryUrl(HttpRequest request) throws HttpException {
        LOGGER.info("Received request: {}", request);

        SerialisationHandler handler = find(asList(
              serialiserFromUri(request),
              serialiserFromHttpHeaders(request),
              xmlSerialiser), notNull());

        handler.handleRequest(request);
    }

    private SerialisationHandler serialiserFromUri(HttpRequest request) throws HttpException {
        SerialisationHandler serialiser = null;

        String argument = request.getParameter("format");
        if (argument != null) {
            serialiser = queryParameterToSerialiser.get(argument);
            if (serialiser == null) {
                throw new HttpException(415, "specified format does not exist");
            }
        }

        return serialiser;
    }

    private SerialisationHandler serialiserFromHttpHeaders(HttpRequest request) {
        String accept = request.getRequestAttributes().get("Accept");
        if (accept == null) {
            return null;
        }

        SerialisationHandler bestHandler = null;

        /*
         * Choose the best mime-type that the client will accept, taking
         * into account which formats we support, the client's preferences
         * (q values) and choosing the most specific (i.e. longest) mime-type.
         * Here is an example value (should be one line)
         *
         *     application/xml;q=0.5,application/json;q=0.8,
         *     application/x-proprietary-format
         *
         * For details, see
         *
         *     http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
         */
        double bestQ = 0;
        String bestEntry = "";
        for (String entry : Splitter.on(',').trimResults().split(accept)) {
            List<String> items = Splitter.on(';').trimResults().splitToList(entry);
            String mimeType = items.get(0);
            List<String> args = items.subList(1, items.size());

            StringBuilder sb = new StringBuilder().append(mimeType);
            double q = 1;
            for (String arg : args) {
                if (arg.startsWith("q=")) {
                    try {
                        q = Double.parseDouble(arg.substring(2));
                    } catch (NumberFormatException e) {
                        LOGGER.debug("malformed q value ('{}') in Accept: {}", q, e.toString());
                        q = 0;
                    }
                } else {
                    sb.append(';').append(arg);
                }
            }
            String entryWithoutQ = sb.toString();

            // REVISIT: no wildcard support for mimetypes; e.g. text/* or */*
            SerialisationHandler handler = mimetypeToSerialiser.get(mimeType);

            if (q >= bestQ && entryWithoutQ.length() > bestEntry.length() && handler != null) {
                bestHandler = handler;
                bestQ = q;
                bestEntry = entryWithoutQ;
            }
        }

        return bestHandler;
    }
}
