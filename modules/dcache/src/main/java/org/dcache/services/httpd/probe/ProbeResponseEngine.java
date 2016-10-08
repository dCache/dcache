package org.dcache.services.httpd.probe;

import com.google.common.base.Charsets;
import com.google.gson.GsonBuilder;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.TimeoutCacheException;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;
import org.dcache.cells.CellStub;
import org.dcache.vehicles.BeanQueryAllPropertiesMessage;
import org.dcache.vehicles.BeanQueryMessage;
import org.dcache.vehicles.BeanQuerySinglePropertyMessage;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * Provides a simple interface to query bean properties of
 * UniversalSpringCells.
 */
public class ProbeResponseEngine implements HttpResponseEngine, CellMessageSender
{
    private CellStub stub;

    public ProbeResponseEngine(String[] someArgs)
    {
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        stub =  new CellStub(endpoint);
    }

    @Override
    public void queryUrl(HttpRequest request) throws HttpException
    {
        try {
            String[] urlItems = request.getRequestTokens();
            if (urlItems.length < 2) {
                throw new HttpException(404, "No such property");
            }

            /* urlItems[0] is the mount point.
             */
            BeanQueryMessage queryMessage = (urlItems.length == 3) ?
                    new BeanQuerySinglePropertyMessage(urlItems[2]) :
                    new BeanQueryAllPropertiesMessage();
            BeanQueryMessage queryReply = stub.sendAndWait(new CellPath(urlItems[1]), queryMessage);

            request.setContentType("application/json; charset=utf-8");
            Writer writer = new OutputStreamWriter(request.getOutputStream(), Charsets.UTF_8);
            writer.append(new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().disableHtmlEscaping().create().toJson(queryReply
                    .getResult()));
            writer.flush();
        } catch (NoRouteToCellException e) {
            throw new HttpException(503, "The cell was unreachable, suspect trouble.");
        } catch (TimeoutCacheException e) {
            throw new HttpException(503, "The cell took too long to reply, suspect trouble.");
        } catch (InvalidMessageCacheException e) {
            throw new HttpException(404, "No such property");
        } catch (CacheException | IOException e) {
            throw new HttpException(500, e.getMessage());
        } catch (InterruptedException e) {
            throw new HttpException(503, "Received interrupt whilst processing data. Please try again later.");
        }
    }

    @Override
    public void startup()
    {

    }

    @Override
    public void shutdown()
    {

    }
}
