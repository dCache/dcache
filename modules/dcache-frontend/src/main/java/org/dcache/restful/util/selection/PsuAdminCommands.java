package org.dcache.restful.util.selection;

import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandThrowableException;
import dmg.util.Exceptions;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolManagerHandlerSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

public class PsuAdminCommands {

    private static final Logger LOGGER = LoggerFactory.getLogger(PsuAdminCommands.class);

    private CellStub poolManagerStub;
    private PoolManagerHandlerSubscriber subscriber;

    @Required
    public void setPoolManagerStub(CellStub poolManagerStub) {
        this.poolManagerStub = poolManagerStub;
    }

    @Required
    public void setSubscriber(PoolManagerHandlerSubscriber subscriber) {
        this.subscriber = subscriber;
    }

    /**
     * Sends the commands in order to one PoolManager instance, then runs 'save' there.
     *
     * @param action description of the change, used in the error message if saving fails
     * @param status status of the successful response
     */
    public Response execute(String action, Response.Status status, String... commands) {
        CellPath path = adminBackend();
        for (String command : commands) {
            send(path, command);
        }
        return saveAndRespond(path, action, status);
    }

    public CellPath adminBackend() {
        try {
            return new CellPath(subscriber.adminBackend().get());
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }

    public String send(CellPath path, String command) {
        try {
            return poolManagerStub.sendAndWait(path, command, String.class);
        } catch (CacheException e) {
            throwIfClientError(e);
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }

    public Response saveAndRespond(CellPath path, String action, Response.Status status) {
        try {
            poolManagerStub.sendAndWait(path, "save", String.class);
        } catch (CacheException | NoRouteToCellException | InterruptedException e) {
            LOGGER.warn("{} but PoolManager configuration could not be saved: {}",
                    action, Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(
                    action + " but configuration could not be saved");
        }
        return successfulResponse(status);
    }

    private static void throwIfClientError(CacheException e) {
        if (!(e.getCause() instanceof CommandThrowableException)) {
            return;
        }
        Throwable target = ((CommandThrowableException) e.getCause()).getTargetException();
        if (!(target instanceof IllegalArgumentException)) {
            return;
        }
        String message = String.valueOf(target.getMessage());
        if (message.contains("Not found") || message.contains("not found")
                || message.contains("not member of") || message.contains("not a subgroup of")) {
            throw new NotFoundException(message);
        }
        throw new ClientErrorException(Response.Status.CONFLICT);
    }
}
