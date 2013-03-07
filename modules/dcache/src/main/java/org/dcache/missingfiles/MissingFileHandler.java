package org.dcache.missingfiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import dmg.cells.nucleus.Reply;

import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.MessageReply;
import org.dcache.missingfiles.plugins.Plugin;
import org.dcache.missingfiles.plugins.PluginChain;
import org.dcache.missingfiles.plugins.PluginVisitor;
import org.dcache.missingfiles.plugins.Result;


/**
 * Main entry point for missing file notification
 */
public class MissingFileHandler implements CellMessageReceiver
{
    private static final Logger _log =
            LoggerFactory.getLogger(RemoteMissingFileStrategy.class);

    private ExecutorService _executor;

    private PluginChain _chain;

    public Reply messageArrived(MissingFileMessage message)
    {
        _log.trace("Received notice {} {}", message.getRequestedPath(),
                message.getInternalPath());

        MessageReply<MissingFileMessage> reply =
                new MessageReply<>();

        Request request = new Request(message, reply);
        _executor.submit(request);

        return reply;
    }


    @Required
    public void setPluginChain(PluginChain chain)
    {
        _chain = chain;
    }

    @Required
    public void setExecutorService(ExecutorService service)
    {
        _executor = service;
    }


    private static Action actionFor(Result result)
    {
        switch(result) {
            case FAIL:
                return Action.FAIL;
            case RETRY:
                return Action.RETRY;
            default:
                throw new IllegalArgumentException("No Action for Result." +
                        result);
        }
    }

    private static boolean isTerminalResult(Result result)
    {
        switch(result) {
            case FAIL:
            case RETRY:
                return true;
            default:
                return false;
        }
    }

    /**
     * Class that handles an individual request.  This decouples the action
     * of processing a request from the message-processing thread.
     */
    public class Request implements PluginVisitor, Runnable
    {
        private final MissingFileMessage _msg;
        private final MessageReply<MissingFileMessage> _reply;
        private final String _id = UUID.randomUUID().toString();

        public Request(MissingFileMessage msg, MessageReply<MissingFileMessage> reply)
        {
            _msg = msg;
            _reply = reply;
        }

        public String getId()
        {
            return _id;
        }

        @Override
        public void run()
        {
            _chain.accept(this);

            // dropped off the end of the chain, so fail the request
            replyWith(Action.FAIL);
        }


        @Override
        public boolean visit(Plugin plugin)
        {
            Future<Result> future = plugin.accept(_msg.getSubject(),
                    _msg.getRequestedPath(), _msg.getInternalPath());

            Result result;

            try {
                result = future.get();
            } catch (CancellationException e) {
                _log.trace("Operation was cancelled");
                return false;
            } catch (InterruptedException e) {
                _log.trace("Interrupted while waiting for plugin result");
                return false;
            } catch (ExecutionException e) {
                Throwable t = e.getCause();
                _log.error("Plugin bug: " + t.getMessage(),
                        t);
                return true;
            }

            if(isTerminalResult(result)) {
                Action action = actionFor(result);
                replyWith(action);
                return false;
            }

            return true;
        }


        private void replyWith(Action action)
        {
            _msg.setAction(action);
            _reply.reply(_msg);
        }
    }
}
