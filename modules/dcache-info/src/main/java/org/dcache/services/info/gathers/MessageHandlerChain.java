package org.dcache.services.info.gathers;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.cells.nucleus.UOID;

import dmg.cells.nucleus.CellMessageSender;


/**
 * A MessageHandlerChain allows multiple MessageHandler subclass instances to attempt to
 * process an incoming Message.  This allows easy addition of extra monitoring by receiving
 * additional messages.
 *
 * Zero or more MessageHandler subclass instances are registered with the MessageHandlerChain.
 * When passed an incoming Message, the MessageHandlerChain instance will pass the Message to
 * each MessageHandler subclass instance in turn until one succeeds in processing the Message.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class MessageHandlerChain implements MessageMetadataRepository<UOID>,
        MessageSender, CellMessageAnswerable, CellMessageSender
{

	/** The period between successive flushes of ancient metadata, in milliseconds */
	private static final long METADATA_FLUSH_THRESHOLD = 3600000; // 1 hour
	private static final long METADATA_FLUSH_PERIOD = 600000; // 10 minutes

	/** Our default timeout for sending messages, in milliseconds */
	private static final long STANDARD_TIMEOUT = 1000;



	private static final Logger _log = LoggerFactory.getLogger( MessageHandlerChain.class);

        private final List<MessageHandler> _messageHandler = new LinkedList<>();

	private CellEndpoint _endpoint;

        @Override
        public void setCellEndpoint(CellEndpoint endpoint)
        {
            _endpoint = endpoint;
        }

	/**
	 * @return a simple array of registered MessageHandlers subclass types.
	 */
	public String[] listMessageHandlers()
        {
            int i=0;
            String[] msgHandlers;

            synchronized (_messageHandler) {
                msgHandlers = new String[_messageHandler.size()];

                for( MessageHandler mh : _messageHandler) {
                    // We're assuming only one instance per Class
                    msgHandlers[i++] = mh.getClass().getSimpleName();
                }
            }

            return msgHandlers;
	}


	/**
	 * Common method to send a CellMessage and register a handler for the return message.
	 * This is deprecated against using Vehicles and registering MessageHandlers.
     * @param ttl lifetime of resulting metric, in seconds.
     * @param handler the call-back handler for the return message
	 * @param path the CellPath to target cell
	 * @param requestString the String, requesting information
	 */
	@Override
    public void sendMessage( long ttl, CellMessageAnswerable handler, CellPath path, String requestString) {

		if( handler == null) {
			_log.error( "ignoring attempt to send string-based message without call-back");
			return;
		}

		CellMessage envelope = new CellMessage( path, requestString);
		sendMessage( ttl, handler, envelope);
	}


	/**
	 * The preferred way of sending requests for information.
     * @param ttl lifetime of resulting metric, in seconds.
	 * @param path the CellPath for the recipient of this message
	 * @param message the Message payload
	 */
	@Override
    public void sendMessage( long ttl, CellPath path, Message message) {
		CellMessage envelope = new CellMessage( path, message);
		sendMessage( ttl, null, envelope);
	}


	/**
	 * Send a message envelope and record metadata against it.
     * @param ttl the metadata for the message
     * @param handler the call-back for this method, or null if none should be used.
	 * @param envelope the message to send
	 * @throws SerializationException if the payload isn't serialisable.
	 */
	@Override
    public void sendMessage( long ttl, CellMessageAnswerable handler, CellMessage envelope) throws SerializationException {
        putMetricTTL( envelope.getUOID(), ttl);
        _endpoint.sendMessage( envelope, handler != null ? handler : this, MoreExecutors.sameThreadExecutor(), STANDARD_TIMEOUT);
	}


    public void setHandlers(List<MessageHandler> handlers)
    {
        synchronized (_messageHandler) {
            _messageHandler.clear();
            _messageHandler.addAll(handlers);
        }
    }


	/**
	 *  SUPPORT FOR MessageMetadataRepository INTERFACE
	 */

    /**
     * For each message we send, a small amount of metadata is recorded (when it was sent and a long).
     * The long is so, when the return message is received, we can pass this parameter on
     * to the message processing plug-in.  The time is so we can (every so often) delete stale entries
     * due to message-loss.
     */
    private static class MessageMetadata {
        Date _timeSent;
        final long _ttl;
        MessageMetadata( long ttl) {
            _timeSent = new Date();
            _ttl = ttl;
        }
    }

    private final Map<UOID,MessageMetadata> _msgMetadata = new ConcurrentHashMap<>();
    private Date _nextFlushOldMetadata;

    @Override
    public boolean containsMetricTTL( UOID messageId) {
        return _msgMetadata.containsKey( messageId);
    }

    @Override
    public long getMetricTTL( UOID messageId)
    {
        flushOldMetadata();

        _log.debug("Querying for metric ttl stored against message-ID {}",
                messageId);

        MessageMetadata metadata = _msgMetadata.get(messageId);

        if (metadata == null) {
            throw new IllegalArgumentException("No metadata recorded for " +
                    "message " + messageId);
        }

        return metadata._ttl;
    }

    @Override
    public void remove( UOID messageId)
    {
        if (_msgMetadata.remove(messageId) == null) {
            throw new IllegalArgumentException("No metadata recorded for " +
                    "message " + messageId);
        }
    }


    @Override
    public void putMetricTTL( UOID messageId, long ttl) {
        if( messageId == null) {
            throw new NullPointerException("Attempting to record ttl against null messageId");
        }

        if( _log.isDebugEnabled()) {
            _log.debug("Adding metric ttl " + ttl + " against message-ID " + messageId);
        }

        _msgMetadata.put( messageId, new MessageMetadata( ttl));
    }


    /**
     * Scan through our recorded Metadata and remove very old entries.
     * This is only done "every so often" and adds some safety against
     * lost packets resulting in accumulated memory usage.
     */
    private void flushOldMetadata() {

        Date now = new Date();

        if( _nextFlushOldMetadata != null && now.before( _nextFlushOldMetadata)) {
            return;
        }

        // Flush ancient metadata
        for( Iterator<MessageMetadata> itr = _msgMetadata.values().iterator(); itr.hasNext();) {
            MessageMetadata item = itr.next();

            if( now.getTime() - item._timeSent.getTime() > METADATA_FLUSH_THRESHOLD) {
                itr.remove();
            }
        }

        _nextFlushOldMetadata = new Date( System.currentTimeMillis() + METADATA_FLUSH_PERIOD);
    }


    /*
     * The following three methods implement CellMessageAnswerable interface for
     * Message methods.
     */

    @Override
    public void answerArrived(CellMessage request, CellMessage answer) {
        Object messagePayload = answer.getMessageObject();

        if( !(messagePayload instanceof Message)) {
            _log.debug( "Received msg where payload is not instanceof Message");
            return;
        }

        if( !containsMetricTTL( request.getLastUOID())) {
            _log.error( "Attempt to add metrics without recorded metric TTL for msg " + request);
            return;
        }

        synchronized (_messageHandler) {
            for( MessageHandler mh : _messageHandler) {
                if (mh.handleMessage((Message) messagePayload,
                        getMetricTTL(request.getLastUOID()))) {
                    return;
                }
            }
        }
    }

    @Override
    public void answerTimedOut(CellMessage request) {
        remove( request.getLastUOID());
        _log.info("Message timed out");
    }

    @Override
    public void exceptionArrived(CellMessage request, Exception exception) {
        remove( request.getLastUOID());
        if( exception instanceof NoRouteToCellException) {
            // This can happen after a cell dies and info hasn't caught up
            _log.debug( "Sending message to {} failed: {}",
                    ((NoRouteToCellException)exception).getDestinationPath(),
                    exception.getMessage());
        } else if ( exception instanceof IllegalArgumentException) {
            // Can happen for a short while when a poolgroup is deleted
            _log.debug( "Command failed: {}", exception.getMessage());
        } else {
            _log.error( "Received remote exception: ", exception);
        }
    }
}
