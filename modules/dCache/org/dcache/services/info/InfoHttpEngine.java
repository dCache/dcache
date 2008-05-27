package org.dcache.services.info;

import java.io.OutputStream;
import java.util.Date;
import java.io.NotSerializableException;
import java.io.IOException;
import java.util.concurrent.TimeoutException; // We hijack this exception.

import org.apache.log4j.Logger;

import org.dcache.vehicles.InfoGetSerialisedDataMessage;

import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.MessageEvent;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;

/**
 * This class provides support for querying the info cell via the admin web-interface.  It
 * implements the HttpResponseEngine to handle requests at a particular point (a specific alias).
 * <p>
 * Currently it provides only a complete XML dump of the info cell.  If the info cell cannot
 * be contacted or takes too long to reply, or there was a problem when (de-)serialising the
 * Message then an appropriate HTTP status code (50x server-side error) is generated.
 * <p>
 * It is anticipated that clients query this information (roughly) once per minute.
 * <p>
 * The implementation caches the XML data obtained from the info cell for one second.  This
 * is a safety feature, reducing the impact on the info cell of pathologically broken clients
 * that make many requests per second.
 * <p>
 * Future versions may include additional functionality, such as server-side transformation of
 * the XML data into another format, or selecting only part of the available XML tree.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class InfoHttpEngine implements HttpResponseEngine, Cell {

	private static Logger _log = Logger.getLogger( HttpResponseEngine.class);

	private static final String INFO_CELL_NAME = "info";
	
	/** The maximum age of our cache, in milliseconds */
	private static final long MAX_CACHE_AGE = 1000;
	
	/** How long we should wait for the info cell to reply before timing out, in milliseconds */
	private static final long INFO_CELL_TIMEOUT = 4000;
	
	private final CellPath _infoCellPath = new CellPath( INFO_CELL_NAME);
	private final CellNucleus _nucleus;

	/** Our local cache of the XML data */
	private byte _cache[];
	private Date _whenReceived;


	/**
	 * The constructor simply creates a new nucleus for us to use when sending messages.
	 */
	public InfoHttpEngine() {
        if( _log.isInfoEnabled())
        	_log.info("in InfoHttpEngine constructor");

		_nucleus = new CellNucleus( this, this.getClass().getSimpleName());
	}

	/**
	 * Handle a request for data.  This either returns the cached contents (if
	 * still valid), or queries the info cell for information.
	 */
	public void queryUrl(HttpRequest request) throws HttpException {

		String[]    urlItems = request.getRequestTokens();
        OutputStream out     = request.getOutputStream();

        if( _log.isInfoEnabled()) {
        	StringBuilder sb = new StringBuilder();
        	
        	for( String urlItem : urlItems) {
        		if( sb.length() > 0)
        			sb.append( "/");
         		sb.append( urlItem);
        	}
        	_log.info( "Received request for: " + sb.toString());
        }
        
        
        /**
         * Maintain our cache of XML.  This prevents end-users from thrashing the info cell.
         */
        if( _whenReceived == null || System.currentTimeMillis() - _whenReceived.getTime() > MAX_CACHE_AGE) {

        	try {
        		updateXMLCache();
        	} catch( TimeoutException e) {
                throw new HttpException( 503 , "The info cell took too long to reply, suspect trouble.");        		
        	} catch( NotSerializableException e) {
                throw new HttpException( 500, "Internal error when requesting info from info cell.");
        	} catch( NoRouteToCellException e) {
                throw new HttpException( 503 , "Unable to contact the info cell.  Please ensure the info cell is running.");        		
        	} catch( NullPointerException e) {
        		throw new HttpException( 500, "Received no sensible reply from info cell.  See info cell for details.");
        	}
        }
        
        request.setContentType( "application/xml");

        try {
            request.printHttpHeader( _cache.length);
        	out.write( _cache);
        } catch( IOException e) {
        	_log.error("IOException caught whilst writing output : " + e.getMessage());
        }
	}
	
	
	/**
	 * Send a message off to the info cell
	 */
	public void updateXMLCache() throws NotSerializableException, NoRouteToCellException, TimeoutException {

		if( _log.isDebugEnabled())
        	_log.debug( "Attempting to update XML cache");
		
		CellMessage envelope = new CellMessage( _infoCellPath, new InfoGetSerialisedDataMessage());

		try {
			CellMessage replyMsg = _nucleus.sendAndWait( envelope, INFO_CELL_TIMEOUT);
			
			if( replyMsg == null)
				throw new TimeoutException();
			
			Object replyObj = replyMsg.getMessageObject();

			// Bizarre!  We have to throw this ourselves.
			if( replyObj instanceof NoRouteToCellException)
				throw (NoRouteToCellException) replyObj;
			
			// A catch-all for when the reply isn't what we are expecting.
			if( !(replyObj instanceof InfoGetSerialisedDataMessage))
				throw new NotSerializableException();
			
			InfoGetSerialisedDataMessage reply = (InfoGetSerialisedDataMessage) replyObj;
			
			String serialisedData = reply.getSerialisedData();
			
			if( serialisedData != null) {
				_cache = serialisedData.getBytes();
				_whenReceived = new Date();
			} else {
				
				/**
				 *  TODO: replyStr == null should only come from a problem within the Info cell
				 *  when serialising the content.  This should be handled by a specific Exception
				 *  that is propagated via the vehicle.
				 */
				throw new NullPointerException();
			}
			
		} catch( InterruptedException e) {
			_cache = null;
			_whenReceived = null;
			// But otherwise, we ignore this Exception (is this correct?)
		}
	}
	
		
   public String getInfo() {
	   return "working normally";
   }
   
   /**
    *  We don't expect any messages to arrive outside of our sendAndWait() above.
    */
   public void   messageArrived( MessageEvent me ) {}   
   public void prepareRemoval( KillEvent killEvent) {}
   public void exceptionArrived( ExceptionEvent ce) {}
}
