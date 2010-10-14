/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.ftp.dc;

import java.util.Map;
import java.util.HashMap;

import org.globus.ftp.GridFTPSession;
import org.globus.ftp.Session; 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractDataChannel implements DataChannel {

    private static Log logger = LogFactory.getLog(AbstractDataChannel.class.getName());

    protected Session session;

    protected static Map dataHandlers;

    private static final int SOURCE = 1;
    private static final int SINK   = 2;

    static {
	try {

	    // Stream [Image/Ascii] Reader and Writer support

	    registerHandler(Session.MODE_STREAM, 
			    Session.TYPE_IMAGE,
			    SOURCE,
			    StreamImageDCReader.class);

	    registerHandler(Session.MODE_STREAM, 
			    Session.TYPE_ASCII,
			    SOURCE,
			    StreamAsciiDCReader.class);
	
	    registerHandler(Session.MODE_STREAM, 
			    Session.TYPE_IMAGE,
			    SINK,
			    StreamImageDCWriter.class);

	    registerHandler(Session.MODE_STREAM, 
			    Session.TYPE_ASCII,
			    SINK,
			    StreamAsciiDCWriter.class);

	    // EBlock

	    registerHandler(GridFTPSession.MODE_EBLOCK,
			    Session.TYPE_IMAGE,
			    SOURCE,
			    EBlockImageDCReader.class);

	    registerHandler(GridFTPSession.MODE_EBLOCK,
			    Session.TYPE_IMAGE,
			    SINK,
			    EBlockImageDCWriter.class);

	    // EBlock ASCII modes not supported

	} catch (Exception e) {
	    throw new RuntimeException("Failed to install default data channel handlers: " + e.getMessage());
	}
    }


    public AbstractDataChannel(Session session) {
	this.session = session;
    }


    public static void registerHandler(int transferMode,
				int transferType,
				int type,
				Class clazz) 
	throws Exception {
	switch (type) {
	case SOURCE:
	    if (!DataChannelReader.class.isAssignableFrom(clazz)) {
		throw new Exception("Incorrect type");
	    }
	    break;
	case SINK:
	    if (!DataChannelWriter.class.isAssignableFrom(clazz)) {
		throw new Exception("Incorrect type");
	    }
	    break;
	default:
	    throw new IllegalArgumentException("Type not supported: " + 
					       type);
	}   

	String id = getHandlerID(transferMode, transferType, type);

	if (dataHandlers == null) {
	    dataHandlers = new HashMap();
	}

	// Allow for overwrites
	/*
	if (dataHandlers.get(id) != null) {
	    throw new Exception("Handler already registered.");
	}
	*/
	
	logger.debug("registering handler for class " + clazz.toString() + "; id = " + id); 
	dataHandlers.put(id, clazz);
    }

    /**
     * Tests if the client supports specified transfer type and mode
     * (the client can read data in specific type & mode from the
     * data connection)
     */
    public boolean isDataSourceModeSupported() {
	String id = getHandlerID(session.transferMode, session.transferType, SOURCE);
	return (dataHandlers.get(id) != null);
    }

    /**
     * Tests if the client supports specified transfer type and mode
     * (the client can write data in specific type & mode to the
     * data connection)
     */
    public boolean isDataSinkModeSupported() {
	String id = getHandlerID(session.transferMode, session.transferType, SINK);
	return (dataHandlers.get(id) != null);
    }

    // currently context is only needed in case of EBlock mode

    public DataChannelReader getDataChannelSource(TransferContext context) 
	throws Exception {
	String id = getHandlerID(session.transferMode, session.transferType, SOURCE);
	logger.debug("type/mode: " + id);
	Class clazz = (Class)dataHandlers.get(id);
	if (clazz == null) {
	    throw new Exception("No data reader for type/mode" + id);
	}
	DataChannelReader reader =  (DataChannelReader)clazz.newInstance();
	if (reader instanceof EBlockAware) {
	    ((EBlockAware)reader).setTransferContext((EBlockParallelTransferContext)context);
	}
	return reader;
    }

    public DataChannelWriter getDataChannelSink(TransferContext context) 
	throws Exception {
	String id = getHandlerID(session.transferMode, session.transferType, SINK);
	Class clazz = (Class)dataHandlers.get(id);
	if (clazz == null) {
	    throw new Exception("No data reader for type/mode");
	}
	DataChannelWriter writer =  (DataChannelWriter)clazz.newInstance();
	if (writer instanceof EBlockAware) {
	    ((EBlockAware)writer).setTransferContext((EBlockParallelTransferContext)context);
	}
	return writer;
    }

    // it is important for this method to handle all possible
    // mode/transfer types
    private static String getHandlerID(int transferMode,
				       int transferType,
				       int type) {
	String id = "";

	switch (transferMode) {
	case Session.MODE_STREAM:
	    id += "S-"; break;
	case GridFTPSession.MODE_EBLOCK:
	    id += "E-"; break;
	default:
	    throw new IllegalArgumentException("Mode not supported: " + 
					       transferMode);
	}

	switch (transferType) {
	case Session.TYPE_IMAGE:
	    id += "I-"; break;
	case Session.TYPE_ASCII:
	    id += "A-"; break;
	default:
	    throw new IllegalArgumentException("Type not supported: " + 
					       transferType);
	}

	switch (type) {
	case SOURCE:
	    id += "R"; break;
	case SINK:
	    id += "W"; break;
	default:
	    throw new IllegalArgumentException("Type not supported: " + 
					       type);
	}   
	
	if (id.equals("")) {
	    return null;
	}
	return id;
    }
	
}
