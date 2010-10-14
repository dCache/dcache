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

import org.globus.ftp.Session;
import org.globus.ftp.GridFTPSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GridFTPDataChannelFactory implements DataChannelFactory {

    protected static Log logger =
        LogFactory.getLog(GridFTPDataChannelFactory.class.getName());

    public DataChannel getDataChannel(Session session, SocketBox socketBox) {
	if (! (session instanceof GridFTPSession)) {
	    throw new IllegalArgumentException(
					       "session should be a GridFTPSession");
	}

	logger.debug("starting secure data channel");
	return new GridFTPDataChannel(session, socketBox);

    }
}
