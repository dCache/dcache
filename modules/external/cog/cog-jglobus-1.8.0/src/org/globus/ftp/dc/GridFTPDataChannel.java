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

import org.globus.ftp.GridFTPSession;
import org.globus.ftp.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
   GridFTPDataChannel, unlike SimpleDataChannel, does not own the associated socket and does not destroy it when the transfer completes. It is the facade who is responsible for socket lifetime management. This approach allows for data channel reuse.
 **/
public class GridFTPDataChannel extends SimpleDataChannel {

	private static Log logger =
		LogFactory.getLog(GridFTPDataChannel.class.getName());

	// utility alias to session
	protected GridFTPSession gSession;

	public GridFTPDataChannel(Session session, SocketBox socketBox) {
		super(session, socketBox);
		gSession = (GridFTPSession) session;
		transferThreadFactory = new GridFTPTransferThreadFactory();
	}

	// todo: reimplement close()?
}
