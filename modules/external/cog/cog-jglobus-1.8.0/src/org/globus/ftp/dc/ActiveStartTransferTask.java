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

import org.globus.ftp.vanilla.BasicServerControlChannel;
import org.globus.ftp.vanilla.FTPServerFacade;
import org.globus.ftp.Session;
import org.globus.ftp.DataSource;
import org.globus.ftp.DataSink;
import org.globus.ftp.HostPort;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
   This task will start the transfer on the supplied socket.
   The socket is assumed to have been already connected to the
   remote server (for instance, by active connect task).
   It is little tricky: it will cause data channel to start
   a new thread. By the time this task completes, the new
   thread is running the transfer.
   Any resulting exceptions are piped to the local control channel.
**/
public class ActiveStartTransferTask extends Task {
	static Log logger =
		LogFactory.getLog(ActiveStartTransferTask.class.getName());
	HostPort hostPort;
	BasicServerControlChannel control;

	protected static final int STOR = 1, RETR = 2;
	int operation;
	DataSink sink;
	DataSource source;
	SocketBox box;
	Session session;
	DataChannelFactory factory;
	TransferContext context;

	public ActiveStartTransferTask(
		DataSink sink,
		BasicServerControlChannel control,
		SocketBox box,
		Session session,
		DataChannelFactory factory,
		TransferContext context) {
		this.sink = sink;
		init(STOR, control, box, session, factory, context);
	}

	public ActiveStartTransferTask(
		DataSource source,
		BasicServerControlChannel control,
		SocketBox box,
		Session session,
		DataChannelFactory factory,
		TransferContext context) {
		this.source = source;
		init(RETR, control, box, session, factory, context);
	}

	private void init(
		int operation,
		BasicServerControlChannel control,
		SocketBox box,
		Session session,
		DataChannelFactory factory,
		TransferContext context) {

		if (box == null) {
			throw new IllegalArgumentException("Socket box is null");
		}
		if (control == null) {
			throw new IllegalArgumentException("Control channel is null");
		}
		this.factory = factory;
		this.session = session;
		this.operation = operation;
		this.control = control;
		this.box = box;
		this.context = context;
	}

	public void execute() {
		try {
			try {
				if (box.getSocket() == null) {
					throw new IllegalArgumentException("socket is null");
				}
				logger.debug("active start transfer task executing");

				DataChannel dChannel = factory.getDataChannel(session, box);
				if (operation == STOR) {
					dChannel.startTransfer(sink, control, context);
				} else {

					dChannel.startTransfer(source, control, context);

				}
 			} catch (Exception e) {
	 			FTPServerFacade.exceptionToControlChannel(
		 			e,
					"startTransfer() failed",
					control);
			}
		} catch (Exception e) {
		    FTPServerFacade.cannotPropagateError(e);
		}

	}
}
