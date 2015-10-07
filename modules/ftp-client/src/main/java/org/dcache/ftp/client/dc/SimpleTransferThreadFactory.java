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
package org.dcache.ftp.client.dc;

import org.dcache.ftp.client.DataSource;
import org.dcache.ftp.client.DataSink;
import org.dcache.ftp.client.vanilla.BasicServerControlChannel;

public class SimpleTransferThreadFactory
        implements TransferThreadFactory
{

    @Override
    public TransferThread
    getTransferSinkThread(DataChannel dataChannel,
                          SocketBox socketBox,
                          DataSink sink,
                          BasicServerControlChannel localControlChannel,
                          TransferContext context) throws Exception
    {

        return new TransferSinkThread((SimpleDataChannel) dataChannel,
                                      socketBox,
                                      sink,
                                      localControlChannel,
                                      context);
    }

    @Override
    public TransferThread
    getTransferSourceThread(DataChannel dataChannel,
                            SocketBox socketBox,
                            DataSource source,
                            BasicServerControlChannel localControlChannel,
                            TransferContext context) throws Exception
    {

        return new TransferSourceThread((SimpleDataChannel) dataChannel,
                                        socketBox,
                                        source,
                                        localControlChannel,
                                        context);
    }

}

