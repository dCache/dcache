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

import java.io.OutputStream;
import java.io.IOException;

import org.dcache.ftp.client.Buffer;

public class StreamImageDCWriter implements DataChannelWriter
{

    protected OutputStream output;

    @Override
    public void setDataStream(OutputStream out)
    {
        output = out;
    }

    @Override
    public void write(Buffer buf)
            throws IOException
    {
        output.write(buf.getBuffer(), 0, buf.getLength());
    }

    @Override
    public void endOfData() throws IOException
    {
    }

    @Override
    public void close()
            throws IOException
    {
        output.close();
    }
}
