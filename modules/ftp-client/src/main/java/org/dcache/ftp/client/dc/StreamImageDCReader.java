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

import static org.dcache.util.ByteUnit.KiB;

import java.io.IOException;
import java.io.InputStream;
import org.dcache.ftp.client.Buffer;

public class StreamImageDCReader implements DataChannelReader {

    public static final int BUF_SIZE = KiB.toBytes(500);

    protected final int bufferSize = BUF_SIZE;
    protected InputStream input;

    @Override
    public void setDataStream(InputStream in) {
        input = in;
    }

    @Override
    public Buffer read()
          throws IOException {
        byte[] bt = new byte[bufferSize];
        int read = input.read(bt);
        if (read == -1) {
            return null;
        } else {
            return new Buffer(bt, read);
        }
    }

    @Override
    public void close()
          throws IOException {
        input.close();
    }

}
