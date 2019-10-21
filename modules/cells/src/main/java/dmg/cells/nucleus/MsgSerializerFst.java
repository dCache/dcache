/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dmg.cells.nucleus;

import org.nustaq.serialization.FSTConfiguration;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;

/**
 * The class contains methods for serializing and deserializing
 * objects to/from a byte array representation. It uses
 * the fast-serialization (FST) serializer.
 */
public final class MsgSerializerFst {

    private static final int INITIAL_BUFFER_SIZE = 256;
    private static final FSTConfiguration fstConf = FSTConfiguration.createDefaultConfiguration();
    static {
        fstConf.setPreferSpeed(true);
    }

    private static final byte[] FST_MESSAGE_HEADER = new byte[] {
            0x05, 0x4d,   // 054D -> [o]bject [s]tream [for] [d]Cache
            0x00, 0x01    // version 1
    };

    private MsgSerializerFst() {}

    public static byte[] encode(Object message) {
        checkState(message != null, "Unencoded message payload is null.");
        byte[] serialized = fstConf.asByteArray(message);

        ByteArrayOutputStream array = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        array.write(FST_MESSAGE_HEADER, 0, FST_MESSAGE_HEADER.length);
        array.write(serialized, 0, serialized.length);

        return array.toByteArray();
    }

    public static Object decode(byte[] messageStream) {
        checkState(messageStream != null, "Encoded message payload is null.");
        checkState (isFstEncoded(messageStream));
        return fstConf.asObject(Arrays.copyOfRange(messageStream, 4, messageStream.length));
    }

    public static boolean isFstEncoded(byte[] messageStream) {
        return (messageStream[0] == FST_MESSAGE_HEADER[0] &&
                messageStream[1] == FST_MESSAGE_HEADER[1] &&
                messageStream[2] == FST_MESSAGE_HEADER[2] &&
                messageStream[3] == FST_MESSAGE_HEADER[3] );
    }
}
