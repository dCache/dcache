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

import java.io.*;

import static com.google.common.base.Preconditions.checkState;

/**
 * The class contains methods for serializing and deserializing
 * objects to/from a byte array representation. It uses
 * the native Java Object Serialization.
 */
public final class MsgSerializerJos {
    private static final int INITIAL_BUFFER_SIZE = 256;

    private MsgSerializerJos() {}

    public static byte[] encode(Object message) {
        checkState(message != null, "Unencoded message payload is null.");
        ByteArrayOutputStream array = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        try (ObjectOutputStream out = new ObjectOutputStream(array)) {
            out.writeObject(message);
        } catch (InvalidClassException e) {
            throw new SerializationException("Failed to serialize object: "
                    + e + "(this is usually a bug)", e);
        } catch (NotSerializableException e) {
            throw new SerializationException("Failed to serialize object because the object is not serializable (this is usually a bug)", e);
        } catch (IOException e) {
            throw new SerializationException("Failed to serialize object: " + e, e);
        }

        return array.toByteArray();
    }

    public static Object decode(byte[] messageStream) {
        checkState(messageStream != null, "Encoded message payload is null.");
        try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(messageStream))) {
            Object decoded = stream.readObject();
            return decoded;
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Failed to deserialize object: The class could not be found. Is there a software version mismatch in your installation?", e);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize object: " + e, e);
        }
    }
}
