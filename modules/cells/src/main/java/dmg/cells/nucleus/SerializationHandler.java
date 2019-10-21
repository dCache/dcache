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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * The class contains methods for serializing and deserializing
 * objects to/from a byte array representation. It selects the
 * deserialization method based on a header that is appended
 * to serialized byte arrays based on the used serializer.
 * Currently the class can differentiate between JOS and FST.
 */
public final class SerializationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SerializationHandler.class);

    public enum Serializer {
        UNDEFINED("undefined"), JOS("standard"), FST("experimental");

        private final String displayName;
        Serializer(String name) {
            this.displayName = name;
        }

        public String toString() {
            return displayName;
        }
    }

    private SerializationHandler() {}

    public static Serializer enumFromConfigString(String configString) {
        if(isNullOrEmpty(configString)){
            return Serializer.JOS;
        }
        String serializerString = configString.toLowerCase();
        switch(serializerString) {
            case "experimental":
                return Serializer.FST;
            case "standard" :
                return Serializer.JOS;
            default:
                LOGGER.warn("Unknown serializer specified in configuration. Defaulting to {}.", Serializer.JOS);
                return Serializer.JOS;

        }
    }

    public static boolean isEncodedWith(byte[] msgStream, Serializer serializer) {
        return serializer == Serializer.FST && MsgSerializerFst.isFstEncoded(msgStream);
    }

    public static byte[] encode(Object message, Serializer serializer) {
        switch(serializer) {
            case JOS :
                return MsgSerializerJos.encode(message);
            case FST:
                return MsgSerializerFst.encode(message);
            case UNDEFINED:
            default:
                throw new UnsupportedOperationException("No such serializer. This should never happen.");
        }
    }

    public static Object decode(byte[] messageStream) {
        if (MsgSerializerFst.isFstEncoded(messageStream)) {
            return MsgSerializerFst.decode(messageStream);
        }
        return MsgSerializerJos.decode(messageStream);
    }

}
