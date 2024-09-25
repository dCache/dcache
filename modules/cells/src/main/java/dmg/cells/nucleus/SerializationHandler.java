/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 - 2023 Deutsches Elektronen-Synchrotron
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

import static com.google.common.base.Strings.isNullOrEmpty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class contains methods for serializing and deserializing objects to/from a byte array
 * representation. It selects the deserialization method based on a header that is appended to
 * serialized byte arrays based on the used serializer. Currently the class can differentiate
 * between JOS and FST.
 */
public final class SerializationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SerializationHandler.class);

    public enum Serializer {
        UNDEFINED("undefined"), JOS("standard");

        private final String displayName;

        Serializer(String name) {
            this.displayName = name;
        }

        public String toString() {
            return displayName;
        }
    }

    private SerializationHandler() {
    }

    public static Serializer enumFromConfigString(String configString) {
        if (isNullOrEmpty(configString)) {
            return Serializer.JOS;
        }
        String serializerString = configString.toLowerCase();
        switch (serializerString) {
            case "standard":
                return Serializer.JOS;
            case "experimental":
                // keep the keyword for the future use.
            default:
                LOGGER.warn("Unknown serializer specified in configuration. Defaulting to {}.",
                      Serializer.JOS);
                return Serializer.JOS;

        }
    }

    public static boolean isEncodedWith(byte[] msgStream, Serializer serializer) {
        return true; // we expect only one serialization type
    }

    public static byte[] encode(Object message, Serializer serializer) {
        switch (serializer) {
            case JOS:
                return MsgSerializerJos.encode(message);
            case UNDEFINED:
            default:
                throw new UnsupportedOperationException(
                      "No such serializer. This should never happen.");
        }
    }

    public static Object decode(byte[] messageStream) {
        return MsgSerializerJos.decode(messageStream);
    }

}
