package diskCacheV111.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This class contains various utility methods for serialising and
 * deserialising objects. It is intended to facilitate unit-testing of
 * objects that implement the Serializable interface: if we want to check
 * that we can deserialise objects and catch if we break serialisable
 * compatibility.
 */
public class SerializableUtils {

    private static final int STRING_LINE_LENGTH = 61;

    public static void assertSerialisationExpected( String message,
                                                    String encodedExpected,
                                                    Object object)
            throws IOException {
        String encodedResult =
                SerializableUtils.serialiseAndEncodeObject( object);
        assertEquals( message, encodedExpected, encodedResult);
    }

    public static void assertDeserialisationExpected(
                                                      String message,
                                                      Object expectedObject,
                                                      String encodedSerialisedObject)
            throws ClassNotFoundException, IOException {
        byte[] serialisedData =
                SerializableUtils.decode( encodedSerialisedObject);
        ByteArrayInputStream byteStream =
                new ByteArrayInputStream( serialisedData);
        ObjectInput objectInput = new ObjectInputStream( byteStream);

        Object deserialisedObject = objectInput.readObject();

        assertEquals( message, expectedObject, deserialisedObject);
    }

    public static String serialiseAndEncodeObject( Object object)
            throws IOException {
        ByteArrayOutputStream storage = new ByteArrayOutputStream();
        ObjectOutput objectOutput = new ObjectOutputStream( storage);
        objectOutput.writeObject( object);
        objectOutput.close();

        return SerializableUtils.encode( storage.toByteArray());
    }

    // Based on code from http://...
    public static String encode( byte[] byteStream) {
        StringBuilder result = new StringBuilder();
        for( byte curr : byteStream) {
            result.append(Integer.toString((curr & 0xff) + 0x100, 16)
                    .substring(1));
        }
        return result.toString();
    }

    // Taken from
    // http://stackoverflow.com/questions/140131/convert-a-string-representation-of-a-hex-dump-to-a-byte-array-using-java
    public static byte[] decode( final String encoded) {
        if( (encoded.length() % 2) != 0) {
            throw new IllegalArgumentException(
                    "Input string must contain an even number of characters");
        }

        final byte result[] = new byte[encoded.length() / 2];
        final char enc[] = encoded.toCharArray();
        for( int i = 0; i < enc.length; i += 2) {
            StringBuilder curr = new StringBuilder( 2);
            curr.append( enc[i]).append( enc[i + 1]);
            result[i / 2] = (byte) Integer.parseInt( curr.toString(), 16);
        }
        return result;
    }

    public static void emitJavaStringDeclaration( String name, String data) {
        List<String> lines = breakStringIntoLines( data);
        String declaration = buildJavaStringDeclaration( name, lines);
        System.out.println( declaration);
    }

    public static String buildJavaStringDeclaration( String name,
                                                     List<String> lines) {
        StringBuilder sb = new StringBuilder();

        sb.append("    private static final String ").append(name)
                .append(" =\n");

        boolean isFirstLine = true;

        for( String line : lines) {
            if( isFirstLine) {
                sb.append("                     ");
            } else {
                sb.append("\n                   + ");
            }
            sb.append("\"").append(line).append("\"");
            isFirstLine = false;
        }

        sb.append( ";\n");

        return sb.toString();
    }

    public static List<String> breakStringIntoLines( String data) {
        List<String> lines = new ArrayList<>();

        String remaining;
        for( String current = data; current.length() > 0; current = remaining) {

            int thisLineLength =
                    current.length() < STRING_LINE_LENGTH ? current.length()
                            : STRING_LINE_LENGTH;

            String thisLine = current.substring( 0, thisLineLength);
            remaining = current.substring( thisLineLength, current.length());

            lines.add( thisLine);
        }

        return lines;
    }

}
