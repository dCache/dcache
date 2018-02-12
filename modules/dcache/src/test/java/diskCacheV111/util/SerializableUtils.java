package diskCacheV111.util;

import org.apache.curator.shaded.com.google.common.io.BaseEncoding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
public class SerializableUtils
{
    private static final int STRING_LINE_LENGTH = 61;

    public static void assertSerialisationExpected(String message, String expected,
            Object object) throws IOException
    {
        String actual = BaseEncoding.base16().lowerCase().encode(serialise(object));

        assertEquals(message, expected, actual);
    }

    public static void assertDeserialisationExpected(String message, Object expected,
            String encoded) throws ClassNotFoundException, IOException
    {
        Object actual = deserialise(BaseEncoding.base16().lowerCase().decode(encoded));

        assertEquals(message, expected, actual);
    }

    private static byte[] serialise(Object object) throws IOException
    {
        ByteArrayOutputStream storage = new ByteArrayOutputStream();
        try (ObjectOutput objectOutput = new ObjectOutputStream(storage)) {
            objectOutput.writeObject(object);
        }
        return storage.toByteArray();
    }

    private static Object deserialise(byte[] serialised) throws IOException,
            ClassNotFoundException
    {
        return new ObjectInputStream(new ByteArrayInputStream(serialised))
                .readObject();
    }

    public static void emitJavaStringDeclaration(String name, Object id) throws IOException
    {
        String data = BaseEncoding.base16().lowerCase().encode(serialise(id));
        List<String> lines = breakStringIntoLines(data);
        String declaration = buildJavaStringDeclaration(name, lines);
        System.out.println( declaration);
    }

    private static String buildJavaStringDeclaration(String name, List<String> lines)
    {
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

    private static List<String> breakStringIntoLines(String data)
    {
        List<String> lines = new ArrayList<>();

        String remaining;
        for (String current = data; !current.isEmpty(); current = remaining) {
            int length = current.length() < STRING_LINE_LENGTH
                    ? current.length()
                    : STRING_LINE_LENGTH;

            lines.add(current.substring(0, length));
            remaining = current.substring(length, current.length());
        }

        return lines;
    }

}
