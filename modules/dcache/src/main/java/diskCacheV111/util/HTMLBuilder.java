package diskCacheV111.util;

import java.util.Map;
import java.io.StringWriter;

/**
 * Simple wrapper arround HTMLWriter to expose the functionality in a
 * builder style interface. Rather than writing the HTML document to
 * another writer, the builder returns the finished HTML document as a
 * string.
 */
public class HTMLBuilder extends HTMLWriter
{
    /**
     * Construct a new instance. The instance is bound to a cell
     * nucleus context, represented by a dictionary. Various settings
     * are taken from the dictionary.
     *
     * @param context Cell nucleus context
     */
    public HTMLBuilder(Map<String,Object> context)
    {
        super(new StringWriter(), context);
    }

    /**
     * Returns the HTML document as a string.
     */
    public String toString()
    {
        return _writer.toString();
    }

    /**
     * Writes the HTML document to the context.
     *
     * @param name The key under which to file the document in the context.
     */
    public void writeToContext(String name)
    {
        _context.put(name, toString());
    }
}
