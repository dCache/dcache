package dmg.cells.nucleus;

/**
 * Thrown when a serious error occurred during serialization or
 * de-serialization. Usually indicates that an object is not
 * serializable or deserialized.
 */
public class SerializationException extends RuntimeException
{
    private static final long serialVersionUID = -8869521471350239459L;

    public SerializationException(String str)
    {
        super(str);
    }

    public SerializationException(String str, Throwable cause)
    {
        super(str, cause);
    }
}
