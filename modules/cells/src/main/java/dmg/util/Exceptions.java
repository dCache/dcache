package dmg.util;

/**
 * Contains static utility methods for exceptions.
 */
public class Exceptions
{
    private Exceptions()
    {
    }

    /**
     * Returns the message string of the Throwable and that of all its
     * causes.
     */
    public static String getMessageWithCauses(Throwable t)
    {
        StringBuilder msg = new StringBuilder(t.getMessage());
        t = t.getCause();
        while (t != null) {
            msg.append("; caused by: ").append(t.getMessage());
            t = t.getCause();
        }
        return msg.toString();
    }
}
