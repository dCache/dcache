package dmg.cells.nucleus;

/**
 * Implemented by objects capable of providing diagnostic context
 * information.
 *
 * Typically implemented by messages, with the expectation that the
 * information is pushed to the Log4j Nested Diagnostic Context while
 * processing the message.
 */
public interface HasDiagnosticContext
{
    String getDiagnosticContext();
}