package org.dcache.acl;

/**
 * An exception occurred by ACLHandler
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class ACLException extends Exception {

    private static final long serialVersionUID = 42L;

    private static final String MSG_FAILED = " failed: ";

    private String _action;

    public ACLException(String action) {
        super(action + MSG_FAILED + "empty message.");
        _action = action;
    }

    public ACLException(String action, String message) {
        super(action + MSG_FAILED + message);
        _action = action;
    }

    public ACLException(String action, Throwable cause) {
        super(action + MSG_FAILED + cause.getMessage(), cause);
        _action = action;
    }

    public ACLException(String action, String message, Throwable cause) {
        super(action + MSG_FAILED + message + ": " + cause.getMessage(), cause);
        _action = action;
    }

    public String getAction() {
        return _action;
    }

    public String toString(){
        return "ACLException: " + getMessage();
    }

}
