package org.dcache.webadmin.model.dataaccess.communication;

/**
 * A command sender has to be able to send commands to dCache cells.
 * @author jans
 */
public interface CommandSender {

    void sendAndWait() throws InterruptedException;

    boolean allSuccessful();
}
