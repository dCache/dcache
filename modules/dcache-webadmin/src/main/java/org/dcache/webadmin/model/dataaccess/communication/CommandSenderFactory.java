package org.dcache.webadmin.model.dataaccess.communication;

/**
 * describes the mechanism to get a CommandSender instantiated
 * @author jans
 */
public interface CommandSenderFactory {

    public CommandSender createCommandSender(CellMessageGenerator<?> messageGenerator);
}
