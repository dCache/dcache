package org.dcache.util.cli;

import java.io.Serializable;

import dmg.util.CommandException;
import dmg.util.command.HelpFormat;

import org.dcache.util.Args;

/**
 * Implementations of this interface provides means to execute shell commands
 * and provide help and ACL information for such commands.
 */
public interface CommandExecutor
{
    /**
     * Returns true if and only if the command has any ACLs.
     */
    boolean hasACLs();

    /**
     * Returns the ACLs of the command. Returns the empty array
     * if no ACLs are defined.
     */
    String[] getACLs();

    /**
     * Returns the full help information of the command.
     *
     * If the format is not supported, the help is returned in some other format.
     *
     * @param format The format to return the help in.
     */
    String getFullHelp(HelpFormat format);

    /**
     * Returns a one-line signature and an optional description of the command.
     *
     * If the format is not supported, the help is returned in some other format.
     *
     * @param format The format to return the help in.
     */
    String getHelpHint(HelpFormat format);

    /**
     * Executes the command on the specified arguments.
     *
     * @param arguments The arguments for this execution.
     * @return Result of executing the command.
     */
    Serializable execute(Args arguments) throws CommandException;
}
