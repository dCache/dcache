package dmg.cells.nucleus;

import dmg.util.CommandInterpreter;

/**
 * An implementation of this interface can dynamically provide new admin commands (typically by
 * loading external classes).
 */

public interface CellDynamicCommandProvider {
    /**
     * Inject command interpreter into which commands can be dynamically added.
     * @param commandInterpreter command interpreter to use.
     */

    void setCommandInterpreter(CommandInterpreter commandInterpreter);
}