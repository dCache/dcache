package dmg.cells.nucleus;

/**
 * Classes implementing this method receive notifications about cell and route
 * creation and removal events.
 */
public interface CellEventListener
{
    default void cellCreated(CellEvent ce)
    {
    }

    default void cellDied(CellEvent ce)
    {
    }

    default void routeAdded(CellEvent ce)
    {
    }

    default void routeDeleted(CellEvent ce)
    {
    }
}
