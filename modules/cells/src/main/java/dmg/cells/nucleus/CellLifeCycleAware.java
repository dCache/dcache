package dmg.cells.nucleus;

/**
 * Classes implementing this method receive Cell life cycle
 * notifications.
 */
public interface CellLifeCycleAware
{
    /**
     * Called just after the cell has been started.
     */
    default void afterStart() {}

    /**
     * Called just before the cell is killed.
     */
    default void beforeStop() {}

    /**
     * Called before every execution of a complete cell setup.
     *
     * In case there is no setup to apply, this method will not be called. May be called before
     * {@code afterStart} if the cell is initialized with a setup during startup. If a setup
     * is loaded or reloaded subsequently, the method is called before executing that setup.
     *
     * Note that the cell setup context is not considered a 'setup' by these callbacks and the
     * callbacks are not called when applying the setup context.
     *
     * In case the method fails with an exception, the cell will be killed.
     */
    default void beforeSetup() {}

    /**
     * Called after every execution of a complete cell setup.
     *
     * Unless any {@code beforeSetup} call failed and thus killed the cell, every invocation of
     * {@code beforeSetup} is followed by an invocation of {@code afterSetup} once the setup has
     * been fully applied.
     *
     * In case the method fails with an exception, the cell will be killed.
     *
     * {@see beforeSetup}
     */
    default void afterSetup() {}
}
