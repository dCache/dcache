package org.dcache.pool.repository;

/**
 * Implementations of this interface may listen for state change events from
 * a repository.
 */
public interface StateChangeListener
{
    /**
     * Called upon state changes of any repository entry.
     *
     * New entries are generated in the NEW state and do not trigger notifications
     * until the state is changed. Upon pool restart, a state change event is generated
     * for every entry with the source state being NEW.
     */
    void stateChanged(StateChangeEvent event);

    /**
     * Called upon access time changes of any repository entry.
     */
    void accessTimeChanged(EntryChangeEvent event);

    /**
     * Called upon changes to sticky flags of any repository entry.
     */
    void stickyChanged(StickyChangeEvent event);
}
