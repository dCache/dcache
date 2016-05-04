package org.dcache.pinmanager;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.security.auth.Subject;

import java.util.Date;
import java.util.List;

import diskCacheV111.util.PnfsId;

import org.dcache.pinmanager.model.Pin;

/**
 * Data Access Object abstraction for pin persistence.
 *
 * Provides a fluent API for CRUD operations.
 */
@ParametersAreNonnullByDefault
public interface PinDao
{
    /**
     * Returns a criterion builder.
     */
    PinCriterion where();

    /**
     * Returns a field value builder.
     */
    PinUpdate set();

    /**
     * Creates a new pin with the given field values.
     */
    Pin create(PinUpdate update);

    /**
     * Returns the pins matching a selection criterion.
     */
    List<Pin> get(PinCriterion criterion);

    /**
     * Returns the pins matching a selection criterion with an
     * upper limit on the number of pins returned.
     */
    List<Pin> get(PinCriterion criterion, int limit);

    /**
     * Returns the pin matching a unique criterion.
     *
     * @return The matching pin or null if the criterion did not match a pin
     * @throws IncorrectResultSizeDataAccessException if more than one
     * pin has been found for the given criterion.
     */
    @Nullable
    Pin get(UniquePinCriterion criterion);

    /**
     * Returns the number of pins matching a selection criterion.
     */
    int count(PinCriterion criterion);

    /**
     * Updates a specific pin with the given field values.
     *
     * @return The updated pin or null if the criterion did not match a pin
     * @throws JdbcUpdateAffectedIncorrectNumberOfRowsException if more than one row is updated.
     */
    @Nullable
    default Pin update(Pin pin, PinUpdate update) {
        return update(where().id(pin.getPinId()), update);
    }

    /**
     * Updates a pin matching a unique criterion with the given field
     * values.
     *
     * @return The updated pin or null if the criterion did not match a pin
     * @throws JdbcUpdateAffectedIncorrectNumberOfRowsException if more than one row is updated.
     */
    @Nullable
    Pin update(UniquePinCriterion criterion, PinUpdate update);

    /**
     * Updates all pins matching a selection criterion with the given field
     * values.
     */
    int update(PinCriterion criterion, PinUpdate update);

    /**
     * Deletes all pins matching a selection criterion.
     */
    int delete(PinCriterion criterion);

    /**
     * Deletes a specific pin.
     */
    default int delete(Pin pin) {
        return delete(where().id(pin.getPinId()));
    }

    /**
     * Performs the given action for every pin matching the selection criterion.
     */
    void foreach(PinCriterion c, InterruptibleConsumer<Pin> f) throws InterruptedException;

    /**
     * Fluent interface to construct selection criteria.
     */
    interface PinCriterion
    {
        UniquePinCriterion id(long id);

        PnfsIdPinCriterion pnfsId(PnfsId id);

        RequestIdPinCriterion requestId(String requestId);

        PinCriterion expirationTimeBefore(Date date);

        PinCriterion state(Pin.State state);

        PinCriterion stateIsNot(Pin.State state);

        PinCriterion pool(String pool);

        PinCriterion sticky(String sticky);

        UniquePinCriterion sameIdAs(UniquePinCriterion c);
    }

    /**
     * Fluent interface to construct selection criteria where the
     * PnfsId has been fixed.
     */
    interface PnfsIdPinCriterion extends PinCriterion
    {
        UniquePinCriterion requestId(String requestId);

        PnfsIdPinCriterion expirationTimeBefore(Date date);

        PnfsIdPinCriterion state(Pin.State state);

        PnfsIdPinCriterion stateIsNot(Pin.State state);

        PnfsIdPinCriterion pool(String pool);

        PnfsIdPinCriterion sticky(String sticky);
    }

    /**
     * Fluent interface to construct selection criteria where the
     * request ID has been fixed.
     */
    interface RequestIdPinCriterion extends PinCriterion
    {
        UniquePinCriterion pnfsId(PnfsId id);

        RequestIdPinCriterion expirationTimeBefore(Date date);

        RequestIdPinCriterion state(Pin.State state);

        RequestIdPinCriterion stateIsNot(Pin.State state);

        RequestIdPinCriterion pool(String pool);

        RequestIdPinCriterion sticky(String sticky);
    }

    /**
     * Fluent interface to construct selection criteria matching at
     * most a single pin.
     */
    interface UniquePinCriterion extends PinCriterion, PnfsIdPinCriterion, RequestIdPinCriterion
    {
        UniquePinCriterion id(long id);

        UniquePinCriterion pnfsId(PnfsId id);

        UniquePinCriterion requestId(String requestId);

        UniquePinCriterion expirationTimeBefore(Date date);

        UniquePinCriterion state(Pin.State state);

        UniquePinCriterion stateIsNot(Pin.State state);

        UniquePinCriterion pool(String pool);

        UniquePinCriterion sticky(String sticky);
    }

    /**
     * Fluent interface to define pin field values.
     */
    interface PinUpdate
    {
        PinUpdate expirationTime(@Nullable Date date);
        PinUpdate pool(@Nullable String pool);
        PinUpdate requestId(@Nullable String requestId);
        PinUpdate state(Pin.State state);
        PinUpdate sticky(@Nullable String sticky);
        PinUpdate subject(Subject subject);

        PinUpdate pnfsId(PnfsId pnfsId);
    }

    interface InterruptibleConsumer<T>
    {
        void accept(T v) throws InterruptedException;
    }
}
