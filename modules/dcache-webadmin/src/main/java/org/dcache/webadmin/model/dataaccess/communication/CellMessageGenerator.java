package org.dcache.webadmin.model.dataaccess.communication;

import java.io.Serializable;

import dmg.cells.nucleus.CellPath;

import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;

/**
 * Generates messages to be send and acts as a container for the requests generated.
 * So that they can easily retrieved via an iterator.
 * @author jans
 */
public interface CellMessageGenerator<M extends Serializable> extends Iterable<CellMessageRequest<M>> {

    int getNumberOfMessages();

    interface CellMessageRequest<M> {

        M getPayload();

        Class<M> getPayloadType();

        boolean isSuccessful();

        void setSuccessful(boolean successful);

        CellPath getDestination();

        void setAnswer(Serializable answer);

        M getAnswer();
    }
}
