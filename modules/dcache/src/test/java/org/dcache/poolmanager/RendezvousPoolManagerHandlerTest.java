package org.dcache.poolmanager;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.junit.Test;
import org.junit.Before;

import diskCacheV111.vehicles.PoolMgrGetPoolMsg;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;

import org.dcache.cells.FutureCellMessageAnswerable;
import org.dcache.vehicles.FileAttributes;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class RendezvousPoolManagerHandlerTest {


    private final String SERVICE_NAME = "PoolManager@local";

    private RendezvousPoolManagerHandler poolManagerHandler;
    private CellEndpoint cellEndpoint;
    private PoolMgrGetPoolMsg msg;
    private ArgumentCaptor<CellMessage> envelopeCaptor;

    @Before
    public void setUp() {
	cellEndpoint = mock(CellEndpoint.class);
	envelopeCaptor = ArgumentCaptor.forClass(CellMessage.class);
	msg = mock(PoolMgrGetPoolMsg.class);
	when(msg.getFileAttributes())
		.thenReturn(FileAttributes
			.ofPnfsId("00008B38D575774F46439388F526C8149EE2"));
    }

    @Test
    public void shouldReturnOneOfBackends() {

	givenPoolManagers("pm1@head1", "pm2@head1");
	messageWithoutAffinity();
	sendMessage();

	assertThat(envelopeCaptor.getValue().getDestinationPath().toAddressString(),
		is(SERVICE_NAME));
    }

    @Test
    public void shouldReturnServicePath() {

	givenPoolManagers("pm1@head1", "pm2@head1");
	messageWithAffinity();
	sendMessage();

	assertThat(envelopeCaptor.getValue().getDestinationPath().toAddressString(),
		anyOf(is("pm1@head1"), is("pm2@head1")));
    }

    private void givenPoolManagers(String... backends) {
	CellAddressCore service = new CellAddressCore(SERVICE_NAME);
	List<CellAddressCore> backendAddresses = Arrays.stream(backends)
		.map(CellAddressCore::new)
		.collect(Collectors.toList());

	poolManagerHandler = new RendezvousPoolManagerHandler(service, backendAddresses);
    }

    private void messageWithAffinity() {
	when(msg.requiresAffinity()).thenReturn(true);
    }

    private void messageWithoutAffinity() {
	when(msg.requiresAffinity()).thenReturn(false);
    }

    private void sendMessage() {

	poolManagerHandler.sendAsync(cellEndpoint, msg, 1L);

	verify(cellEndpoint).sendMessage(envelopeCaptor.capture(),
		any(FutureCellMessageAnswerable.class),
		any(Executor.class), anyLong());
    }

}
