package org.dcache.macaroons;

import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsBuilder;
import com.github.nitram509.jmacaroons.MacaroonsVerifier;
import com.google.common.net.InetAddresses;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class ContextExtractingCaveatVerifierTest {

    private ContextExtractingCaveatVerifier contextCaveatVerifier;

    @Before
    public void setUp() {
        contextCaveatVerifier = new ContextExtractingCaveatVerifier();
    }

    @Test
    public void testUnsupportedCaveat() {
        assertFalse(contextCaveatVerifier.verifyCaveat("ip:127.0.0.1"));
    }

    @Test
    public void testMacaroonWithManyCaveat() {

        String location = "https://dcache.org";
        String secretKey = "tiramisu is better!";
        String identifier = "junit";

        Macaroon macaroon = new MacaroonsBuilder(location, secretKey, identifier)
                .add_first_party_caveat(
                        "ip:192.168.1.1/24"
                )
                .add_first_party_caveat(
                        "before:2047-08-05T00:00:00.00Z"
                )
                .getMacaroon();

        MacaroonsVerifier verifier = new MacaroonsVerifier(macaroon);

        ClientIPCaveatVerifier clientIPVerifier = new ClientIPCaveatVerifier(InetAddresses.forString("192.168.2.1"));
        verifier.satisfyGeneral(clientIPVerifier);
        verifier.satisfyGeneral(contextCaveatVerifier);

        assertFalse("IP address from not allowed range accepted", verifier.isValid(secretKey));
    }
}
