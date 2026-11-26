/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.macaroons;

import static org.dcache.macaroons.MacaroonContextBuilder.macaroonContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.fail;

import com.github.nitram509.jmacaroons.MacaroonsBuilder;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Supplier;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that a revoked macaroon is rejected by MacaroonProcessor.expandMacaroon, that revocation
 * is keyed by the macaroon itself (not by any separately-issued identifier), and that other
 * macaroons are unaffected.
 */
public class MacaroonRevocationTest {

    private static final byte[] SECRET = new byte[32];
    private static final String SECRET_ID = "test-secret";

    private static final InetAddress CLIENT;

    static {
        try {
            CLIENT = InetAddress.getByName("192.168.1.1");
        } catch (UnknownHostException e) {
            throw new Error(e);
        }
    }

    private static MacaroonProcessor processor;
    private static MacaroonRevocationList revocations;

    @BeforeClass
    public static void setupProcessor() {
        for (int i = 0; i < SECRET.length; i++) {
            SECRET[i] = (byte) i;
        }
        SecretHandler handler = new SecretHandler() {
            @Override
            public IdentifiedSecret secretExpiringAfter(Instant expiry,
                  Supplier<IdentifiedSecret> newSecret) {
                return new IdentifiedSecret(SECRET_ID, SECRET);
            }

            @Override
            public byte[] findSecret(String identifier) {
                return SECRET_ID.equals(identifier) ? SECRET : null;
            }
        };
        revocations = new MacaroonRevocationList();
        processor = new MacaroonProcessor();
        processor.setSecretHandler(handler);
        processor.setRevocationList(revocations);
    }

    private String mintMacaroon() throws Exception {
        return processor.buildMacaroon(Instant.now().plus(1, ChronoUnit.HOURS),
              macaroonContext().withUid(2).withGid(3).withUsername("tester").build(),
              List.of()).getMacaroon();
    }

    @Test
    public void shouldExpandUnrevokedMacaroon() throws Exception {
        processor.expandMacaroon(mintMacaroon(), CLIENT);
        // no exception means pass
    }

    @Test
    public void shouldRejectRevokedMacaroon() throws Exception {
        String token = mintMacaroon();

        revocations.revoke(token);

        try {
            processor.expandMacaroon(token, CLIENT);
            fail("revoked macaroon was accepted");
        } catch (InvalidMacaroonException e) {
            assertThat(e.getMessage(), containsString("has been revoked"));
        }
    }

    @Test
    public void shouldRevokeByPresentingTheTokenItself() throws Exception {
        String token = mintMacaroon();

        // a second, independent copy of the same token resolves to the same revocation key
        String copy = token;

        revocations.revoke(copy);

        try {
            processor.expandMacaroon(token, CLIENT);
            fail("revoked macaroon was accepted via original copy");
        } catch (InvalidMacaroonException e) {
            assertThat(e.getMessage(), containsString("has been revoked"));
        }
    }

    @Test
    public void revokingOneMacaroonLeavesOthersAlone() throws Exception {
        String leaked = mintMacaroon();
        String other = mintMacaroon();

        revocations.revoke(leaked);

        processor.expandMacaroon(other, CLIENT);
    }

    @Test
    public void attenuatedChildIsNotRevokedWithItsParent() throws Exception {
        String parent = mintMacaroon();

        revocations.revoke(parent);

        // a child created by adding a caveat has a different final signature ...
        String child = MacaroonsBuilder
              .modify(MacaroonsBuilder.deserialize(parent))
              .add_first_party_caveat("ip:10.20.30.40")
              .getMacaroon().serialize();

        try {
            processor.expandMacaroon(child, CLIENT);
            fail("child unexpectedly valid from this client (ip caveat should also bite)");
        } catch (InvalidMacaroonException e) {
            // the child is rejected only because of its own ip caveat, never as 'revoked'
            assertThat(e.getMessage(), not(containsString("revoked")));
        }
    }

    @Test
    public void revokedIdentityMatchesMintedId() throws Exception {
        String token = mintMacaroon();

        String identity = MacaroonRevocationList.identityOf(token);

        // dCache reports '<issueId>:<truncated-signature>' when minting; the same value must
        // appear in the revocation list so admins can correlate entries with user reports.
        assertThat(identity, containsString(":"));
        assertThat(revocations.revoke(token), equalTo(identity));
    }

    @Test
    public void shouldPruneEntriesOlderThanThePruneWindow() throws Exception {
        revocations.setPruneAfter(1);

        String token = mintMacaroon();
        revocations.revoke(token);  // freshly revoked: must survive
        revocations.injectRevokedEntry("ab", "ancient",
              System.currentTimeMillis() - java.util.concurrent.TimeUnit.DAYS.toMillis(8));

        revocations.revoke(mintMacaroon());  // triggers opportunistic pruning

        assertThat(revocations.asMap().containsKey("ab"), equalTo(false));
        assertThat(revocations.isRevoked(MacaroonRevocationList.signatureOf(token)),
              equalTo(true));
    }

    @Test
    public void shouldNeverPruneEntriesWithoutATimestamp() throws Exception {
        revocations.setPruneAfter(1);
        revocations.injectRevokedEntry("cd", "written-by-an-older-version");

        revocations.revoke(mintMacaroon());  // triggers opportunistic pruning

        assertThat(revocations.asMap().containsKey("cd"), equalTo(true));
    }

    @Test
    public void shouldReadExpiryFromMintedMacaroon() throws Exception {
        long expiry = MacaroonRevocationList.expiryOf(mintMacaroon());
        long now = System.currentTimeMillis();
        // mintMacaroon() issues one hour of validity
        assertThat(expiry > now, equalTo(true));
        assertThat(expiry < now + java.util.concurrent.TimeUnit.HOURS.toMillis(2),
              equalTo(true));
    }

    @Test
    public void shouldPruneByClaimedExpiryBeforeTheMaxWindow() throws Exception {
        revocations.setPruneAfter(java.util.concurrent.TimeUnit.DAYS.toMillis(30));

        String token = mintMacaroon();  // expires in one hour
        revocations.revoke(token);
        revocations.injectRevokedEntry("ab", "expired-already",
              System.currentTimeMillis() - java.util.concurrent.TimeUnit.HOURS.toMillis(3),
              System.currentTimeMillis() - java.util.concurrent.TimeUnit.HOURS.toMillis(2));

        revocations.revoke(mintMacaroon());  // triggers opportunistic pruning

        assertThat(revocations.asMap().containsKey("ab"), equalTo(false));
        assertThat(revocations.isRevoked(MacaroonRevocationList.signatureOf(token)),
              equalTo(true));  // its expiry is still ahead: entry survives
    }

    @Test
    public void shouldCapDoctoredFarFutureExpiryAtTheOuterWindow() throws Exception {
        revocations.setPruneAfter(1);  // outer window: revokedAt + 1ms + margin

        // A pasted (never verifiable) token claiming 100 more days of validity must
        // not keep its entry alive past the outer safety window.
        revocations.injectRevokedEntry("cd", "doctored",
              System.currentTimeMillis() - java.util.concurrent.TimeUnit.HOURS.toMillis(2),
              System.currentTimeMillis()
                    + java.util.concurrent.TimeUnit.DAYS.toMillis(100));

        revocations.revoke(mintMacaroon());  // triggers opportunistic pruning

        assertThat(revocations.asMap().containsKey("cd"), equalTo(false));
    }

    @Test
    public void shouldRefuseSixthRevocationWhenLimitedToFive() throws Exception {
        MacaroonRevocationList small = new MacaroonRevocationList();
        small.setMaxRevoked(5);

        for (int i = 0; i < 5; i++) {
            small.revoke(mintMacaroon());
        }
        try {
            small.revoke(mintMacaroon());
            fail("revocation list accepted more than its maximum");
        } catch (InvalidMacaroonException e) {
            assertThat(e.getMessage(), containsString("full"));
        }
    }

    @Test
    public void shouldRejectGarbageAsNotAMacaroon() throws Exception {
        try {
            revocations.revoke("total-garbage");
            fail("garbage accepted as a macaroon");
        } catch (InvalidMacaroonException e) {
            assertThat(e.getMessage(), containsString("Not a valid macaroon"));
        }
    }

    @Test
    public void restoreMakesMacaroonUsableAgain() throws Exception {
        String token = mintMacaroon();
        revocations.revoke(token);

        revocations.restore(token);

        processor.expandMacaroon(token, CLIENT);
    }
}
