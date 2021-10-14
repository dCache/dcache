/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import org.junit.Test;

public class LoAPrincipalTest {

    /**
     * Base64 encoded serialisation of LoAPrincipal(LoA.IGTF_LOA_CEDER).
     */
    private static final String SERIALIZED_IGTF_LOA_CEDER =
          "rO0ABXNyABxvcmcuZGNhY2hlLmF1dGguTG9BUHJpbmNpcGFsAAAAAAAAAAECAAFM"
                + "AARfbG9hdAAVTG9yZy9kY2FjaGUvYXV0aC9Mb0E7eHB+cgATb3JnLmRjYWNoZS"
                + "5hdXRoLkxvQQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAAS"
                + "AAB4cHQADklHVEZfTE9BX0NFREVS";

    /**
     * Base64 encoded serialisation of LoAPrincipal(LoA.IGTF_LOA_CEDAR).
     */
    private static final String SERIALIZED_IGTF_LOA_CEDAR =
          "rO0ABXNyABxvcmcuZGNhY2hlLmF1dGguTG9BUHJpbmNpcGFsAAAAAAAAAAECAAFM"
                + "AARfbG9hdAAVTG9yZy9kY2FjaGUvYXV0aC9Mb0E7eHB+cgATb3JnLmRjYWNoZS"
                + "5hdXRoLkxvQQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAAS"
                + "AAB4cHQADklHVEZfTE9BX0NFREFS";

    private byte[] serialise(Object target) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(target);
        out.close();
        return bytes.toByteArray();
    }

    private <T> T deserialise(byte[] data, Class<T> expectedType)
          throws IOException, ClassNotFoundException {
        ByteArrayInputStream bytes = new ByteArrayInputStream(data);
        ObjectInputStream in = new ObjectInputStream(bytes);
        Object rawObject = in.readObject();
        return expectedType.cast(rawObject);
    }

    private String serialiseToString(Object target) throws IOException {
        byte[] data = serialise(target);
        return Base64.getEncoder().withoutPadding().encodeToString(data);
    }

    private <T> T deserialiseFromString(String data, Class<T> expectedType)
          throws IOException, ClassNotFoundException {
        byte[] rawData = Base64.getDecoder().decode(data);
        return deserialise(rawData, expectedType);
    }

    private LoAPrincipal serialiseRoundTrip(LoAPrincipal in)
          throws IOException, ClassNotFoundException {
        byte[] data = serialise(in);
        return deserialise(data, LoAPrincipal.class);
    }

    @Test
    public void shouldRoundTripClassic() throws Exception {
        LoAPrincipal classic = new LoAPrincipal(LoA.IGTF_AP_CLASSIC);

        LoAPrincipal roundTripClassic = serialiseRoundTrip(classic);

        assertThat(roundTripClassic, equalTo(classic));
    }

    @Test
    public void shouldRoundTripCedar() throws Exception {
        LoAPrincipal cedar = new LoAPrincipal(LoA.IGTF_LOA_CEDAR);

        LoAPrincipal roundTripClassic = serialiseRoundTrip(cedar);

        assertThat(roundTripClassic, equalTo(cedar));
    }

    @Test
    public void shouldRoundTripCederAsCedar() throws Exception {
        LoAPrincipal ceder = new LoAPrincipal(LoA.IGTF_LOA_CEDER);

        LoAPrincipal roundTrip = serialiseRoundTrip(ceder);

        assertThat(roundTrip, not(equalTo(ceder)));
        assertThat(roundTrip.getLoA(), equalTo(LoA.IGTF_LOA_CEDAR));
    }

    @Test
    public void shouldSerialiseCedarAsCedar() throws Exception {
        String serialised = serialiseToString(new LoAPrincipal(LoA.IGTF_LOA_CEDAR));

        assertThat(serialised, equalTo(SERIALIZED_IGTF_LOA_CEDAR));
    }

    @Test
    public void shouldSerialiseCederAsCedar() throws Exception {
        String serialised = serialiseToString(new LoAPrincipal(LoA.IGTF_LOA_CEDER));

        assertThat(serialised, equalTo(SERIALIZED_IGTF_LOA_CEDAR));
    }

    @Test
    public void shouldDeserialiseCedarAsCedar() throws Exception {
        LoAPrincipal deserialisedPrincipal = deserialiseFromString(SERIALIZED_IGTF_LOA_CEDAR,
              LoAPrincipal.class);

        assertThat(deserialisedPrincipal, equalTo(new LoAPrincipal(LoA.IGTF_LOA_CEDAR)));
    }

    @Test
    public void shouldDeserialiseCederAsCedar() throws Exception {
        LoAPrincipal deserialisedPrincipal = deserialiseFromString(SERIALIZED_IGTF_LOA_CEDER,
              LoAPrincipal.class);

        assertThat(deserialisedPrincipal, equalTo(new LoAPrincipal(LoA.IGTF_LOA_CEDAR)));
    }
}
