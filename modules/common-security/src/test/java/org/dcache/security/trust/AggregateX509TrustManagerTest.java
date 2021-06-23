/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.security.trust;

import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;

import javax.net.ssl.X509TrustManager;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AggregateX509TrustManagerTest
{
    private X509TrustManager manager;
    private List<X509TrustManager> inner;

    @Before
    public void setup()
    {
        manager = null;
    }

    @Test(expected=NullPointerException.class)
    public void shouldThrowNpeIfConstructedWithNull()
    {
        new AggregateX509TrustManager(null);
    }

    @Test(expected=CertificateException.class)
    public void shouldRejectClientWithEmptyManager() throws Exception
    {
        givenTrustManagers();

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkClientTrusted(chain, "TLS");
    }

    @Test
    public void shouldAcceptClientWithSingleAcceptingManager() throws Exception
    {
        givenTrustManagers(aTrustManager());

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkClientTrusted(chain, "TLS");

        verify(inner.get(0)).checkClientTrusted(chain, "TLS");
        verify(inner.get(0), never()).checkServerTrusted(any(), any());
        verify(inner.get(0), never()).getAcceptedIssuers();
    }

    @Test
    public void shouldAcceptServerWithSingleAcceptingManager() throws Exception
    {
        givenTrustManagers(aTrustManager());

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkServerTrusted(chain, "TLS");

        verify(inner.get(0), never()).checkClientTrusted(any(), any());
        verify(inner.get(0)).checkServerTrusted(chain, "TLS");
        verify(inner.get(0), never()).getAcceptedIssuers();
    }

    @Test(expected=CertificateException.class)
    public void shouldRejectClientWithSingleRejectingManager() throws Exception
    {
        givenTrustManagers(aTrustManager().thatFailsClientsWith(new CertificateException()));

        manager.checkClientTrusted(new X509Certificate[0], "TLS");
    }

    @Test(expected=CertificateException.class)
    public void shouldRejectServerWithSingleRejectingManager() throws Exception
    {
        givenTrustManagers(aTrustManager().thatFailsServerWith(new CertificateException()));

        manager.checkServerTrusted(new X509Certificate[0], "TLS");
    }

    @Test
    public void shouldReturnAcceptedIssuersFromSingleManager() throws Exception
    {
        X509Certificate issuer = mock(X509Certificate.class);
        givenTrustManagers(aTrustManager().thatAcceptsIssuers(issuer));

        X509Certificate[] issuers = manager.getAcceptedIssuers();

        verify(inner.get(0), never()).checkClientTrusted(any(), any());
        verify(inner.get(0), never()).checkServerTrusted(any(), any());
        verify(inner.get(0)).getAcceptedIssuers();
        assertThat(issuers, is(arrayContaining(issuer)));
    }

    @Test
    public void shouldAcceptClientWithTwoAcceptingManager() throws Exception
    {
        givenTrustManagers(aTrustManager(), aTrustManager());

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkClientTrusted(chain, "TLS");

        verify(inner.get(0)).checkClientTrusted(chain, "TLS");
        verify(inner.get(0), never()).checkServerTrusted(any(), any());
        verify(inner.get(0), never()).getAcceptedIssuers();
        verify(inner.get(1), never()).checkServerTrusted(any(), any());
        verify(inner.get(1), never()).getAcceptedIssuers();
    }

    @Test
    public void shouldAcceptServerWithTwoAcceptingManager() throws Exception
    {
        givenTrustManagers(aTrustManager(), aTrustManager());

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkServerTrusted(chain, "TLS");

        verify(inner.get(0), never()).checkClientTrusted(any(), any());
        verify(inner.get(0)).checkServerTrusted(chain, "TLS");
        verify(inner.get(0), never()).getAcceptedIssuers();
        verify(inner.get(1), never()).checkClientTrusted(any(), any());
        verify(inner.get(1), never()).getAcceptedIssuers();
    }

    @Test
    public void shouldAcceptClientWithARejectingManagerAndAcceptingManager() throws Exception
    {
        givenTrustManagers(aTrustManager().thatFailsClientsWith(new CertificateException("msg")),
                aTrustManager());

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkClientTrusted(chain, "TLS");

        verify(inner.get(0)).checkClientTrusted(chain, "TLS");
        verify(inner.get(0), never()).checkServerTrusted(any(), any());
        verify(inner.get(0), never()).getAcceptedIssuers();
        verify(inner.get(1)).checkClientTrusted(chain, "TLS");
        verify(inner.get(1), never()).checkServerTrusted(any(), any());
        verify(inner.get(1), never()).getAcceptedIssuers();
    }

    @Test
    public void shouldAcceptServerWithARejectingManagerAndAcceptingManager() throws Exception
    {
        givenTrustManagers(aTrustManager().thatFailsServerWith(new CertificateException("msg")),
                aTrustManager());

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkServerTrusted(chain, "TLS");

        verify(inner.get(0), never()).checkClientTrusted(any(), any());
        verify(inner.get(0)).checkServerTrusted(chain, "TLS");
        verify(inner.get(0), never()).getAcceptedIssuers();
        verify(inner.get(1), never()).checkClientTrusted(any(), any());
        verify(inner.get(1)).checkServerTrusted(chain, "TLS");
        verify(inner.get(1), never()).getAcceptedIssuers();
    }

    @Test
    public void shouldAcceptClientWithAnAcceptingManagerAndRejectingManager() throws Exception
    {
        givenTrustManagers(aTrustManager(),
                aTrustManager().thatFailsClientsWith(new CertificateException("msg")));

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkClientTrusted(chain, "TLS");

        verify(inner.get(0)).checkClientTrusted(chain, "TLS");
        verify(inner.get(0), never()).checkServerTrusted(any(), any());
        verify(inner.get(0), never()).getAcceptedIssuers();
        verify(inner.get(1), never()).checkServerTrusted(any(), any());
        verify(inner.get(1), never()).getAcceptedIssuers();
    }

    @Test
    public void shouldAcceptServerWithAnAcceptingManagerAndRejectingManager() throws Exception
    {
        givenTrustManagers(aTrustManager(),
                aTrustManager().thatFailsServerWith(new CertificateException("msg")));

        X509Certificate[] chain = new X509Certificate[0];
        manager.checkServerTrusted(chain, "TLS");

        verify(inner.get(0)).checkServerTrusted(chain, "TLS");
        verify(inner.get(0), never()).checkClientTrusted(any(), any());
        verify(inner.get(0), never()).getAcceptedIssuers();
        verify(inner.get(1), never()).checkClientTrusted(any(), any());
        verify(inner.get(1), never()).getAcceptedIssuers();
    }

    @Test(expected=CertificateException.class)
    public void shouldRejectClientWithTwoRejectingManagers() throws Exception
    {
        givenTrustManagers(aTrustManager().thatFailsClientsWith(new CertificateException("msg1")),
                aTrustManager().thatFailsClientsWith(new CertificateException("msg2")));

        manager.checkClientTrusted(new X509Certificate[0], "TLS");
    }

    @Test(expected=CertificateException.class)
    public void shouldRejectServerWithTwoRejectingManagers() throws Exception
    {
        givenTrustManagers(aTrustManager().thatFailsServerWith(new CertificateException("msg1")),
                aTrustManager().thatFailsServerWith(new CertificateException("msg2")));

        manager.checkServerTrusted(new X509Certificate[0], "TLS");
    }

    @Test
    public void shouldReturnAcceptedIssuersFromTwoManagers() throws Exception
    {
        X509Certificate issuer1 = mock(X509Certificate.class);
        X509Certificate issuer2 = mock(X509Certificate.class);
        givenTrustManagers(aTrustManager().thatAcceptsIssuers(issuer1),
                aTrustManager().thatAcceptsIssuers(issuer2));

        X509Certificate[] issuers = manager.getAcceptedIssuers();

        verify(inner.get(0), never()).checkClientTrusted(any(), any());
        verify(inner.get(0), never()).checkServerTrusted(any(), any());
        verify(inner.get(0)).getAcceptedIssuers();
        verify(inner.get(1), never()).checkClientTrusted(any(), any());
        verify(inner.get(1), never()).checkServerTrusted(any(), any());
        verify(inner.get(1)).getAcceptedIssuers();
        assertThat(issuers, is(arrayContainingInAnyOrder(issuer1, issuer2)));
    }

    private void givenTrustManagers(MockX509TrustManagerBuilder... builders)
    {
        inner = Arrays.stream(builders)
                .map(MockX509TrustManagerBuilder::build)
                .collect(Collectors.toList());

        manager = new AggregateX509TrustManager(inner);
    }

    private MockX509TrustManagerBuilder aTrustManager()
    {
        return new MockX509TrustManagerBuilder();
    }

    /**
     * Fluent builder for mocking X509TrustManager
     */
    private static class MockX509TrustManagerBuilder
    {
        private final X509TrustManager manager = mock(X509TrustManager.class);

        public MockX509TrustManagerBuilder thatFailsClientsWith(CertificateException e)
        {
            try {
                BDDMockito.willThrow(e).given(manager).checkClientTrusted(any(), any());
            } catch (CertificateException e1) {
                throw new RuntimeException(e1);
            }
            return this;
        }

        public MockX509TrustManagerBuilder thatFailsServerWith(CertificateException e)
        {
            try {
                BDDMockito.willThrow(e).given(manager).checkServerTrusted(any(), any());
            } catch (CertificateException e1) {
                throw new RuntimeException(e1);
            }
            return this;
        }

        public MockX509TrustManagerBuilder thatAcceptsIssuers(X509Certificate... issuers)
        {
            BDDMockito.given(manager.getAcceptedIssuers()).willReturn(issuers);
            return this;
        }

        public X509TrustManager build()
        {
            return manager;
        }
    }

}
