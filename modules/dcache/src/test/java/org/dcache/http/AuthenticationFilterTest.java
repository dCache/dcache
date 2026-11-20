/*
 * dCache - http://www.dcache.org/
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
package org.dcache.http;

import static org.dcache.http.AuthenticationFilter.DCACHE_SUBJECT_ATTRIBUTE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AuthenticationFilterTest {

    private final Map<String, Object> attributes = new HashMap<>();

    private AuthenticationFilter filter;
    private LoginStrategy loginStrategy;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private FilterChain chain;
    private Subject subjectInChain;

    @Before
    public void setUp() throws Exception {
        loginStrategy = mock(LoginStrategy.class);
        filter = new AuthenticationFilter();
        filter.setLoginStrategy(loginStrategy);
        filter.setReadOnly(false);
        filter.setEnableBasicAuthentication(true);
        filter.setRealm("dCache");

        request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("192.0.2.1");
        when(request.getRequestURI()).thenReturn("/data/file");
        when(request.getMethod()).thenReturn("GET");
        when(request.getParameterMap()).thenReturn(Map.of());
        doAnswer(i -> attributes.put(i.getArgument(0), i.getArgument(1)))
              .when(request).setAttribute(anyString(), any());
        when(request.getAttribute(anyString())).thenAnswer(i -> attributes.get(i.getArgument(0)));

        response = mock(HttpServletResponse.class);

        chain = mock(FilterChain.class);
        doAnswer(i -> subjectInChain = Subject.current()).when(chain).doFilter(any(), any());
    }

    @Test
    public void shouldRunChainAsLoggedInUser() throws Exception {
        Subject user = Subjects.of(1000, 1000, new int[]{1000});
        when(loginStrategy.login(any())).thenReturn(new LoginReply(user, Set.of()));

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(eq(request), any());
        assertThat(subjectInChain, is(sameInstance(user)));
        assertThat(attributes.get(DCACHE_SUBJECT_ATTRIBUTE), is(sameInstance(user)));
    }

    @Test
    public void shouldPassBasicCredentialsToLogin() throws Exception {
        when(request.getHeader("Authorization")).thenReturn("Basic "
              + Base64.getEncoder().encodeToString("alice:secret".getBytes(StandardCharsets.UTF_8)));
        when(loginStrategy.login(any())).thenReturn(new LoginReply());

        filter.doFilter(request, response, chain);

        ArgumentCaptor<Subject> login = ArgumentCaptor.forClass(Subject.class);
        verify(loginStrategy).login(login.capture());
        assertThat(login.getValue().getPrivateCredentials(),
              hasItem(new PasswordCredential("alice", "secret")));
    }

    @Test
    public void shouldPassSpnegoUserToLogin() throws Exception {
        filter.setEnableSpnegoAuthentication(true);
        Principal user = () -> "alice@EXAMPLE.ORG";
        when(request.getUserPrincipal()).thenReturn(user);
        when(loginStrategy.login(any())).thenReturn(new LoginReply());

        filter.doFilter(request, response, chain);

        ArgumentCaptor<Subject> login = ArgumentCaptor.forClass(Subject.class);
        verify(loginStrategy).login(login.capture());
        assertThat(login.getValue().getPrincipals(),
              hasItem(new KerberosPrincipal("alice@EXAMPLE.ORG")));
    }

    @Test
    public void shouldIgnoreUserPrincipalWithoutSpnego() throws Exception {
        when(request.getUserPrincipal()).thenReturn(() -> "alice@EXAMPLE.ORG");
        when(loginStrategy.login(any())).thenReturn(new LoginReply());

        filter.doFilter(request, response, chain);

        verify(request, never()).getUserPrincipal();
    }

    @Test
    public void shouldAnswerUnauthorizedWhenLoginIsDenied() throws Exception {
        when(loginStrategy.login(any())).thenThrow(new PermissionDeniedCacheException("denied"));

        filter.doFilter(request, response, chain);

        verify(chain, never()).doFilter(any(), any());
        verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED, "denied");
        verify(response).setHeader("WWW-Authenticate", "Basic realm=\"dCache\"");
    }

    @Test
    public void shouldAnswerInternalErrorWhenLoginFails() throws Exception {
        when(loginStrategy.login(any())).thenThrow(new CacheException("broken"));

        filter.doFilter(request, response, chain);

        verify(chain, never()).doFilter(any(), any());
        verify(response).sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldLetAnonymousAccessiblePathThroughAsNobody() throws Exception {
        filter.setAnonymousAccessiblePaths(List.of("/data/file"));
        when(loginStrategy.login(any())).thenThrow(new PermissionDeniedCacheException("denied"));

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(eq(request), any());
        assertThat(subjectInChain, is(sameInstance(Subjects.NOBODY)));
    }

    @Test
    public void shouldAddChallengeWhenChainAnswersUnauthorized() throws Exception {
        when(loginStrategy.login(any())).thenReturn(new LoginReply());
        doAnswer(i -> {
            ((HttpServletResponse) i.getArgument(1)).sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        }).when(chain).doFilter(any(), any());

        filter.doFilter(request, response, chain);

        verify(response).setHeader("WWW-Authenticate", "Basic realm=\"dCache\"");
        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test
    public void shouldRunChainAsSubjectOfSession() throws Exception {
        Subject user = new Subject();
        user.getPrincipals().add(new LoginNamePrincipal("alice"));
        HttpSession session = mock(HttpSession.class);
        when(session.getAttribute("subject")).thenReturn(user);
        when(request.getSession(false)).thenReturn(session);

        filter.doFilter(request, response, chain);

        verify(loginStrategy, never()).login(any());
        assertThat(subjectInChain, is(sameInstance(user)));
    }

    @Test
    public void shouldAnswerNotFoundForSessionWithoutSubject() throws Exception {
        when(request.getSession(false)).thenReturn(mock(HttpSession.class));

        filter.doFilter(request, response, chain);

        verify(chain, never()).doFilter(any(), any());
        verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
    }
}
