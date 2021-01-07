package org.dcache.webdav.federation;

 import io.milton.http.Request;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class QueryStringInfoTests
{

    @Test
    public void shouldNotHaveNextWhenNoParameters()
    {
        Map<String,String> parameters = mock(Map.class);
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(false));
    }

    @Test
    public void shouldNotHaveNextWhenOnlyRid()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("5");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(false));
    }

    @Test
    public void shouldNotHaveNextWhenOnlyR1()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("r1")).willReturn("7,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(false));
    }

    @Test
    public void shouldWorkWhenNotFoundAndFirstInStackWithOneExtraReplica()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("5");
        given(parameters.get("r1")).willReturn("7,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenNotFound(),
                is("http://www.example.org/a/file?rid=7&notfound=5"));
    }

    @Test
    public void shouldWorkWhenForbiddenAndFirstInStackWithOneExtraReplica()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenForbidden(),
                is("http://www.example.org/a/file?rid=4&forbidden=9"));
    }

    @Test
    public void shouldWorkWhenNotFoundAndAlreadyNotFoundWithOneExtra()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("notfound")).willReturn("1");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenNotFound(),
                is("http://www.example.org/a/file?rid=4&notfound=1,9"));
    }

    @Test
    public void shouldWorkWhenForbiddenAndAlreadyMissingWithOneExtra()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("notfound")).willReturn("1");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenForbidden(),
                is("http://www.example.org/a/file?rid=4&forbidden=9&notfound=1"));
    }

    @Test
    public void shouldWorkWhenNotFoundAndAlreadyForbiddenWithOneExtra()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("forbidden")).willReturn("1");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenNotFound(),
                is("http://www.example.org/a/file?rid=4&forbidden=1&notfound=9"));
    }

    @Test
    public void shouldWorkWhenForbiddenAndAlreadyForbiddenWithOneExtra()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("forbidden")).willReturn("1");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenForbidden(),
                is("http://www.example.org/a/file?rid=4&forbidden=1,9"));
    }

    @Test
    public void shouldWorkWhenForbiddenAndAlreadyNotFoundAndForbiddenWithOneExtra()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("forbidden")).willReturn("1");
        given(parameters.get("notfound")).willReturn("2");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenForbidden(),
                is("http://www.example.org/a/file?rid=4&forbidden=1,9&notfound=2"));
    }

    @Test
    public void shouldWorkWhenNotFoundAndAlreadyNotFoundAndForbiddenWithOneExtra()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("forbidden")).willReturn("1");
        given(parameters.get("notfound")).willReturn("2");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenNotFound(),
                is("http://www.example.org/a/file?rid=4&forbidden=1&notfound=2,9"));
    }

    @Test
    public void shouldWorkWhenNotFoundAndTwoExtra()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        given(parameters.get("r2")).willReturn("6,http://www.example.com/my/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenNotFound(),
                is("http://www.example.org/a/file?rid=4&notfound=9&"
                        + "r1=6,http://www.example.com/my/file"));
    }

    @Test
    public void shouldWorkWhenNotFoundAndThreeExtra()
    {
        Map<String,String> parameters = mock(Map.class);
        given(parameters.get("rid")).willReturn("9");
        given(parameters.get("r1")).willReturn("4,http://www.example.org/a/file");
        given(parameters.get("r2")).willReturn("6,http://www.example.com/my/file");
        given(parameters.get("r3")).willReturn("13,http://www.example.net/another/file");
        Request request = mock(Request.class);
        given(request.getParams()).willReturn(parameters);

        ReplicaInfo info = ReplicaInfo.forRequest(request);

        assertThat(info.hasNext(), is(true));
        assertThat(info.buildLocationWhenNotFound(),
                is("http://www.example.org/a/file?rid=4&notfound=9&"
                        + "r1=6,http://www.example.com/my/file&"
                        + "r2=13,http://www.example.net/another/file"));
    }
}
