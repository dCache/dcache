package org.dcache.webdav.federation;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.escape.Escaper;
import com.google.common.net.PercentEscaper;
import io.milton.http.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * This class represents the information passed by the client in the URL when
 * a request comes from the GlobalAccessService.  The format is documented here:
 * <p>
 * https://svnweb.cern.ch/trac/lcgdm/wiki/Dpm/WebDAV/Extensions#GlobalAccessService
 * <p>
 * Format is that the query part of a URL contains a list key-value pairs; the
 * each key and value is joined by '=' and the key-value pairs are joined by
 * '&'s.  This is the normal format used by web-forms when submitting results
 * via a GET request.
 * <p>
 * Certain keys are recognised and the values have the following semantics:
 * <pre>
 *   rid        the replica-id for the replica being requested,
 *   forbidden  a comma-list of replica-id for replicas that have been tried
 *              and that would have failed with a 403 FORBIDDEN response,
 *   notfound   a comma-list of replicas-id for replicas that have been tried
 *              and that would have failed with a 404 NOT FOUND response,
 *   r&lt;index>   a comma-separated pair of replica-id and URL, respectively.
 *              This item repeats a replica that is still to be attempted.
 * </pre>
 * <p>
 * For the {@literal r&lt;index>} fields, {@literal &lt;index>} is some
 * positive integer.  The values of {@literal r&lt;index>} do not repeat.
 * Considered together, all {@literal r&lt;index>} fields represent a stack of
 * replicas that are still to be attempted, with {@literal r1} representing the
 * next replica.
 * <p>
 * Note that creating an object is a light-weight operation.  The computational
 * effort of parsing the supplied information happens when {@code #hasNext}
 * method is called the first time.  This call must happen before
 * {@code #buildLocationWhenNotFound} or {@code #buildLocationWhenForbidden} is
 * called.
 */
public class ReplicaInfo
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaInfo.class);

    private static final ReplicaInfo EMPTY_INFO = new ReplicaInfo();
    private static final Splitter ON_FIRST_COMMA = Splitter.on(',')
            .trimResults().limit(2);

    // Equivalent to Guava's uriQueryStringEscaper(false), but this hasn't been
    // released yet.
    private static final Escaper QUERY_STRING_ESCAPER =
            new PercentEscaper("-._~!$'()*,;@:/?", false);

    // Used to escape the schema and path part of the redirected URI.
    private static final Escaper SCHEMA_AND_PATH_ESCAPER =
            new PercentEscaper("-._~!$'()*,;@:/", false);

    private final Map<String,String> _parameters;

    private boolean _isParsed;
    private String _nextReplica;
    private String _ourId;
    private List<String> _remainingReplicas = new ArrayList<>();

    public static ReplicaInfo forRequest(Request request)
    {
        Map<String,String> parameters = request.getParams();

        if (parameters == null) {
            return EMPTY_INFO;
        }

        String r1 = parameters.get("r1");

        if (isNullOrEmpty(parameters.get("rid"))
                || isNullOrEmpty(r1)
                || r1.indexOf(',') == -1) {
            LOG.trace("returning empty QueryStringInfo for request");
            return EMPTY_INFO;
        } else {
            LOG.trace("returning non-empty QueryStringInfo for request");
            return new ReplicaInfo(request);
        }
    }

    private ReplicaInfo()
    {
        _parameters = null;
        _isParsed = true;
    }

    private ReplicaInfo(Request request)
    {
        _parameters = request.getParams();
    }

    private void parseParameters()
    {
        _ourId = _parameters.get("rid");

        String replica;
        for (int index = 1;
                (replica = _parameters.get("r"+index)) != null;
                index++) {
            if (_nextReplica == null) {
                _nextReplica = replica;
            } else {
                _remainingReplicas.add(replica);
            }
        }

        _isParsed = true;
    }

    public boolean hasNext()
    {
        if (!_isParsed) {
            parseParameters();
        }

        return _ourId != null && _nextReplica != null;
    }

    private StringBuilder buildNextReplicaLocation()
    {
        StringBuilder sb = new StringBuilder();

        List<String> nextReplica =
                Lists.newArrayList(ON_FIRST_COMMA.split(_nextReplica));
        sb.append(SCHEMA_AND_PATH_ESCAPER.escape(nextReplica.get(1)));
        sb.append("?rid=").append(QUERY_STRING_ESCAPER.escape(nextReplica.get(0)));

        return sb;
    }

    private StringBuilder addRemainingReplicas(StringBuilder sb)
    {
        int index = 1;

        for(String url : this._remainingReplicas) {
            sb.append('&').append('r').append(index++);
            sb.append('=').append(QUERY_STRING_ESCAPER.escape(url));
        }

        return sb;
    }

    public String buildLocationWhenNotFound()
    {
        checkState(_isParsed && _ourId != null && _nextReplica != null);

        StringBuilder sb = buildNextReplicaLocation();

        sb.append(ampersandValueOrEmpty("forbidden"));
        sb.append('&').append(getAppendedField("notfound", _ourId));

        return addRemainingReplicas(sb).toString();
    }

    public String buildLocationWhenForbidden()
    {
        checkState(_isParsed && _ourId != null && _nextReplica != null);

        StringBuilder sb = buildNextReplicaLocation();

        sb.append('&').append(getAppendedField("forbidden", _ourId));
        sb.append(ampersandValueOrEmpty("notfound"));

        return addRemainingReplicas(sb).toString();
    }

    private CharSequence ampersandValueOrEmpty(String name)
    {
        String item = _parameters.get(name);

        if (item == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('&').append(name);
        sb.append('=').append(QUERY_STRING_ESCAPER.escape(item));
        return sb;
    }

    private CharSequence getAppendedField(String name, String item)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(name).append('=');

        String existing = _parameters.get(name);
        if (existing != null) {
            sb.append(QUERY_STRING_ESCAPER.escape(existing)).append(',');
        }

        sb.append(item);

        return sb;
    }

    @Override
    public String toString()
    {
        if (_parameters == null) {
            return "<EMPTY>";
        }

        if (!_isParsed) {
            return "<NOT PARSED>";
        }

        return "ourId=" + _ourId + ", next="+ _nextReplica + ", remaining=" +
                _remainingReplicas;
    }
}