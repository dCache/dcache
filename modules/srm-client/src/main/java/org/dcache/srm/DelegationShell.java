/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.srm;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.axis.SimpleTargetedChain;
import org.apache.axis.configuration.SimpleProvider;
import org.bouncycastle.jce.PKCS10CertificationRequest;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PEMWriter;
import org.globus.axis.transport.HTTPSSender;
import org.globus.axis.util.Util;
import org.globus.common.CoGProperties;
import org.globus.gsi.CredentialException;
import org.globus.gsi.GSIConstants.CertificateType;
import org.globus.gsi.X509Credential;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;

import javax.xml.rpc.ServiceException;
import javax.xml.rpc.holders.StringHolder;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Callable;

import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.delegation.gridsite1.DelegationExceptionType;
import org.dcache.delegation.gridsite1.NewProxyReq;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.delegation.gridsite2.DelegationServiceLocator;
import org.dcache.util.Args;
import org.dcache.util.cli.ShellApplication;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * A client for interacting with a GridSite delegation endpoint that supports
 * both scriptable and interactive usage.  This shell supports both v1.1.0 and
 * v2.0.0 endpoints.  Theoretically it also supports v1.0.0
 * endpoints as these have the same XML namespace as v1.1.0 and all v1.0.0
 * requests are also v1.1.0 requests; however this hasn't been tested.
 */
public class DelegationShell extends ShellApplication
{
    private enum GridSiteVersion
    {
        V1_1_0,
        V2_0_0;
    }

    /**
     * The policy when given an endpoint: should we assume a GridSite version
     * or try to probe which version is supported.  The two PROBE options
     * differ only if an endpoint supports both versions.
     */
    private enum GridSiteVersionPolicy
    {
        ASSUME_V1,
        ASSUME_V2,
        PROBE;
    }

    private final DelegationServiceLocator _v2Locator;
    private final org.dcache.delegation.gridsite1.DelegationServiceLocator _v1Locator;
    private final SimpleProvider _provider = new SimpleProvider();
    private String _prompt = "$ ";
    private Delegation _client;
    private GridSiteVersionPolicy _versionPolicy = GridSiteVersionPolicy.PROBE;

    public static void main(String[] arguments) throws Throwable
    {
        Args args = new Args(arguments);

        try (DelegationShell shell = new DelegationShell()) {
            if (args.hasOption("endpoint")) {
                shell.setEndpoint(URI.create(args.getOption("endpoint")));
                args = args.removeOptions("endpoint");
            }

            shell.start(args);
        } catch (RuntimeException e) {
            System.err.println("Bug detected: " + e.toString());
            e.printStackTrace(System.err);
        } catch (Exception e) {
            System.err.println(e.toString());
        }
    }

    public DelegationShell() throws Throwable
    {
        _provider.deployTransport("https", new SimpleTargetedChain(new HTTPSSender()));
        Util.registerTransport();
        _v1Locator = new org.dcache.delegation.gridsite1.DelegationServiceLocator(_provider);
        _v2Locator = new DelegationServiceLocator(_provider);
    }

    @Override
    protected String getCommandName()
    {
        return "delegation";
    }

    @Override
    protected String getPrompt()
    {
        return _prompt;
    }


    private org.dcache.delegation.gridsite1.Delegation buildV1Client(URL url)
    {
        try {
            return _v1Locator.getGridsiteDelegation(url);
        } catch (ServiceException e) {
            // Should never happen
            throw new RuntimeException("Failed to create v1 client: " +
                    e.toString(), e);
        }
    }

    private org.dcache.delegation.gridsite2.Delegation buildV2Client(URL url)
    {
        try {
            return _v2Locator.getGridsiteDelegation(url);
        } catch (ServiceException e) {
            // Should never happen
            throw new RuntimeException("Failed to create v1 client: " +
                    e.toString(), e);
        }
    }

    /**
     * Test whether the supplied endpoint supports GridSite v1.1.0.  One problem
     * is that (broken) implementations will simply close a connection after
     * a request if the remote server doesn't like the credential used when
     * initialising the SSL connection.
     */
    private boolean isEndpointV1(URL url) throws CommandException
    {
        org.dcache.delegation.gridsite1.DelegationServiceLocator locator =
                new org.dcache.delegation.gridsite1.DelegationServiceLocator(_provider);
        try {
            org.dcache.delegation.gridsite1.Delegation client =
                    locator.getGridsiteDelegation(url);
            client.getNewProxyReq();
            return true;
        } catch (ServiceException e) {
            throw new RuntimeException("Problem in isEndpointV1: " +
                    e.getMessage(), e);
        } catch (RemoteException e) {
            throwIfProblemWithUrl(e);
            throwIfProblemLocalEnvironment(e);
            return false;
        }
    }

    private boolean isEndpointV2(URL url) throws CommandException
    {
        org.dcache.delegation.gridsite2.DelegationServiceLocator locator =
                new org.dcache.delegation.gridsite2.DelegationServiceLocator(_provider);
        try {
            org.dcache.delegation.gridsite2.Delegation client =
                    locator.getGridsiteDelegation(url);
            client.getVersion();
            return true;
        } catch (ServiceException e) {
            throw new RuntimeException("Problem in isEndpointV2: " +
                    e.getMessage(), e);
        } catch (RemoteException e) {
            throwIfProblemWithUrl(e);
            throwIfProblemLocalEnvironment(e);
        }
        return false;
    }

    private Delegation buildClient(URL url) throws CommandException
    {
        switch (_versionPolicy)
        {
        case ASSUME_V1:
            return new Delegation(buildV1Client(url));
        case ASSUME_V2:
            return new Delegation(buildV2Client(url));
        case PROBE:
            if (isEndpointV2(url)) {
                return new Delegation(buildV2Client(url));
            } else if (isEndpointV1(url)) {
                return new Delegation(buildV1Client(url));
            } else {
                throw new CommandException("endpoint seems to be " +
                        "neither v1.1.0 nor v2.0.0; use \"set endpoint " +
                        "policy\" to force using a particular version.");
            }
        }

        throw new CommandException("Unknown version policy");
    }

    private void setEndpoint(URI uri) throws CommandException
    {
        uri = checkSyntax(uri);
        String newPrompt = "[" + uri + "] $ ";
        uri = canonicalise(uri);

        try {
            _client = buildClient(uri.toURL());
            _prompt = newPrompt;
        } catch (MalformedURLException e) {
            throw new CommandException("Bad URI: " + e.toString());
        }
    }

    private URI checkSyntax(URI uri) throws IllegalArgumentException
    {
        String schema = uri.getScheme();

        checkArgument(schema != null, "Missing schema: URI should start 'https://', 'srm:', 'fts:' or 'cream:'");

        switch (schema.toLowerCase()) {
        case "https":
            checkArgument(uri.getHost() != null, "URIs must include a host part");
            checkArgument(uri.getPath() != null && !uri.getPath().equals(""), "URIs must have a path");
            break;
        case "srm":
        case "fts":
        case "cream":
        case "dpm":
            if (uri.getPath() != null) {
                checkArgument(uri.getPath().isEmpty(), "URIs should not specify a path");
                checkArgument(uri.getHost() != null, "URIs must include a host part");
                checkArgument(uri.getPort() == -1, "URIs should not specify the port");
            }
            break;

        default:
            throw new IllegalArgumentException("URI should start 'https://', 'srm:', 'fts:', 'cream:' or 'dpm:'");
        }

        return uri;
    }

    private static void throwIfProblemWithUrl(Exception e) throws CommandException
    {
        Throwable t = Throwables.getRootCause(e);

        if (t instanceof UnknownHostException) {
            throw new CommandException("Unknown host: " + t.getMessage());
        }

        if (t instanceof ConnectException) {
            throw new CommandException("Failed to connect to endpoint: " + t.getMessage());
        }
    }

    private static void throwIfProblemLocalEnvironment(Exception e) throws CommandException
    {
        Throwable t = Throwables.getRootCause(e);

        if (t instanceof CredentialException) {
            throw new CommandException("Problem with local credental: " + t.getMessage());
        }
    }

    private static void throwAsGenericProblem(Exception e) throws CommandException
    {
        if (e instanceof DelegationException || e instanceof DelegationExceptionType) {
            throw new CommandException("Remote server said: " + e.toString());
        } else {
            Throwable t = Throwables.getRootCause(e);
            StringBuilder sb = new StringBuilder();
            sb.append("Problem communicating with endpoint: ");
            if (!(t instanceof RuntimeException)) {
                sb.append(t.getClass().getCanonicalName()).append(": ");
            }
            sb.append(t.getMessage());
            throw new CommandException(sb.toString());
        }
    }

    private static CommandException asCommandException(Exception cause)
    {
        try {
            throwIfProblemWithUrl(cause);
            throwIfProblemLocalEnvironment(cause);
            throwAsGenericProblem(cause);
        } catch (CommandException e) {
            return e;
        }

        return new CommandException("Unexpected message: " + cause.getMessage());
    }

    private void checkHaveEndpoint() throws CommandException
    {
        if (_client == null) {
            throw new CommandException("No endpoint specified; use the 'endpoint' command");
        }
    }

    private URI canonicalise(URI uri)
    {
        String portAndPath;

        switch (uri.getScheme().toLowerCase()) {
        case "https":
            int port = uri.getPort();
            if (port == 443) {
                port = -1;
            }
            portAndPath = (port == -1 ? "" : (":" + port)) + uri.getPath();
            break;
        case "srm":
            portAndPath = ":8445/srm/delegation";
            break;
        case "fts":
            portAndPath = ":8443/glite-data-transfer-fts/services/gridsite-delegation";
            break;
        case "cream":
            portAndPath = ":8443/ce-cream/services/gridsite-delegation";
            break;
        case "dpm":
            portAndPath = ":443/gridsite-delegation";
            break;
        default:
            throw new RuntimeException("Unexpected URI scheme: " + uri.getScheme());
        }

        String host = uri.getPath() == null ? uri.getSchemeSpecificPart() : uri.getHost();
        return URI.create("https://" + host + portAndPath);
    }

    @Command(name = "endpoint policy", hint="manage version discovery policy",
        description="This command shows and controls how a new endpoint is handled. " +
            "If no argument is specified then the current policy is shown and " +
            "the policy is updated if an argument is specified.\n" +
            "\n"+
            "There are two major versions of the GridSite protocol in use: " +
            "v1.1.0 and v2.0.0.  These different versions are broadly similar " +
            "but incompatible.  Both versions are supported, but this " +
            "client needs to know which version to use for a given endpoint.\n" +
            "\n" +
            "Without any argument, the 'endpoint policy' command shows the " +
            "current policy: the short name followed by a brief description.  " +
            "If the short name is 'assume-v1' or 'assume-v2' then the next " +
            "'endpoint' command will assume the endpoint is either " +
            "GridSite v1.1.0 or v2.0.0 respectively; subsequent commands " +
            "will fail if GridSite version is incorrect.  If the short name " +
            "is 'probe' then the client will try a GridSite v2.0.0 command " +
            "and a v1.1.0 command to discover which version the endpoint " +
            "supports.  If neither command is successful then the 'set " +
            "endpoint' command will fail.\n" +
            "\n" +
            "With an argument, this command will update the policy.  The new " +
            "policy will only affect subsequent calls to 'endpoint'; the " +
            "current endpoint (if any) is not affected\n" +
            "\n" +
            "NOTE: some implementations of GridSite fail unauthorised " +
            "requests in a way indestinguishable from an endpoint that " +
            "does not supporting GridSite at all.  This can lead to the " +
            "'probe' policy failing during the 'endpoint' command.")
    public class EndpointPolicy implements Callable<Serializable>
    {
        @Argument(usage="Updated policy for new connections.",
                valueSpec="assume-v1|assume-v2|probe", required=false)
        public String policy;

        @Override
        public Serializable call() throws Exception
        {
            if (policy == null) {
                switch (_versionPolicy) {
                case ASSUME_V1:
                    return "assume-v1 -- connect to endpoint as GridSite v1.1.0";
                case ASSUME_V2:
                    return "assume-v2 -- connect to endpoint as GridSite v2.0.0";
                case PROBE:
                    return "probe -- try GridSite commands to identify version";
                }

                throw new RuntimeException ("Unknown policy: " + _versionPolicy);
            } else {
                switch (policy)
                {
                case "assume-v1":
                    _versionPolicy = GridSiteVersionPolicy.ASSUME_V1;
                    return null;
                case "assume-v2":
                    _versionPolicy = GridSiteVersionPolicy.ASSUME_V2;
                    return null;
                case "probe":
                    _versionPolicy = GridSiteVersionPolicy.PROBE;
                    return null;
                default:
                    throw new CommandException("Unknown policy '" + policy +
                            "'.  Should be assume-v1, assume-v2 or probe");
                }
            }
        }
    }

    @Command(name = "get version", hint="query software version",
            description="Query the remote server to discover which version " +
                    "of the software it is running.  This command is only " +
                    "available to GridSite v2.0.0 endpoints.")
    public class GetVersionCommand implements Callable<Serializable>
    {
        @Override
        public Serializable call() throws CommandException
        {
            checkHaveEndpoint();

            try {
                return _client.getVersion();
            } catch (RemoteException e) {
                throw asCommandException(e);
            }
        }
    }

    @Command(name = "get interface version", hint = "query delegation version",
            description = "query the remote server to discover which version " +
                    "of the GridSite interface it is providing.  This command " +
                    "is only available to GridSite v2.0.0 endpoints.")
    public class GetInterfaceVersionCommand implements Callable<Serializable>
    {
        @Override
        public Serializable call() throws CommandException
        {
            checkHaveEndpoint();

            try {
                return _client.getInterfaceVersion();
            } catch (RemoteException e) {
                throw asCommandException(e);
            }
        }
    }

    @Command(name = "get service metadata", hint = "query arbitrary service metadata",
            description = "Query the remote server to discover the value " +
                    "corresponding to the supplied key.  This command is only " +
                    "available to GridSite v2.0.0 endpoints.")
    public class GetServiceMetadataCommand implements Callable<Serializable>
    {
        @Argument(usage="Key for the requested information.")
        String key;

        @Override
        public Serializable call() throws CommandException
        {
            checkHaveEndpoint();

            try {
                return _client.getServiceMetadata(key);
            } catch (RemoteException e) {
                throw asCommandException(e);
            }
        }
    }

    @Command(name = "delegate", hint="delegate a credential",
            description = "Delegate the current credential to the server.  " +
                    "There are two forms of this command: client-supplied-id " +
                    "and server-supplied-id.  In either case, the delegated " +
                    "credential will have an ID that may be used later to " +
                    "trigger the credential's destruction.\n" +
                    "\n" +
                    "Invoking this command and specifying an ID as the argument " +
                    "triggers the client-supplied-id form.  The server will " +
                    "store the credential against this ID, allowing it to be " +
                    "destroyed later on.\n" +
                    "\n" +
                    "For a server-supplied ID, invoke the command without " +
                    "any arguments.  The remote server will choose an ID for " +
                    "the delegated credential and this ID will be reported, " +
                    "allowing manual triggering of the credential's destruction.\n" +
                    "\n" +
                    "The lifetime of the delegated credential may be limited.  " +
                    "By default delegated credentials are limited to 24 hours " +
                    "but this lifetime may be extended or shortened using the " +
                    "-lifetime option.  Independent of this option, the " +
                    "delegated credential's lifetime is always limited to that " +
                    "of the proxy certificate.")
    public class DelegateCommand implements Callable<Serializable>
    {
        @Argument(usage="Id of delegated credential; if omitted the server will choose one.",
                required=false)
        String id;

        @Option(name="lifetime", usage="Desired lifetime of delegated credential in seconds.")
        Integer lifetime = 60*60*24;

        @Override
        public Serializable call() throws Exception
        {
            checkHaveEndpoint();

            String csrData;
            String id = this.id;

            try {
                if (id == null) {
                    IdAndRequest result = _client.getNewProxyReq();
                    csrData = result.request;
                    id = result.id;
                } else {
                    csrData = _client.getProxyReq(id);
                }
                PKCS10CertificationRequest csr = fromPEM(csrData);
                String path = CoGProperties.getDefault().getProxyFile();
                X509Credential credential = new X509Credential(path);

                lifetime = limitLifetime(credential.getCertificateChain(), lifetime);

                X509Certificate certificate = createCertificate(credential, csr, lifetime);
                String result = toPEM(concat(certificate, credential.getCertificateChain()));
                _client.putProxy(id, result);
            } catch (RemoteException e) {
                throw asCommandException(e);
            }

            if (this.id == null) {
                console.printString("Delegated credential has id " + id + "\n");
            }

            return null;
        }

        private int limitLifetime(X509Certificate[] certificates, int lifetime) throws IOException
        {
            Date expiry = new Date(System.currentTimeMillis() + lifetime*1000);

            for (X509Certificate certificate : certificates) {
                if (certificate.getNotAfter().before(expiry)) {
                    expiry = certificate.getNotAfter();
                    console.printString("Generated certificate expire " + expiry + "\n");
                }
            }

            return (int)((expiry.getTime() - System.currentTimeMillis()) / 1000);
        }

        private X509Certificate createCertificate(X509Credential proxy, PKCS10CertificationRequest csr, int lifetime)
                throws NoSuchAlgorithmException, NoSuchProviderException,
                InvalidKeyException, IOException, CertificateEncodingException,
                SignatureException, CredentialException, GeneralSecurityException
        {
            PublicKey pubKey = csr.getPublicKey();
            BouncyCastleCertProcessingFactory factory =
                    BouncyCastleCertProcessingFactory.getDefault();

            return factory.createProxyCertificate(proxy.getCertificateChain()[0],
                    proxy.getPrivateKey(), pubKey, lifetime,
                    CertificateType.GSI_4_INDEPENDENT_PROXY, null, null);
        }

        private Iterable<X509Certificate> concat(X509Certificate certificate,
                X509Certificate[] existingChain)
        {
            return Iterables.concat(Collections.singleton(certificate),
                    Arrays.asList(existingChain));
        }

        private String toPEM(Iterable<X509Certificate> certificates) throws IOException
        {
            StringWriter output = new StringWriter();
            PEMWriter writer = new PEMWriter(output);
            for (X509Certificate certificate : certificates) {
                writer.writeObject(certificate);
            }
            writer.flush();
            return output.toString();
        }

        private PKCS10CertificationRequest fromPEM(String data) throws IOException
        {
            PEMReader reader = new PEMReader(new StringReader(data));
            return (PKCS10CertificationRequest)reader.readObject();
        }
    }

    @Command(name="destroy", hint="destroy a delegated credential",
            description = "This command contacts the endpoint and requests " +
                    "that the credential delegated against the supplied ID " +
                    "is destroyed.  After being destroyed, the server cannot " +
                    "undertake any further activity using this credential.")
    public class DestroyCommand implements Callable<Serializable>
    {
        @Argument(usage="Id of delegated credential.")
        String id;

        @Override
        public Serializable call() throws CommandException
        {
            checkHaveEndpoint();

            try {
                _client.destroy(id);
            } catch (RemoteException e) {
                throw asCommandException(e);
            }

            return null;
        }
 }

    @Command(name = "get termination time", hint="query when delegated credential expiries",
            description = "This command will query the remote server to " +
                    "discover when the delegated credential will expire.")
    public class GetTerminationTime implements Callable<Serializable>
    {
        @Argument
        String id;

        @Override
        public Serializable call() throws CommandException
        {
            checkHaveEndpoint();

            Date termination;

            try {
                termination = _client.getTerminationTime(id).getTime();
            } catch (RemoteException e) {
                throw asCommandException(e);
            }

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String relative;
            long remaining = (termination.getTime() - System.currentTimeMillis())/1000;
            if (remaining <= 0) {
                relative = "expired";
            } else if (remaining < 120 ) {
                relative = remaining + " seconds";
            } else if (remaining < 7200 ) {
                relative = (remaining / 60) + " minutes";
            } else if (remaining < 172800 ) {
                relative = (remaining / 3600) + " hours";
            } else {
                relative = (remaining / 86400) + " days";
            }
            return df.format(termination) + " (" + relative + ")";
        }
    }

    @Command(name = "endpoint",
            hint = "specify which GridSite Delegation endpoint to use",
            description = "Specify the endpoint that subsequent commands " +
                    "will be directed towards.  There are two forms for specifying " +
                    "a URI: a generic URI and a software-specific URI.\n" +
                    "\n" +
                    "The generic URI has the form 'https://HOST[:PORT]PATH' " +
                    "where HOST is the hostname of the server, PORT is the " +
                    "port number (port 443 is used if not specified) and PATH " +
                    "is the path to the delegation service.  " +
                    "'https://delegation.example.org:8445/services/delegation' " +
                    "is an example of a generic URI.\n" +
                    "\n" +
                    "There are several software-specific URIs that make it " +
                    "easier to contact a specific software.  All software-" +
                    "specific URIs may be written 'TYPE://HOST' or 'TYPE:HOST' " +
                    "where TYPE is the kind of software and HOST is the " +
                    "hostname of the endpoint.  Note that no port number or " +
                    "path is supplied as the correct values are added " +
                    "automatically.  Currently supported values of TYPE are " +
                    "'srm' 'dpm' 'cream' and 'fts'.\n" +
                    "\n" +
                    "When this command is called, the GridSite version policy " +
                    "is followed.  This may involve assuming the endpoint is " +
                    "a particular version or probing the endpoint to discover " +
                    "which version it supports.  If the endpoint is probed " +
                    "and supports neither version then the 'endpoint' command " +
                    "will fail.  The policy is managed with the " +
                    "'endpoint policy' command.")
    public class EndpointCommand implements Callable<Serializable>
    {
        @Argument(usage="Delegation endpoint.", metaVar="URI")
        String uri;

        @Override
        public Serializable call() throws CommandException, IOException
        {
            try {
                setEndpoint(URI.create(uri));
            } catch (IllegalArgumentException | IllegalStateException e) {
                throw new CommandException(e.getMessage());
            }

            return null;
        }
    }

    /**
     * An class that represents the delegation endpoint that provides either
     * GridSite v1.1.0 or v2.0.0 behaviour.  If a request is version specific
     * and that version isn't being used then a CommandException is thrown.
     */
    private static class Delegation
    {
        private final org.dcache.delegation.gridsite1.Delegation _v1Client;
        private final org.dcache.delegation.gridsite2.Delegation _v2Client;

        private Delegation(org.dcache.delegation.gridsite1.Delegation client)
        {
            _v1Client = client;
            _v2Client = null;
        }

        private Delegation(org.dcache.delegation.gridsite2.Delegation client)
        {
            _v1Client = null;
            _v2Client = client;
        }

        public GridSiteVersion getGridSiteVersion()
        {
            return (_v2Client == null) ? GridSiteVersion.V1_1_0 : GridSiteVersion.V2_0_0;
        }

        public String getVersion() throws CommandException, RemoteException
        {
            guardVersion(GridSiteVersion.V2_0_0, "only supported for v2.0.0 endpoints");

            return _v2Client.getVersion();
        }

        public String getProxyReq(String id) throws RemoteException
        {
            if (_v1Client != null) {
                return _v1Client.getProxyReq(id);
            } else {
                return _v2Client.getProxyReq(id);
            }
        }

        public void putProxy(String id, String chain) throws RemoteException
        {
            if (_v1Client != null) {
                _v1Client.putProxy(id, chain);
            } else {
                _v2Client.putProxy(id, chain);
            }
        }

        public void destroy(String id) throws RemoteException
        {
            if (_v1Client != null) {
                _v1Client.destroy(id);
            } else {
                _v2Client.destroy(id);
            }
        }

        public IdAndRequest getNewProxyReq() throws RemoteException
        {
            if (_v1Client != null) {
                NewProxyReq result = _v1Client.getNewProxyReq();
                return new IdAndRequest(result.getDelegationID(), result.getProxyRequest());
            } else {
                StringHolder req = new StringHolder();
                StringHolder id = new StringHolder();
                _v2Client.getNewProxyReq(req, id);
                return new IdAndRequest(id.value, req.value);
            }
        }

        public String getInterfaceVersion() throws RemoteException, CommandException
        {
            guardVersion(GridSiteVersion.V2_0_0, "only supported for v2.0.0 endpoints");

            return _v2Client.getInterfaceVersion();
        }

        public String getServiceMetadata(String key) throws RemoteException,
                CommandException
        {
            guardVersion(GridSiteVersion.V2_0_0, "only supported for v2.0.0 endpoints");

            return _v2Client.getServiceMetadata(key);
        }

        public Calendar getTerminationTime(String id) throws CommandException,
                RemoteException
        {
            guardVersion(GridSiteVersion.V2_0_0, "only supported for v2.0.0 endpoints");

            return _v2Client.getTerminationTime(id);
        }

        private void guardVersion(GridSiteVersion version, String message)
                throws CommandException
        {
            if (version != getGridSiteVersion()) {
                throw new CommandException(message);
            }
        }
    }

    /**
     * Simple class to hold the server-generated delegation ID along with
     * the corresponding Certificate Signing Request.
     */
    private static class IdAndRequest
    {
        private final String id;
        private final String request;

        private IdAndRequest(String delegationId, String certificateSigningRequest)
        {
            id = delegationId;
            request = certificateSigningRequest;
        }
    }
}
