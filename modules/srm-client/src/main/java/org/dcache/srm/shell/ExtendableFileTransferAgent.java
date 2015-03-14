package org.dcache.srm.shell;

import com.google.common.collect.ImmutableMap;
import eu.emi.security.authn.x509.X509Credential;
import org.apache.axis.types.URI;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;


/**
 * A FileTransferAgent that delegates the actual activity to some other
 * FileTransferAgent based on Java SPI.  This class also supports the
 * MultiProtocolFileTransfer interface by injecting itself as the
 * FileTransferAgent the transfer should use.
 */
public class ExtendableFileTransferAgent implements FileTransferAgent, CredentialAware
{
    private final ServiceLoader<FileTransferAgent> agents =
            ServiceLoader.load(FileTransferAgent.class);

    private ImmutableMap<String,FileTransferAgent> _protocolAgent;
    private ImmutableMap<String,Integer> _protocolPriority;
    private X509Credential _credential;

    @Override
    public void setCredential(X509Credential credential)
    {
        _credential = credential;
    }

    @Override
    public String getTransportName()
    {
        return "multi-protocol";
    }

    @Override
    public Map<String,String> getOptions()
    {
        Map<String,String> options = new HashMap<>();

        for (FileTransferAgent agent : agents) {
            String name = agent.getTransportName();

            if (name != null) {
                for (Map.Entry<String,String> e : agent.getOptions().entrySet()) {
                    options.put(name+"."+e.getKey(), e.getValue());
                }
            }
        }

        return options;
    }

    @Override
    public void setOption(String key, String value)
    {
        int index = key.indexOf('.');
        if (index == -1 || index == 0 || index == key.length()-1) {
            throw new IllegalArgumentException("Unknown key: " + key);
        }

        String transport = key.substring(0, index);

        for (FileTransferAgent agent : agents) {
            if (transport.equals(agent.getTransportName())) {
                agent.setOption(key.substring(index+1), value);
                return;
            }
        }

        throw new IllegalArgumentException("Unknown key: " + key);
    }

    @Override
    public void start()
    {
        for (FileTransferAgent agent : agents) {
            if (agent instanceof CredentialAware) {
                ((CredentialAware)agent).setCredential(_credential);
            }

            agent.start();
        }

        buildProtocols();
    }

    private void buildProtocols()
    {
        Map<String,FileTransferAgent> protocolAgent = new HashMap<>();
        Map<String,Integer> protocolPriority = new HashMap<>();

        for (FileTransferAgent agent : agents) {
            for (Map.Entry<String,Integer> e : agent.getSupportedProtocols().entrySet()) {
                String protocol = e.getKey();
                int priority = e.getValue();
                Integer existing = protocolPriority.get(protocol);
                if (existing == null || existing < priority) {
                    protocolPriority.put(protocol, priority);
                    protocolAgent.put(protocol, agent);
                }
            }
        }

        _protocolAgent = ImmutableMap.copyOf(protocolAgent);
        _protocolPriority = ImmutableMap.copyOf(protocolPriority);
    }

    @Override
    public FileTransfer download(URI source, File destination)
    {
        FileTransferAgent agent = _protocolAgent.get(source.getScheme());
        if (agent == null) {
            throw new IllegalArgumentException("Scheme " + source.getScheme() + " not supported.");
        }
        return agent.download(source, destination);
    }

    @Override
    public FileTransfer upload(File source, URI destination)
    {
        FileTransferAgent agent = _protocolAgent.get(destination.getScheme());
        if (agent == null) {
            throw new IllegalArgumentException("Scheme " + destination.getScheme() + " not supported.");
        }
        return agent.upload(source, destination);
    }

    @Override
    public void close() throws Exception
    {
        for (FileTransferAgent agent : agents) {
            agent.close();
        }
    }

    @Override
    public Map<String,Integer> getSupportedProtocols()
    {
        return _protocolPriority;
    }
}
