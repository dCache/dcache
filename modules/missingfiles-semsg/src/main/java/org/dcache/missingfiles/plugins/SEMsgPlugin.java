package org.dcache.missingfiles.plugins;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.dcache.auth.FQAN;
import org.dcache.auth.Subjects;
import org.dcache.util.ConfigurationProperties;
import org.dcache.util.FireAndForgetTask;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Missing-files plugin for sending notification that access was attempted to
 * a file.  We use an external program, SEMsg_SendNotAvailable to send the actual
 * messages.  This plugin collects the messages and sends them in batches.
 */
public class SEMsgPlugin implements Plugin
{
    private static final Logger _log =
            LoggerFactory.getLogger(SEMsgPlugin.class);

    private Executor _executor = Executors.newSingleThreadExecutor();

    private static final String PROPERTY_TOPIC = "missing-files.plugins.semsg.broker.topic";
    private static final String PROPERTY_ENDPOINT = "missing-files.plugins.semsg.broker.endpoint";
    private static final String PROPERTY_COMMAND = "missing-files.plugins.semsg.command";
    private static final String PROPERTY_CERTIFICATE = "missing-files.plugins.semsg.certificate";
    private static final String PROPERTY_PRIVATE_KEY = "missing-files.plugins.semsg.private-key";

    private static final Future<Result> DEFER = new FinalResult(Result.DEFER);

    private String _topic;
    private String _endpoint;
    private String _command;
    private String _certificatePath;
    private String _privateKeyPath;

    public SEMsgPlugin(ConfigurationProperties properties)
    {
        _topic = getRequiredProperty(properties, PROPERTY_TOPIC);
        _endpoint = getRequiredProperty(properties, PROPERTY_ENDPOINT);
        _command = getRequiredProperty(properties, PROPERTY_COMMAND);
        _certificatePath = getRequiredProperty(properties, PROPERTY_CERTIFICATE);
        _privateKeyPath = getRequiredProperty(properties, PROPERTY_PRIVATE_KEY);
    }

    private static String getRequiredProperty(ConfigurationProperties properties, String key)
    {
        String value = properties.getValue(key);

        if(value == null) {
            throw new IllegalArgumentException("missing property: " + key);
        }

        return value;
    }


    @Override
    public Future<Result> accept(final Subject subject,
            final String requestPath, final String internalPath)
    {
        _executor.execute(new FireAndForgetTask(() -> sendMessage(subject, requestPath)));

        return DEFER;
    }


    private void sendMessage(Subject subject, String path)
    {
        String authDn = buildAuthDnFor(subject);
        try {
            int rc = runCommand(authDn, path);

            if(rc != 0) {
                _log.error("call to command failed: rc={}", rc);
            }
        } catch (InterruptedException e) {
            _log.debug("Interrupted while sending notification");
        } catch (IOException e) {
            _log.error("{}", e.getMessage());
        }
    }

    private String buildAuthDnFor(Subject subject)
    {
        StringBuilder sb = new StringBuilder();


        String dn = Subjects.getDn(subject);
        if(dn != null) {
            sb.append(dn);

            for(FQAN fqan : Subjects.getFqans(subject)) {
                sb.append(',').append(fqan);
            }
        } else {
            sb.append("<UNKNOWN>");
        }

        return sb.toString();
    }


    private int runCommand(String authDn, String path) throws IOException, InterruptedException
    {
        List<String> args = newArrayList(_command, _endpoint, authDn,
                "-t", _topic, path);

        ProcessBuilder builder = new ProcessBuilder(args);

        _log.debug("Command: {}", builder.command());

        Map<String,String> envar = builder.environment();
        envar.put("X509_USER_CERT", _certificatePath);
        envar.put("X509_USER_KEY", _privateKeyPath);

        Process process = builder.start();

        return process.waitFor();
    }
}
