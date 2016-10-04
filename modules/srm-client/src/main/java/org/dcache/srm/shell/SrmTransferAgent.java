package org.dcache.srm.shell;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.axis.types.URI;
import org.apache.axis.types.UnsignedLong;

import javax.annotation.Nonnull;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRMException;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTGetFileRequest;
import org.dcache.srm.v2_2.ArrayOfTGetRequestFileStatus;
import org.dcache.srm.v2_2.ArrayOfTPutFileRequest;
import org.dcache.srm.v2_2.ArrayOfTPutRequestFileStatus;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.SrmPrepareToGetRequest;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.SrmPutDoneRequest;
import org.dcache.srm.v2_2.SrmPutDoneResponse;
import org.dcache.srm.v2_2.SrmReleaseFilesRequest;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TAccessPattern;
import org.dcache.srm.v2_2.TConnectionType;
import org.dcache.srm.v2_2.TDirOption;
import org.dcache.srm.v2_2.TGetFileRequest;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TPutFileRequest;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TTransferParameters;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.dcache.srm.shell.TStatusCodes.checkSuccess;

/**
 *  Support for transferring a file using SRM.
 */
public class SrmTransferAgent extends AbstractFileTransferAgent
{
    private final ScheduledExecutorService _srmExecutor = Executors.newSingleThreadScheduledExecutor();

    private ISRM srm;
    private FileTransferAgent agent;
    private ImmutableList<String> protocols;

    public void setSrm(ISRM srm)
    {
        this.srm = checkNotNull(srm);
    }

    public void setFileTransferAgent(FileTransferAgent agent)
    {
        this.agent = checkNotNull(agent);
    }

    @Nonnull
    @Override
    public Map<String,String> getOptions()
    {
        return agent.getOptions();
    }

    /**
     * Alter an option.
     */
    @Override
    public void setOption(String key, String value)
    {
        agent.setOption(key, value);
    }


    protected synchronized ImmutableList<String> getProtocolPreference()
    {
        if (protocols == null) {
            protocols = buildProtocolPreference(agent);
        }

        return protocols;
    }

    private static ImmutableList<String> buildProtocolPreference(FileTransferAgent agent)
    {
        Ordering<Map.Entry<String,Integer>> order = Ordering.natural().
                onResultOf(new Function<Map.Entry<String,Integer>, Integer>(){
                    @Override
                    public Integer apply(Map.Entry<String,Integer> input)
                    {
                        return input.getValue();
                    }
                }).reverse();

        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (Map.Entry<String,Integer> entry : order.sortedCopy(agent.getSupportedProtocols().entrySet())) {
            builder.add(entry.getKey());
        }

        return builder.build();
    }

    @Override
    public void close() throws Exception
    {
        MoreExecutors.shutdownAndAwaitTermination(_srmExecutor, 500, TimeUnit.MILLISECONDS);
        agent.close();
    }

    @Override
    public FileTransfer download(URI source, File destination)
    {
        if (source.getScheme().equals("srm")) {
            GetTransfer transfer = new GetTransfer(source, destination);
            transfer.start();
            return transfer;
        }

        return null;
    }

    @Override
    public FileTransfer upload(File source, URI destination)
    {
        if (destination.getScheme().equals("srm")) {
            PutTransfer transfer = new PutTransfer(source, destination);
            transfer.start();
            return transfer;
        }

        return null;
    }

    @Override
    public String getTransportName()
    {
        return "srm";
    }

    /**
     * Base class for uploads and downloads.
     */
    abstract private class AbstractSRMTransfer extends AbstractFileTransfer
    {
        private final URI _surl;
        private final File _localFile;

        protected String _description = "awaiting initiation";
        private String _token;
        private FileTransfer _transfer;

        AbstractSRMTransfer(URI surl, File localFile)
        {
            _surl = surl;
            _localFile = localFile;

            Futures.addCallback(this, new FutureCallback(){
                @Override
                public void onSuccess(Object result)
                {
                }

                @Override
                public void onFailure(Throwable t)
                {
                    if (t instanceof CancellationException) {
                        onCancel();
                    }
                }
            });
        }

        private void onCancel()
        {
            FileTransfer transfer = getTransfer();
            if (transfer != null) {
                transfer.cancel(true);
            } else {
                abortRequest();
            }
        }

        @Override
        public String getStatus()
        {
            StringBuilder sb = new StringBuilder();

            sb.append(_surl).append(": ");

            if (_transfer == null) {
                sb.append(_description);
            } else {
                sb.append(_transfer.getStatus());
            }

            return sb.toString();
        }

        public ArrayOfString getProtocolPreference()
        {
            ImmutableList<String> protocols = SrmTransferAgent.this.getProtocolPreference();
            return new ArrayOfString(protocols.toArray(new String[protocols.size()]));
        }

        public void setTransfer(FileTransfer transfer)
        {
            _transfer = transfer;
        }

        public FileTransfer getTransfer()
        {
            return _transfer;
        }

        protected void setRequestToken(String token)
        {
            _token = token;
        }

        public String getRequestToken()
        {
            return _token;
        }

        public URI getSurl()
        {
            return _surl;
        }

        public File getLocalFile()
        {
            return _localFile;
        }

        public void setDescription(String description)
        {
            _description = description;
        }

        protected void fail(RemoteException e)
        {
            setDescription("Problem contacting remote server : " + e.toString());
        }

        protected void fail(SRMException e)
        {
            setDescription(e.getMessage() == null ? "Unspecified problem" : e.getMessage());
        }

        protected void abortRequest()
        {
            String token = getRequestToken();
            if (token != null) {
                try {
                    SrmAbortRequestRequest request = new SrmAbortRequestRequest(token, null);
                    SrmAbortRequestResponse response = srm.srmAbortRequest(request);
                    checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
                } catch (RemoteException e) {
                    setDescription(_description + ", failed to abort request: " +
                            e.getMessage());
                } catch (SRMException e) {
                    setDescription(_description + ", failed to abort request: " +
                            "[" + e.getStatusCode() + "] " + e.getMessage());
                }
            }
        }

        class FailOnBugTask implements Runnable
        {
            private final Runnable _inner;

            FailOnBugTask(Runnable inner)
            {
                _inner = inner;
            }

            @Override
            public void run()
            {
                try {
                    _inner.run();
                } catch (RuntimeException e) {
                    setDescription("bug detected: " + e.toString());
                    e.printStackTrace();
                }
            }
        }


    }

    /**
     * Class for handling downloads specifically.
     */
    public class PutTransfer extends AbstractSRMTransfer
    {
        public PutTransfer(File source, URI target)
        {
            super(target, source);
        }

        private void start()
        {
            _srmExecutor.execute(new FailOnBugTask(new Runnable(){
                @Override
                public void run()
                {
                    prepareToPut();
                }
            }));
        }

        private void prepareToPut()
        {
            setDescription("requesting transfer URL.");

            ArrayOfTPutFileRequest fileRequests = new ArrayOfTPutFileRequest(new TPutFileRequest[]{
                new TPutFileRequest(getSurl(), new UnsignedLong(getLocalFile().length())),
            });
            TTransferParameters params = new TTransferParameters(TAccessPattern.TRANSFER_MODE,
                    TConnectionType.WAN, null, getProtocolPreference());

            // FIXME: make target ALRP configurable
            TRetentionPolicyInfo retention = new TRetentionPolicyInfo(TRetentionPolicy.REPLICA,
                    TAccessLatency.ONLINE);

            // FIXME: add user-description to allow rediscovery of ongoing requests
            // FIXME: add overwrite option
            // FIXME: add space-token option
            SrmPrepareToPutRequest request = new SrmPrepareToPutRequest(
                    null, fileRequests, null, TOverwriteMode.NEVER, null,
                    (int)TimeUnit.DAYS.toSeconds(1), null, null, null, null,
                    retention, params);

            try {
                SrmPrepareToPutResponse response = srm.srmPrepareToPut(request);
                setRequestToken(response.getRequestToken());


                TStatusCode requestStatus = response.getReturnStatus().getStatusCode();
                if (requestStatus != TStatusCode.SRM_REQUEST_QUEUED &&
                        requestStatus != TStatusCode.SRM_REQUEST_INPROGRESS &&
                        requestStatus != TStatusCode.SRM_SUCCESS) {
                    if (response.getArrayOfFileStatuses() != null) {
                        TPutRequestFileStatus file = getFileStatus(response.getArrayOfFileStatuses());
                        throw new SRMException(file.getStatus().getExplanation());
                    } else {
                        throw new SRMException(response.getReturnStatus().getExplanation());
                    }
                }

                TPutRequestFileStatus file = getFileStatus(response.getArrayOfFileStatuses());

                TStatusCode requestStatusCode = response.getReturnStatus().getStatusCode();

                if (requestStatusCode == TStatusCode.SRM_REQUEST_QUEUED ||
                        requestStatusCode == TStatusCode.SRM_REQUEST_INPROGRESS) {

                    TStatusCode fileStatusCode = file.getStatus().getStatusCode();
                    if (fileStatusCode != requestStatusCode) {
                        throw new SRMException("mismatch between file and response status: " +
                                fileStatusCode + " != " + requestStatusCode);
                    }

                    setDescription("waiting to request status update.");
                    int delay = max(min(file.getEstimatedWaitTime(), 60), 1);
                    _srmExecutor.schedule(new FailOnBugTask(new Runnable() {
                        @Override
                        public void run()
                        {
                            statusOfPrepareToPut();
                        }
                    }), delay, TimeUnit.SECONDS);
                } else {
                    checkSuccess(file.getStatus(), TStatusCode.SRM_SPACE_AVAILABLE);
                    initiateTransfer(file.getTransferURL());
                }
            } catch (RemoteException e) {
                fail(e);
                abortRequest();
                failed(e);
            } catch (SRMException e) {
                fail(e);
                abortRequest();
                failed(e);
            }
        }

        private void statusOfPrepareToPut()
        {
            setDescription("request status update.");
            SrmStatusOfPutRequestRequest request =
                    new SrmStatusOfPutRequestRequest(getRequestToken(), null,
                            new ArrayOfAnyURI(new URI[]{getSurl()}));
            try {
                SrmStatusOfPutRequestResponse response = srm.srmStatusOfPutRequest(request);

                checkSuccess(response.getReturnStatus(), TStatusCode.SRM_REQUEST_QUEUED,
                        TStatusCode.SRM_REQUEST_INPROGRESS, TStatusCode.SRM_SUCCESS);

                TStatusCode requestStatusCode = response.getReturnStatus().getStatusCode();
                TPutRequestFileStatus file = getFileStatus(response.getArrayOfFileStatuses());

                if (requestStatusCode == TStatusCode.SRM_REQUEST_QUEUED ||
                        requestStatusCode == TStatusCode.SRM_REQUEST_INPROGRESS) {
                    TStatusCode fileStatusCode = file.getStatus().getStatusCode();
                    if (fileStatusCode != requestStatusCode) {
                        throw new SRMException("mismatch between file and response status: " +
                                fileStatusCode + " != " + requestStatusCode);
                    }

                    setDescription("waiting to request status update.");

                    int delay = Math.max(Math.min(file.getEstimatedWaitTime(), 60), 1);
                    _srmExecutor.schedule(new FailOnBugTask(new Runnable() {
                        @Override
                        public void run()
                        {
                            statusOfPrepareToPut();
                        }
                    }), delay, TimeUnit.SECONDS);
                } else {
                    checkSuccess(file.getStatus(), TStatusCode.SRM_SPACE_AVAILABLE);
                    initiateTransfer(file.getTransferURL());
                }
            } catch (RemoteException e) {
                fail(e);
                abortRequest();
                failed(e);
            } catch (SRMException e) {
                fail(e);
                abortRequest();
                failed(e);
            }
        }

        private void initiateTransfer(URI turl)
        {
            FileTransfer transfer = agent.upload(getLocalFile(), turl);

            if (transfer == null) {
                setDescription("unable to transfer from " + turl);
                abortRequest();
            } else {
                setDescription("preparing for transfer using " + turl);
                setTransfer(transfer);
                Futures.addCallback(transfer, new FutureCallback<Void>(){
                    @Override
                    public void onSuccess(Void result)
                    {
                        setDescription("file transfer complete.");
                        setTransfer(null);
                        putDone();
                        succeeded();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        setDescription(t.getMessage());
                        setTransfer(null);
                        abortRequest();
                        failed(t);
                    }
                }, _srmExecutor);
            }
        }

        private void putDone()
        {
            SrmPutDoneRequest request = new SrmPutDoneRequest(getRequestToken(),
                    null, new ArrayOfAnyURI(new URI[]{getSurl()}));

            try {
                SrmPutDoneResponse response = srm.srmPutDone(request);
                checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
                TSURLReturnStatus status = getSURLStatus(response.getArrayOfFileStatuses());
                checkSuccess(status.getStatus(), TStatusCode.SRM_SUCCESS);
            } catch (RemoteException e) {
                setDescription(_description + ", failed to release lock: " +
                        e.getMessage());
            } catch (SRMException e) {
                setDescription(_description + ", failed to finalise upload: " +
                        "[" + e.getStatusCode() + "] " + e.getMessage());
            }
        }

        @Nonnull
        private TPutRequestFileStatus getFileStatus(ArrayOfTPutRequestFileStatus container) throws SRMException
        {
            checkForFailure(container != null, "bad reply: null array-container of RequestFileStatus.");

            TPutRequestFileStatus[] statusArray = container.getStatusArray();

            checkForFailure(statusArray != null, "bad reply: null RequestFileStatus array.");

            if (statusArray.length != 1) {
                throw new SRMException("bad reply: RequestFileStatus array with length " + statusArray.length + ".");
            }

            TPutRequestFileStatus status = statusArray[0];

            checkForFailure(status != null, "bad reply: RequestFileStatus array with null element.");

            return status;
        }

        @Nonnull
        private TSURLReturnStatus getSURLStatus(ArrayOfTSURLReturnStatus container) throws SRMException
        {
            checkForFailure(container != null, "bad reply: null array-container of SURLReturnStatus.");

            TSURLReturnStatus[] statusArray = container.getStatusArray();

            checkForFailure(statusArray != null, "bad reply: null SURLReturnStatus.");

            if (statusArray.length != 1) {
                throw new SRMException("bad reply: SURLReturnStatus array with length " + statusArray.length + ".");
            }

            TSURLReturnStatus status = statusArray[0];

            checkForFailure(status != null, "bad reply: SURLReturnStatus array will null element");

            return status;
        }
    }


    /**
     * Class for handling downloads specifically.
     */
    public class GetTransfer extends AbstractSRMTransfer
    {
        public GetTransfer(URI source, File target)
        {
            super(source, target);
        }

        private void start()
        {
            _srmExecutor.execute(new FailOnBugTask(new Runnable(){
                @Override
                public void run()
                {
                    prepareToGet();
                }
            }));
        }

        private void prepareToGet()
        {
            setDescription("requesting transfer URL.");

            ArrayOfTGetFileRequest fileRequests = new ArrayOfTGetFileRequest(new TGetFileRequest[]{
                new TGetFileRequest(getSurl(), new TDirOption(false, false, 0)),
            });
            TTransferParameters params = new TTransferParameters(TAccessPattern.TRANSFER_MODE,
                    TConnectionType.WAN, null, getProtocolPreference());

            // FIXME: add user-description to allow rediscovery of ongoing requests
            SrmPrepareToGetRequest request = new SrmPrepareToGetRequest(
                    null, fileRequests, null, null, null, (int)TimeUnit.DAYS.toSeconds(1), -1, null,
                    null, params);

            try {
                SrmPrepareToGetResponse response = srm.srmPrepareToGet(request);
                setRequestToken(response.getRequestToken());

                checkSuccess(response.getReturnStatus(), TStatusCode.SRM_REQUEST_QUEUED,
                        TStatusCode.SRM_REQUEST_INPROGRESS, TStatusCode.SRM_SUCCESS);

                TGetRequestFileStatus file = getFileStatus(response.getArrayOfFileStatuses());

                TStatusCode requestStatusCode = response.getReturnStatus().getStatusCode();

                if (requestStatusCode == TStatusCode.SRM_REQUEST_QUEUED ||
                        requestStatusCode == TStatusCode.SRM_REQUEST_INPROGRESS) {

                    TStatusCode fileStatusCode = file.getStatus().getStatusCode();
                    if (fileStatusCode != requestStatusCode) {
                        throw new SRMException("mismatch between file and response status: " +
                                fileStatusCode + " != " + requestStatusCode);
                    }

                    setDescription("waiting to request status update.");
                    int delay = max(min(file.getEstimatedWaitTime(), 60), 1);
                    _srmExecutor.schedule(new FailOnBugTask(new Runnable() {
                        @Override
                        public void run()
                        {
                            statusOfPrepareToGet();
                        }
                    }), delay, TimeUnit.SECONDS);
                } else {
                    checkSuccess(file.getStatus(), TStatusCode.SRM_FILE_PINNED);
                    initiateTransfer(file.getTransferURL());
                }
            } catch (RemoteException e) {
                fail(e);
                releaseFiles();
                failed(e);
            } catch (SRMException e) {
                fail(e);
                releaseFiles();
                failed(e);
            }
        }

        @Nonnull
        private TGetRequestFileStatus getFileStatus(ArrayOfTGetRequestFileStatus fileStatus) throws SRMException
        {
            checkForFailure(fileStatus != null, "bad reply: null array-container of RequestFileStatus.");

            TGetRequestFileStatus[] statuses = fileStatus.getStatusArray();

            checkForFailure(statuses != null, "bad reply: null RequestFileStatus array.");

            if (statuses.length != 1) {
                throw new SRMException("bad reply: RequestFileStatus array with length " + statuses.length + ".");
            }

            TGetRequestFileStatus status = statuses[0];

            checkForFailure(status != null, "bad reply: RequestFileStatus array with null element.");

            return status;
        }

        private void statusOfPrepareToGet()
        {
            setDescription("request status update.");
            SrmStatusOfGetRequestRequest request =
                    new SrmStatusOfGetRequestRequest(getRequestToken(), null,
                            new ArrayOfAnyURI(new URI[]{getSurl()}));
            try {
                SrmStatusOfGetRequestResponse response = srm.srmStatusOfGetRequest(request);

                checkSuccess(response.getReturnStatus(), TStatusCode.SRM_REQUEST_QUEUED,
                        TStatusCode.SRM_REQUEST_INPROGRESS, TStatusCode.SRM_SUCCESS);

                TStatusCode requestStatusCode = response.getReturnStatus().getStatusCode();
                TGetRequestFileStatus file = getFileStatus(response.getArrayOfFileStatuses());

                if (requestStatusCode == TStatusCode.SRM_REQUEST_QUEUED ||
                        requestStatusCode == TStatusCode.SRM_REQUEST_INPROGRESS) {
                    TStatusCode fileStatusCode = file.getStatus().getStatusCode();
                    if (fileStatusCode != requestStatusCode) {
                        throw new SRMException("mismatch between file and response status: " +
                                fileStatusCode + " != " + requestStatusCode);
                    }

                    setDescription("waiting to request status update.");

                    int delay = Math.max(Math.min(file.getEstimatedWaitTime(), 60), 1);
                    _srmExecutor.schedule(new FailOnBugTask(new Runnable() {
                        @Override
                        public void run()
                        {
                            statusOfPrepareToGet();
                        }
                    }), delay, TimeUnit.SECONDS);
                } else {
                    checkSuccess(file.getStatus(), TStatusCode.SRM_FILE_PINNED);
                    initiateTransfer(file.getTransferURL());
                }
            } catch (RemoteException e) {
                fail(e);
                releaseFiles();
                failed(e);
            } catch (SRMException e) {
                fail(e);
                releaseFiles();
                failed(e);
            }
        }

        private void initiateTransfer(URI turl)
        {
            FileTransfer transfer = agent.download(turl, getLocalFile());

            if (transfer == null) {
                setDescription("unable to transfer from " + turl);
                releaseFiles();
            } else {
                setDescription("preparing for transfer using " + turl);
                setTransfer(transfer);
                Futures.addCallback(transfer, new FutureCallback<Void>(){
                    @Override
                    public void onSuccess(Void result)
                    {
                        setDescription("file transfer complete.");
                        setTransfer(null);
                        releaseFiles();
                        succeeded();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        setDescription(t.getMessage());
                        setTransfer(null);
                        releaseFiles();
                        failed(t);
                    }
                }, _srmExecutor);
            }
        }

        private void releaseFiles()
        {
            if (getRequestToken() == null) {
                return;
            }

            String transferDescription = _description;

            String description = transferDescription;
            if (description.endsWith(".")) {
                description = description.substring(0, description.length()-1);
            }
            setDescription(description + ", finalising transfer");

            SrmReleaseFilesRequest request = new SrmReleaseFilesRequest(getRequestToken(),
                    null, new ArrayOfAnyURI(new URI[]{getSurl()}), false);

            try {
                SrmReleaseFilesResponse response = srm.srmReleaseFiles(request);
                checkSuccess(response.getReturnStatus(), TStatusCode.SRM_SUCCESS);
                setDescription(transferDescription);
            } catch (SRMException e) {
                setDescription(_description + " but failed to release lock: " +
                        "[" + e.getStatusCode() + "] " + e.getMessage());
            } catch (RemoteException e) {
                setDescription(_description + " but failed to release lock: " +
                        e.getMessage());
            }
        }
    }

    private void checkForFailure(boolean isOk, String message) throws SRMException
    {
        if (!isOk) {
            throw new SRMException(message);
        }
    }
}
