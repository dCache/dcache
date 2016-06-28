package org.dcache.pool.repository.v3.entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.entry.state.Sticky;

public class CacheRepositoryEntryState
{
    private static final Logger _log = LoggerFactory.getLogger(CacheRepositoryEntryState.class);

    private static final Pattern VERSION_PATTERN =
        Pattern.compile("#\\s+version\\s+[0-9]\\.[0-9]");

    // format version
    private static final int FORMAT_VERSION_MAJOR = 3;
    private static final int FORMAT_VERSION_MINOR = 0;

    // possible states of entry in the repository

    private final Sticky _sticky = new Sticky();
    private ReplicaState _state;

    // data, control and SI- files locations
    private final Path _controlFile;

    public CacheRepositoryEntryState(Path controlFile) throws IOException {
        _controlFile = controlFile;
        _state = ReplicaState.NEW;

        // read state from file
        try {
            loadState();
        }catch( FileNotFoundException | NoSuchFileException fnf) {
            /*
             * it's not an error state.
             */
        }
    }

    public List<StickyRecord> removeExpiredStickyFlags() throws IOException
    {
        List<StickyRecord> removed = _sticky.removeExpired();
        if (!removed.isEmpty()) {
            makeStatePersistent();
        }
        return removed;
    }

    public void setState(ReplicaState state)
        throws IOException
    {
        if (state == _state) {
            return;
        }

        switch (state) {
        case NEW:
            throw new IllegalStateException("Entry is " + _state);
        case FROM_CLIENT:
            if (_state != ReplicaState.NEW) {
                throw new IllegalStateException("Entry is " + _state);
            }
            break;
        case FROM_STORE:
            if (_state != ReplicaState.NEW) {
                throw new IllegalStateException("Entry is " + _state);
            }
            break;
        case FROM_POOL:
            if (_state != ReplicaState.NEW) {
                throw new IllegalStateException("Entry is " + _state);
            }
            break;
        case CACHED:
            if (_state == ReplicaState.REMOVED ||
                _state == ReplicaState.DESTROYED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            break;
        case PRECIOUS:
            if (_state == ReplicaState.REMOVED ||
                _state == ReplicaState.DESTROYED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            break;
        case BROKEN:
            if (_state == ReplicaState.REMOVED ||
                _state == ReplicaState.DESTROYED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            break;
        case REMOVED:
            if (_state == ReplicaState.DESTROYED) {
                throw new IllegalStateException("Entry is " + _state);
            }
            break;
        case DESTROYED:
            if (_state != ReplicaState.REMOVED) {
                throw new IllegalStateException("Entry is " + _state);
            }
        }

        _state = state;
        makeStatePersistent();
    }

    public ReplicaState getState()
    {
        return _state;
    }

    /*
     *
     *  State transitions
     *
     */

    public boolean setSticky(String owner, long expire, boolean overwrite)
        throws IllegalStateException, IOException
    {
        if (_state == ReplicaState.REMOVED || _state == ReplicaState.DESTROYED) {
            throw new IllegalStateException("Entry in removed state");
        }

        // if sticky flag modified, make changes persistent
        if (_sticky.addRecord(owner, expire, overwrite)) {
            makeStatePersistent();
            return true;
        }
        return false;
    }

    public boolean isSticky()
    {
        return _sticky.isSet();
    }

    /**
     * store state in control file
     * @throws IOException
     */
    private void makeStatePersistent() throws IOException
    {

        //BufferedReader in = new BufferedReader( new FileReader(_controlFile) );
        try (BufferedWriter out = Files.newBufferedWriter(_controlFile)) {

            // write repository version number

            out.write("# version 3.0");
            out.newLine();

            switch (_state) {
            case PRECIOUS:
                out.write("precious");
                out.newLine();
                break;
            case CACHED:
                out.write("cached");
                out.newLine();
                break;
            case FROM_CLIENT:
                out.write("from_client");
                out.newLine();
                break;
            case FROM_STORE:
            case FROM_POOL:
                out.write("from_store");
                out.newLine();
                break;
            }

            String state = _sticky.stringValue();
            if (state != null && state.length() > 0) {
                out.write(state);
                out.newLine();
            }

            out.flush();

        }

    }

    private void loadState() throws IOException
    {
        try (BufferedReader in = Files.newBufferedReader(_controlFile)) {
            _state = ReplicaState.BROKEN;

            String line;
            while ((line = in.readLine()) != null) {

                // ignore empty lines
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }

                // a comment or version string
                if (line.startsWith("#")) {
                    Matcher m = VERSION_PATTERN.matcher(line);

                    // it's the version string
                    if (m.matches()) {
                        String[] versionLine = line.split("\\s");
                        String[] versionNumber = versionLine[2].split("\\.");

                        int major = Integer.parseInt(versionNumber[0]);
                        int minor = Integer.parseInt(versionNumber[1]);

                        if (major > FORMAT_VERSION_MAJOR || minor != FORMAT_VERSION_MINOR) {
                            throw new IOException("control file format mismatch: supported <= "
                                    + FORMAT_VERSION_MAJOR + "." + FORMAT_VERSION_MINOR + " found: " + versionLine[2]);
                        }
                    }

                    continue;
                }

                if (line.equals("precious")) {
                    _state = ReplicaState.PRECIOUS;
                    continue;
                }

                if (line.equals("cached")) {
                    _state = ReplicaState.CACHED;
                    continue;
                }

                if (line.equals("from_client")) {
                    _state = ReplicaState.FROM_CLIENT;
                    continue;
                }

                if (line.equals("from_store")) {
                    _state = ReplicaState.FROM_STORE;
                    continue;
                }

                /*
                 * backward compatibility
                 */

                if (line.equals("receiving.store")) {
                    _state = ReplicaState.FROM_STORE;
                    continue;
                }

                if (line.equals("receiving.cient")) {
                    _state = ReplicaState.FROM_CLIENT;
                    continue;
                }

                // in case of some one fixed the spelling
                if (line.equals("receiving.client")) {
                    _state = ReplicaState.FROM_CLIENT;
                    continue;
                }

                // FORMAT: sticky:owner:exipire
                if (line.startsWith("sticky")) {

                    String[] stickyOptions = line.split(":");

                    String owner;
                    long expire;

                    switch (stickyOptions.length) {
                    case 1:
                        // old style
                        owner = "system";
                        expire = -1;
                        break;
                    case 2:
                        // only owner defined
                        owner = stickyOptions[1];
                        expire = -1;
                        break;
                    case 3:
                        owner = stickyOptions[1];
                        try {
                            expire = Long.parseLong(stickyOptions[2]);
                        } catch (NumberFormatException nfe) {
                            // bad number
                            _state = ReplicaState.BROKEN;
                            return;
                        }

                        break;
                    default:
                        _log.info("Unknow number of arguments in {} [{}]", _controlFile, line);
                        _state = ReplicaState.BROKEN;
                        return;
                    }

                    _sticky.addRecord(owner, expire, true);
                    continue;
                }

                // if none of knows states, then it's BAD state
                _log.error("Invalid state [{}] for entry {}", line, _controlFile);
                break;
            }
        }

    }

    public Collection<StickyRecord> stickyRecords()
    {
        return _sticky.records();
    }
}
