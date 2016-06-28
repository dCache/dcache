package org.dcache.pool.repository.v5;

import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.pool.FaultAction;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.ReplicaStore;
import org.dcache.pool.repository.SpaceRecord;

class CheckHealthTask implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckHealthTask.class);
    public static final int GRACE_PERIOD_ON_FREE = 60_000;

    private ReplicaRepository _repository;

    /**
     * Shared repository account object for tracking space.
     */
    private Account _account;

    /**
     * Meta data about files in the pool.
     */
    private ReplicaStore _replicaStore;

    /**
     * Command string to execute periodically to check the health of the file system,
     * disk array, host, etc.
     */
    private String[] _commands = {};

    public void setRepository(ReplicaRepository repository)
    {
        _repository = repository;
    }

    public void setAccount(Account account)
    {
        _account = account;
    }

    public void setReplicaStore(ReplicaStore store)
    {
        _replicaStore = store;
    }

    public void setCommand(String s)
    {
        _commands = new Scanner(s).scan();
    }

    @Override
    public void run()
    {
        switch (_repository.getState()) {
        case UNINITIALIZED:
        case INITIALIZED:
        case LOADING:
        case FAILED:
        case CLOSED:
            break;
        case OPEN:
            if (!_replicaStore.isOk()) {
                _repository.fail(FaultAction.DISABLED, "I/O test failed");
            }

            if (!checkSpaceAccounting()) {
                LOGGER.error("Marking pool read-only due to accounting errors. This is a bug. Please report it to support@dcache.org.");
                _repository.fail(FaultAction.READONLY, "Accounting errors detected");
            }

            adjustFreeSpace();
        }

        checkHealthCommand();
    }

    private void checkHealthCommand()
    {
        if (_commands.length > 0) {
            NDC.push("health-check");
            try {
                ProcessBuilder builder = new ProcessBuilder(_commands);
                builder.redirectErrorStream(true);
                Process process = builder.start();
                try {
                    StringBuilder output = new StringBuilder();
                    try (InputStream in = process.getInputStream()) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                        String line = reader.readLine();
                        while (line != null) {
                            output.append(line).append('\n');
                            line = reader.readLine();
                        }
                    }
                    int code = process.waitFor();
                    switch (code) {
                    case 0:
                        if (output.length() > 0) {
                            LOGGER.debug("{}", output);
                        }
                        break;
                    case 1:
                        _repository.fail(FaultAction.READONLY, "Health check command failed with exit code 1");
                        if (output.length() > 0) {
                            LOGGER.warn("{}", output);
                        }
                    default:
                        _repository.fail(FaultAction.DISABLED, "Health check command failed with exit code " + code);
                        if (output.length() > 0) {
                            LOGGER.warn("{}", output);
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.debug("Health check command was interrupted");
                    process.destroy();
                }
            } catch (IOException e) {
                LOGGER.error("Failed to launch health check command '{}': {}",
                        Arrays.toString(_commands), e.getMessage());
            } finally {
                NDC.pop();
            }
        }
    }

    private boolean checkSpaceAccounting()
    {
        SpaceRecord record = _account.getSpaceRecord();
        long removable = record.getRemovableSpace();
        long total = record.getTotalSpace();
        long free = record.getFreeSpace();
        long precious = record.getPreciousSpace();
        long used = total - free;

        if (removable < 0) {
            LOGGER.error("Removable space is negative.");
            return false;
        }

        if (total < 0) {
            LOGGER.error("Repository size is negative.");
            return false;
        }

        if (free < 0) {
            LOGGER.error("Free space is negative.");
            return false;
        }

        if (precious < 0) {
            LOGGER.error("Precious space is negative.");
            return false;
        }

        if (used < 0) {
            LOGGER.error("Used space is negative.");
            return false;
        }

        /* The following check cannot be made consistently, since we
         * do not retrieve these values atomically. Therefore we log
         * the error, but do not return false.
         */
        if (precious + removable > used) {
            LOGGER.warn("Used space is less than the sum of precious and removable space (this may be a temporary problem - if it persists then please report it to support@dcache.org).");
        }

        return true;
    }

    private void adjustFreeSpace()
    {
        /* At any time the file system must have at least as much free
         * space as shows in the account. Thus invariantly
         *
         *      _replicaStore.getFreeSpace >= _account.getFree
         *
         * Taking the monitor lock on the account object prevents
         * anybody else from allocating space from the account. Hence
         * throughout the period we have the lock, the file system
         * must have at least as much free space as the account.
         */
        Account account = _account;
        synchronized (account) {
            /* It is not uncommon that file system free space asynchronously from
             * file deletion. Thus after we delete a file, it may take a while
             * before the free space is reported as such by the operating system.
             * To compensate, we suppress this check for a grace period after the
             * last delete.
             */
            if (account.getTimeOfLastFree() > System.currentTimeMillis() - GRACE_PERIOD_ON_FREE) {
                long free = _replicaStore.getFreeSpace();
                long total = _replicaStore.getTotalSpace();

                if (total == 0) {
                    LOGGER.debug("Java reported file system size as 0. Skipping file system size check.");
                    return;
                }

                if (total < account.getTotal()) {
                    LOGGER.warn(AlarmMarkerFactory.getMarker(
                                        PredefinedAlarm.POOL_SIZE, _repository.getPoolName()),
                                "The file system containing the data files "
                                        + "appears to be smaller {} than the configured "
                                        + "pool size {}.",
                                String.format("(%,d bytes)", total),
                                String.format("(%,d bytes)", _account.getTotal()));
                }

                if (free < account.getFree()) {
                    long newSize =
                            account.getTotal() - (account.getFree() - free);
                    LOGGER.warn(AlarmMarkerFactory.getMarker(
                                        PredefinedAlarm.POOL_FREE_SPACE, _repository.getPoolName()),
                                "The file system containing the data files "
                                        + "appears to have less free space {} than "
                                        + "expected {}; reducing the pool size to {} "
                                        + "to compensate. Notice that this does not leave "
                                        + "any space for the meta data. If such data is "
                                        + "stored on the same file system, then it is "
                                        + "paramount that the pool size is reconfigured "
                                        + "to leave enough space for the meta data.",
                                String.format("(%,d bytes)", free),
                                String.format("(%,d bytes)", _account.getFree()),
                                String.format("%,d bytes", newSize));
                    account.setTotal(newSize);
                }
            }
        }
    }

    /**
     * Scanner for parsing strings of white space separated
     * words. Characters may be escaped with a backslash and character
     * sequences may be quoted.
     */
    static class Scanner
    {
        private final CharSequence _line;
        private int _position;

        public Scanner(CharSequence line)
        {
            _line = line;
        }

        private char peek()
        {
            return isEof() ? (char) 0 : _line.charAt(_position);
        }

        private char readChar()
        {
            char c = peek();
            _position++;
            return c;
        }

        private boolean isEof()
        {
            return (_position >= _line.length());
        }

        private boolean isWhitespace()
        {
            return Character.isWhitespace(peek());
        }

        private void scanWhitespace()
        {
            while (isWhitespace()) {
                readChar();
            }
        }

        public String[] scan()
        {
            List<String> arguments = new ArrayList<>();
            scanWhitespace();
            while (!isEof()) {
                arguments.add(scanWord());
                scanWhitespace();
            }
            return arguments.toArray(new String[arguments.size()]);
        }

        /**
         * Scans the next word. A word is a sequence of non-white
         * space characters and escaped or quoted white space
         * characters. The unescaped and unquoted word is returned.
         */
        private String scanWord()
        {
            StringBuilder word = new StringBuilder();
            while (!isEof() && !isWhitespace()) {
                scanWordElement(word);
            }
            return word.toString();
        }

        /**
         * Scans the next element of a word. Elements of a word are
         * non-white space characters, escaped characters and quoted
         * strings. The unescaped and unquoted element is added to word.
         */
        private void scanWordElement(StringBuilder word)
        {
            if (!isEof() && !isWhitespace()) {
                switch (peek()) {
                case '\'':
                    scanSingleQuotedString(word);
                    break;
                case '"':
                    scanDoubleQuotedString(word);
                    break;
                case '\\':
                    scanEscapedCharacter(word);
                    break;
                default:
                    word.append(readChar());
                    break;
                }
            }
        }

        /**
         * Scans a single quoted string. Escaped characters are not
         * recognized. The unquoted string is added to word.
         */
        private void scanSingleQuotedString(StringBuilder word)
        {
            if (readChar() != '\'') {
                throw new IllegalStateException("Parse failure");
            }

            while (!isEof()) {
                char c = readChar();
                switch (c) {
                case '\'':
                    return;
                default:
                    word.append(c);
                    break;
                }
            }
        }

        /**
         * Scans a double quoted string. Escaped characters are
         * recognized. The unquoted and unescaped string is added to
         * word.
         */
        private void scanDoubleQuotedString(StringBuilder word)
        {
            if (readChar() != '"') {
                throw new IllegalStateException("Parse failure");
            }

            while (!isEof()) {
                switch (peek()) {
                case '\\':
                    scanEscapedCharacter(word);
                    break;
                case '"':
                    readChar();
                    return;
                default:
                    word.append(readChar());
                    break;
                }
            }
        }

        /**
         * Scans a backslash escaped character. The escaped character
         * without the escape symbol is added to word.
         */
        private void scanEscapedCharacter(StringBuilder word)
        {
            if (readChar() != '\\') {
                throw new IllegalStateException("Parse failure");
            }

            if (!isEof()) {
                word.append(readChar());
            }
        }
    }

}
