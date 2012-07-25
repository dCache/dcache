package org.dcache.pool.classic;

import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.namespace.FileAttribute;

import dmg.util.Args;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChecksumModuleV1
    extends AbstractCellComponent
    implements CellCommandListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(ChecksumModuleV1.class);

    public final static long BYTES_IN_MEBIBYTE = 1024 * 1024;

    private boolean _onRead;
    private boolean _onWrite;
    private boolean _onTransfer;
    private boolean _onFlush;
    private boolean _onRestore;
    private boolean _enforceCRC;
    private boolean _updatepnfs;

    private volatile boolean _scrub;
    private volatile double  _throughputLimit = Double.POSITIVE_INFINITY;
    private volatile long    _scrubPeriod     = TimeUnit.HOURS.toMillis(24L);

    private ChecksumFactory _defaultChecksumFactory;
    private ChecksumScanner _scanner;

    private final PnfsHandler _pnfs;

    public ChecksumModuleV1(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
        try {
            _defaultChecksumFactory =
                ChecksumFactory.getFactory(ChecksumType.ADLER32);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("adler32 is not supported", e);
        }
    }

    /**
     * Returns the first non-null argument.
     */
    private <T> T getFirstNonNull(T ... objects)
    {
        for (T t : objects) {
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    public void setChecksumScanner(ChecksumScanner scanner)
    {
        _scanner = scanner;
    }

    public void setMoverChecksums(PnfsId id,
                                  File file,
                                  ChecksumFactory factory,
                                  Checksum clientChecksum, // can be null
                                  Checksum transferChecksum)
        throws CacheException,
               InterruptedException,
               IOException
    {
        Checksum pnfsChecksum =
            (clientChecksum == null)
            ? factory.find(_pnfs.getFileAttributes(id, EnumSet.of(FileAttribute.CHECKSUM)).getChecksums())
            : null;
        Checksum fileChecksum =
            (_onWrite || (_onTransfer && transferChecksum == null))
            ? factory.computeChecksum(file)
            : null;
        Checksum checksum =
            getFirstNonNull(clientChecksum, pnfsChecksum, transferChecksum, fileChecksum);

        if (checksum == null) {
            if (!_enforceCRC) {
                return;
            }
            checksum = factory.computeChecksum(file);
        }

        if ((((transferChecksum != null) && !transferChecksum.equals(checksum)))
            || (((fileChecksum != null) && !fileChecksum.equals(checksum)))) {
            throw new CacheException(String.format("Checksum error: client=%s,pnfs=%s,transfer=%s,file=%s",
                                                   clientChecksum,
                                                   pnfsChecksum,
                                                   transferChecksum,
                                                   fileChecksum));
        }

        if ((pnfsChecksum == null) && (checksum != null)) {
            _log.info("Storing checksum {} for {}", checksum, id);
            _pnfs.setChecksum(id, checksum);
        }
    }

    public ChecksumFactory getDefaultChecksumFactory()
    {
        return _defaultChecksumFactory;
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        pw.println("csm set checksumtype "+_defaultChecksumFactory.getType());
        if (_scrub) {
            pw.print("csm set policy -scrub=on");
            pw.print(" -limit="+
                     (Double.isInfinite(_throughputLimit) ? "off"
                                                          : _throughputLimit / BYTES_IN_MEBIBYTE));
            pw.println(" -period="+TimeUnit.MILLISECONDS.toHours(_scrubPeriod));
        } else {
            pw.println("csm set policy -scrub=off");
        }
        pw.print("csm set policy");
        pw.print(" -onread="); pw.print(_onRead?"on":"off");
        pw.print(" -onwrite="); pw.print(_onWrite?"on":"off");
        pw.print(" -onflush="); pw.print(_onFlush?"on":"off");
        pw.print(" -onrestore="); pw.print(_onRestore?"on":"off");
        pw.print(" -ontransfer="); pw.print(_onTransfer?"on":"off");
        pw.print(" -enforcecrc="); pw.print(_enforceCRC?"on":"off");
        pw.print(" -getcrcfromhsm="); pw.print(_updatepnfs?"on":"off");
        pw.println("");
    }

    public long getScrubPeriod()
    {
        return _scrubPeriod;
    }

    public double getThroughputLimit()
    {
        return _throughputLimit;
    }

    public boolean isScrubbingEnabled()
    {
        return _scrub;
    }

    public boolean checkOnRead()
    {
        return _onRead;
    }

    public boolean checkOnFlush()
    {
        return _onFlush;
    }

    public boolean checkOnRestore()
    {
        return _onRestore;
    }

    public boolean checkOnWrite()
    {
        return _onWrite;
    }

    public boolean checkOnTransfer()
    {
        return _onTransfer;
    }

    public boolean enforceCRC()
    {
        return _enforceCRC;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("                Version : $Id$");
        pw.println("          Checksum type : "+_defaultChecksumFactory.getType());
        pw.print(" Checkum calculation on : ");
        if (_onRead) {
            pw.print("read ");
        }
        if (_onWrite) {
            pw.print("write ");
        }
        if (_onFlush) {
            pw.print("flush ");
        }
        if (_onRestore) {
            pw.print("restore ");
        }
        if (_onTransfer) {
            pw.print("transfer ");
        }
        if (_enforceCRC) {
            pw.print("enforceCRC ");
        }
        if (_updatepnfs) {
            pw.print("getcrcfromhsm ");
        }
        if (_scrub) {
            pw.print("scrub(");
            pw.print("limit=" + (Double.isInfinite(_throughputLimit) ? "off" : _throughputLimit / BYTES_IN_MEBIBYTE));
            pw.print(",");
            pw.print("period=" + TimeUnit.MILLISECONDS.toHours(_scrubPeriod));
            pw.print(")");
        }
        pw.println("");
    }

    public final static String hh_csm_info = "";
    public String ac_csm_info_$_0(Args args)
    {
        return getPolicies();
    }

    private String getPolicies()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(" Policies :\n").
            append("        on read : ").append(_onRead).append("\n").
            append("       on write : ").append(_onWrite).append("\n").
            append("       on flush : ").append(_onFlush).append("\n").
            append("     on restore : ").append(_onRestore).append("\n").
            append("    on transfer : ").append(_onTransfer).append("\n").
            append("    enforce crc : ").append(_enforceCRC).append("\n").
            append("  getcrcfromhsm : ").append(_updatepnfs).append("\n").
            append("          scrub : ").append(_scrub).append("\n");
        if (_scrub) {
            if (Double.isInfinite(_throughputLimit)) {
                sb.append("             limit  = off\n");
            } else {
                sb.append("             limit  = ").append(_throughputLimit / BYTES_IN_MEBIBYTE).append(" MiB/s\n");
            }
            sb.append("             period = ").append(TimeUnit.MILLISECONDS.toHours(_scrubPeriod)).append(" hours\n");
        }
        return sb.toString();
    }

    public final static String hh_csm_set_policy = "[-<option>=on|off] ... # see 'help csm set policy";
    public final static String fh_csm_set_policy =
        "  Syntax : csm set policy [-<option>=on[|off]] ...\n"+
        "\n"+
        "    OPTIONS :\n"+
        "       -scrub:  periodically verify pool data against checksums (suboptions :)\n"+
        "\n"+
        "            -limit=<MiB/s>|off : checksum computation throughput limit\n"+
        "            -period=<hours>    : run scrubber every <hours> hours\n"+
        "\n"+
        "       -onread          : run check before each open for reading\n"+
        "       -onwrite         : run check after receiving the file from client (on fs)\n"+
        "       -onflush         : run check before flush to HSM\n"+
        "       -onrestore       : run check after restore from HSM\n"+
        "       -ontransfer      : run check while receiving data from client (on the fly)\n"+
        "       -enforcecrc      : make sure there is at least one crc stored in pnfs\n"+
        "       -getcrcfromhsm   : read the <pool>/data/<pnfsid>.crcval file and send to pnfs\n"+
        "\n";

    public String ac_csm_set_policy_$_0(Args args)
    {
        _scrub      = checkBoolean(args, "scrub"      , _scrub);
        _onRead     = checkBoolean(args, "onread"     , _onRead);
        _onWrite    = checkBoolean(args, "onwrite"    , _onWrite);
        _onFlush    = checkBoolean(args, "onflush"    , _onFlush);
        _onRestore  = checkBoolean(args, "onrestore"  , _onRestore);
        _onTransfer = checkBoolean(args, "ontransfer" , _onTransfer);
        _enforceCRC = checkBoolean(args, "enforcecrc" , _enforceCRC);
        _updatepnfs = checkBoolean(args, "getcrcfromhsm" , _updatepnfs);

        String limitOption = args.getOpt("limit");
        if (limitOption != null) {
            if (limitOption.equals("off")) {
                _throughputLimit = Double.POSITIVE_INFINITY;
            } else {
                double limit =
                    args.getDoubleOption("limit",
                                         _throughputLimit / BYTES_IN_MEBIBYTE)
                                         * BYTES_IN_MEBIBYTE;
                if (limit <= 0) {
                    throw new IllegalArgumentException("Throughput limit must be > 0");
                }
                _throughputLimit = limit;
            }
        }

        String periodOption = args.getOpt("period");
        if (periodOption != null) {
            long period = TimeUnit.HOURS.toMillis(Integer.parseInt(periodOption));
            if (period <= 0) {
                throw new IllegalArgumentException("Scrub interval must be > 0");
            }
            _scrubPeriod = period;
        }

        if (_scrub) {
            _scanner.startScrubber();
        } else {
            _scanner.stopScrubber();
        }

        return !args.hasOption("v") ? "" : getPolicies();
    }

    public boolean getCrcFromHsm()
    {
        return _updatepnfs;
    }

    public final static String hh_csm_set_checksumtype = "adler32|md5";
    public String ac_csm_set_checksumtype_$_1(Args args) throws Exception
    {
        String newChecksumType = args.argv(0);
        //
        // can we do that  ?
        //
        _defaultChecksumFactory =
            ChecksumFactory.getFactory(ChecksumType.getChecksumType(newChecksumType));

        //
        // seems to be ok
        //
        return "New checksumtype : "+ _defaultChecksumFactory.getType();
    }

    private boolean checkBoolean(Args args, String key, boolean currentValue)
    {
        String value = args.getOpt(key);
        if (value == null) {
            return currentValue;
        }
        if (value.equals("on") || value.equals("")) {
            return true;
        } else if (value.equals("off")) {
            return false;
        }

        throw new IllegalArgumentException("-"+key+"[=on|off]");
    }
}
