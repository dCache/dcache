// $Id: PoolStatisticsV0.java,v 1.12 2006-12-15 15:45:40 tigran Exp $Cg

package diskCacheV111.services;

import static java.util.Arrays.asList;
import static org.dcache.util.ByteUnit.BYTES;
import static org.dcache.util.Files.checkDirectory;

import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.google.common.net.UrlEscapers;
import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.dcache.cells.CellStub;
import org.dcache.util.Args;
import org.dcache.util.Exceptions;
import org.dcache.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Patrick Fuhrmann
 * <p>
 * PoolStatisticsV0 collects statistical information from the billing cell and the connected pools
 * and prepares a raw statistics file for further processing. As an optional second step,
 * PoolStatisticsV0 converts the raw statistics file into a tree of html pages.
 * <p>
 * <p>
 * BASIC steps :
 * <p>
 * Step 1 : The PoolManager is queried for the name of currently active Pools. Step 2 : Those pools
 * are queried for the 'per class' statistics information. Step 3 : The billing cell is asked for
 * the data flow data per pool and class. Step 4 : This information is merged and stored as
 * lt;year&gt;-&lt;month&gt;-&lt;day&gt;-&lt;hour&gt;.raw and lt;year&gt;-&lt;month&gt;-&lt;day&gt;-day.raw
 * in &lt;dbBase&gt;/&lt;year&gt;/&lt;month&gt;/&lt;day&gt; Step 5 : If available, the 'raw' files
 * of today and yesterday are merged and stored as lt;year&gt;-&lt;month&gt;-&lt;day&gt;-day.drw
 * into the same directory.
 * <p>
 * <p>
 * HTML steps :
 *
 *
 * <base>/<year>/<month>/<day>/class-<class>.html
 * <base>/<year>/<month>/<day>/pool-<pool>.html
 * <base>/<year>/<month>/<day>/pools.html
 * <base>/<year>/<month>/<day>/classes.html
 * <base>/<year>/<month>/<day>/total.drw
 * <base>/<year>/<month>/<day>/index.html
 * <base>/<year>/<month>/<day>/total.raw    #  sum of(total.drw)
 * <p>
 * updateHtmlMonth ----------------
 * <base>/<year>/<month>/index.html
 * <base>/<year>/<month>/total.raw   #   sum of float of xx/total.raw and av. of pool of
 * xx/total.raw
 * <p>
 * <p>
 * first rows are of  pool and storage group
 * <p>
 * repository at beginning and end of the day
 * <p>
 * 0   bytes on this pool of this storage group of beginning of this day 1   number of files on this
 * pool of this sg of beginning of this day 2   bytes on this pool of this storage group of end of
 * this day 3   number of files on this pool of this sg of end of this day
 * <p>
 * actions during the day
 * <p>
 * 4   total number of transfer (in and out   client <-> dCache) 5   total number of restores (hsm
 * -> dCache) 6   total number of stores (dCache -> HSM) 7   total number of errors
 * <p>
 * 8  total bytes transferred into the dCache (client -> dCache) 9  total bytes transferred out of
 * the dCache (dCache -> client) 10  total bytes transferred from HSM into the dCache 11  total
 * bytes transferred into the HSM from the dCache
 */
public class PoolStatisticsV0 extends CellAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolStatisticsV0.class);

    /*
     *   Magic spells to get the infos out of the different cells.
     */
    private static final String GET_REP_STATISTICS = "rep ls -s -binary";
    private static final String GET_CELL_INFO = "xgetcellinfo";
    private static final String GET_POOL_STATISTICS = "get pool statistics";
    private static final String RESET_POOL_STATISTICS = "clear pool statistics";
    /*
     *  Definition of the counter values from 'billing' cell.
     */
    private static final int YESTERDAY = 0;
    private static final int TODAY = 2;
    private static final int TRANSFER_IN = 8;
    private static final int TRANSFER_OUT = 9;
    private static final int RESTORE = 10;
    private static final int STORE = 11;

    private static final Escaper AS_FILENAME = Escapers.builder().
          addEscape('^', "^o^").
          addEscape('/', "^s^").
          build();
    private static final Escaper AS_PATH_ELEMENT = UrlEscapers.urlPathSegmentEscaper();

    private static final String DEFAULT_AUTHOR = "&copy; dCache.org ";

    private static final DateTimeFormatter _pathFromDate =
          DateTimeFormatter.ofPattern("yyyy" + File.separator + "MM" + File.separator + "dd" +
                File.separator + "yyyy-MM-dd-HH'.raw'");

    private static final DateTimeFormatter _dayPathFromDate =
          DateTimeFormatter.ofPattern("yyyy" + File.separator + "MM" + File.separator + "dd" +
                File.separator + "yyyy-MM-dd-'day.raw'");

    private static final DateTimeFormatter _dayDiffPathFromDate =
          DateTimeFormatter.ofPattern("yyyy" + File.separator + "MM" + File.separator + "dd" +
                File.separator + "yyyy-MM-dd-'day.drw'");

    private static final DateTimeFormatter _htmlPathFromDate =
          DateTimeFormatter.ofPattern("yyyy" + File.separator + "MM" + File.separator + "dd");

    private static final DateTimeFormatter _yearOfCalendar = DateTimeFormatter.ofPattern("yyyy");

    private static final DateTimeFormatter _monthOfCalendar = DateTimeFormatter.ofPattern(
          "MMM MM yyyy");

    private static final DateTimeFormatter _dayOfCalendar = DateTimeFormatter.ofPattern(
          "MM/dd yyyy (EEE)");

    private static final DateTimeFormatter _dayOfCalendarKey = DateTimeFormatter.ofPattern(
          "EEE-MM/dd-yyyy");

    private static String _domainName = "dCache.Unknown.Org";
    private static String _bodyString = "<body bgcolor=white>";

    private CellStub _poolManager;
    private CellStub _billing;
    private CellStub _poolStub;

    private final CellNucleus _nucleus;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private volatile ScheduledFuture<?> scheduledTask;

    private File _dbBase;
    private File _htmlBase;
    private Map<String, Map<String, long[]>> _recentPoolStatistics;
    private boolean _createHtmlTree = true;

    public PoolStatisticsV0(String name, String args) {
        super(name, args);
        _nucleus = getNucleus();
    }

    @Override
    protected void starting() throws Exception {
        Args args = getArgs();
        if (args.argc() < 1) {
            throw new
                  IllegalArgumentException(
                  "Usage : ... <baseDirectory> " +
                        "[-htmlBase=<htmlBase>|none] [-create] [-images=<images>]");
        }

        _poolManager = new CellStub(this, new CellPath(args.getOption("poolManager")),
              TimeUnit.MILLISECONDS.convert(args.getLongOption("poolManagerTimeout"),
                    TimeUnit.valueOf(args.getOption("poolManagerTimeoutUnit"))));
        _billing = new CellStub(this, new CellPath(args.getOption("billing")),
              TimeUnit.MILLISECONDS.convert(args.getLongOption("billingTimeout"),
                    TimeUnit.valueOf(args.getOption("billingTimeoutUnit"))));
        _poolStub = new CellStub(this, null,
              TimeUnit.MILLISECONDS.convert(args.getLongOption("poolTimeout"),
                    TimeUnit.valueOf(args.getOption("poolTimeoutUnit"))));

        _htmlBase = _dbBase = new File(args.argv(0));

        String tmp = args.getOpt("htmlBase");
        if ((tmp != null) && (!tmp.isEmpty())) {
            if (tmp.equals("none")) {
                _createHtmlTree = false;
            } else {
                _htmlBase = new File(tmp);
            }
        }
        tmp = args.getOpt("domain");
        if (tmp != null) {
            _domainName = tmp;
        }

        if (args.hasOption("create")) {
            if (!_dbBase.exists()) {
                //noinspection ResultOfMethodCallIgnored
                _dbBase.mkdirs();
            }
            if (_createHtmlTree && !_htmlBase.exists()) {
                //noinspection ResultOfMethodCallIgnored
                _htmlBase.mkdirs();
            }

        } else {
            checkDirectory(_dbBase);
            if (_createHtmlTree) {
                checkDirectory(_htmlBase);
            }
        }
    }

    @Override
    protected void started() {
        scheduledTask = executor.scheduleAtFixedRate(new HourlyRunner(), 5, 60, TimeUnit.MINUTES);
    }

    @Override
    protected void stopping() {
        executor.shutdown();
    }

    @Override
    public void getInfo(PrintWriter pw) {
        String _images = "/images";
        pw.println("   Cell Name : " + getCellName());
        pw.println("  Cell Class : " + this.getClass().getName());
        pw.println("   Stat Base : " + _dbBase);
        pw.println("   Html Base : " + _htmlBase);
        pw.println("      Images : " + _images);
        pw.println("    Next Run : " +
              (scheduledTask == null ? "-" :
                    TimeUtils.relativeTimestamp(
                          Instant.now().plusSeconds(scheduledTask.getDelay(TimeUnit.SECONDS)))));
    }

    @Override
    public void messageArrived(CellMessage message) {
    }

    private static ZonedDateTime convertDate(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault());
    }

    private String getNiceDayOfCalendar(Calendar calendar) {
        return _dayOfCalendar.format(convertDate(calendar.getTime()));
    }

    private File getCurrentPath(Calendar calendar) {
        return new File(_dbBase,
              _pathFromDate.format(convertDate(calendar.getTime())));
    }

    private File getTodayPath(Calendar calendar) {
        return new File(_dbBase,
              _dayPathFromDate.format(convertDate(calendar.getTime())));
    }

    private File getTodayDiffPath(Calendar calendar) {
        return new File(_dbBase,
              _dayDiffPathFromDate.format(convertDate(calendar.getTime())));
    }

    private File getYesterdayPath(Calendar calendar) {
        calendar = (Calendar) calendar.clone();
        calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) - 1);
        return getTodayPath(calendar);
    }

    private File getHtmlPath(Calendar calendar) {
        return new File(_htmlBase,
              _htmlPathFromDate.format(convertDate(calendar.getTime())));
    }

    public void createDiffFile(File today, File yesterday, File resultFile) throws IOException {
        DataStore todayStore = new DataStore(today);

        Map<String, long[]> todayMap = todayStore.getLinearMap();

        Map<String, long[]> yesterdayMap = new DataStore(yesterday).getLinearMap();

        Map<String, long[]> resultMap = new HashMap<>();

        for (Map.Entry<String, long[]> entry : todayMap.entrySet()) {
            String key = entry.getKey();
            long[] counter = entry.getValue();

            long[] result = new long[12];

            long[] yesterdayCounter = yesterdayMap.get(key);
            if (yesterdayCounter == null) {
                result[0] = -1;
                result[1] = -1;
            } else {
                result[0] = yesterdayCounter[0];
                result[1] = yesterdayCounter[1];
            }
            System.arraycopy(counter, 0, result, 2, 10);

            resultMap.put(key, result);
        }

        //noinspection ResultOfMethodCallIgnored
        resultFile.delete();

        try (PrintWriter pw = new PrintWriter(new FileWriter(resultFile))) {
            pw.println("#");
            Iterator<Map.Entry<String, Object>> it = todayStore.iterator();
            it.forEachRemaining(
                  entry -> pw.println("# " + entry.getKey() + "=" + entry.getValue()));
            pw.println("#");
            resultMap.entrySet().forEach(entry -> {
                pw.print(entry.getKey());
                for (long l : entry.getValue()) {
                    pw.print(" ");
                    pw.print(l);
                }
                pw.println("");
            });
        }
    }

    private class HourlyRunner implements Runnable {

        private Calendar currentTime;

        @Override
        public void run() {
            currentTime = new GregorianCalendar();
            currentTime.set(Calendar.SECOND, 0);
            File path = getCurrentPath(currentTime);
            File parent = path.getParentFile();

            if (!parent.exists()) {
                //noinspection ResultOfMethodCallIgnored
                parent.mkdirs();
            }

            try {
                LOGGER.info("Hourly ticker : {}", new Date());

                LOGGER.info("Starting hourly run for : {}", path);

                createHourlyRawFile(path, currentTime);

                LOGGER.info("Hourly run finished for : {}", path);

                File today = getTodayPath(currentTime);
                LOGGER.info("Creating daily file : {}", today);

                //noinspection ResultOfMethodCallIgnored
                Files.copy(path.toPath(), today.toPath(), StandardCopyOption.REPLACE_EXISTING);

                LOGGER.info("Daily file done : {}", today);

                File yesterday = getYesterdayPath(currentTime);
                if (yesterday.exists()) {
                    File diffFile = getTodayDiffPath(currentTime);
                    LOGGER.info("Starting diff run for : {}", yesterday);

                    createDiffFile(today, yesterday, diffFile);

                    LOGGER.info("Finishing diff run for : {}", diffFile);
                }
                if (currentTime.get(Calendar.HOUR_OF_DAY) == 23) {
                    resetBillingStatistics();
                }
            } catch (Exception ee) {
                LOGGER.warn("Failed to create file {}: {}", path,
                      Exceptions.messageOrClassName(ee));
                path.delete();
            }

            if (!_createHtmlTree) {
                return;
            }
            try {
                LOGGER.info("Creating html tree");
                prepareDailyHtmlFiles(currentTime);
                if (currentTime.get(Calendar.HOUR_OF_DAY) == 23) {
                    updateHtmlMonth(currentTime);
                    updateHtmlYear(currentTime);
                    updateHtmlTop();
                }
            } catch (Exception eee) {
                LOGGER.warn("Exception in creating html tree for : " + path, eee);
            }
        }
    }

    private void updateHtmlMonth(Calendar calendar) throws IOException {
        File dir = getHtmlPath(calendar).getParentFile();
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        File[] list = dir.listFiles(new MonthFileFilter());

        list = resortFileList(list, -1);

        long[] counter = null;
        long[] total = new long[12];
        long[] lastInMonth = new long[12];

        BaseStatisticsHtml html = new BaseStatisticsHtml();
        html.setSorted(false);
        html.setTitle(_monthOfCalendar.format(convertDate(calendar.getTime())));
        html.setKeyType("Date");
        html.setAuthor(DEFAULT_AUTHOR);
        html.setHeader(new MonthDirectoryHeader());

        for (int i = 0, n = list.length; i < n; i++) {
            File x = new File(list[i], "total.raw");
            if (!x.exists()) {
                continue;
            }
            try (BufferedReader br = new BufferedReader(new FileReader(x))) {
                String line = br.readLine();
                if ((line == null) || (line.length() < 12)) {
                    continue;
                }

                StringTokenizer st = new StringTokenizer(line);

                st.nextToken();
                String key = _dayOfCalendar.format(
                      convertDate(new Date(Long.parseLong(st.nextToken()))));
                counter = new long[12];
                for (int j = 0; j < counter.length; j++) {
                    counter[j] = Long.parseLong(st.nextToken());
                }

                if (i == 0) {
                    System.arraycopy(counter, 0, lastInMonth, 0, counter.length);
                }
                add(total, counter);
                html.add(key, AS_PATH_ELEMENT.escape(list[i].getName()) + "/index.html", counter);
            } catch (IOException ignored) {
            }
        }

        try (PrintWriter pw = new PrintWriter(new FileWriter(new File(dir, "index.html")))) {
            html.setPrintWriter(pw);
            html.dump();
        }

        if (counter != null) {
            total[YESTERDAY] = counter[YESTERDAY];
            total[YESTERDAY + 1] = counter[YESTERDAY + 1];
        }
        total[TODAY] = lastInMonth[TODAY];
        total[TODAY + 1] = lastInMonth[TODAY + 1];

        printTotal(new File(dir, "total.raw"), total, calendar.getTime());
    }

    private void updateHtmlYear(Calendar calendar) throws IOException {
        File dir = getHtmlPath(calendar).getParentFile().getParentFile();
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        File[] list = dir.listFiles(new MonthFileFilter());

        list = resortFileList(list, -1);

        long[] counter = null;
        long[] total = new long[12];
        long[] lastInMonth = new long[12];

        BaseStatisticsHtml html = new BaseStatisticsHtml();
        html.setSorted(false);
        html.setTitle(_yearOfCalendar.format(convertDate(calendar.getTime())));
        html.setKeyType("Date");
        html.setAuthor(DEFAULT_AUTHOR);
        html.setHeader(new YearDirectoryHeader());

        for (int i = 0, n = list.length; i < n; i++) {
            File x = new File(list[i], "total.raw");
            if (!x.exists()) {
                continue;
            }
            try (BufferedReader br = new BufferedReader(new FileReader(x))) {
                String line = br.readLine();
                if ((line == null) || (line.length() < 12)) {
                    continue;
                }

                StringTokenizer st = new StringTokenizer(line);

                st.nextToken();
                String key = _monthOfCalendar.format(
                      convertDate(new Date(Long.parseLong(st.nextToken()))));
                counter = new long[12];
                for (int j = 0; j < counter.length; j++) {
                    counter[j] = Long.parseLong(st.nextToken());
                }
                if (i == 0) {
                    System.arraycopy(counter, 0, lastInMonth, 0, counter.length);
                }
                add(total, counter);
                html.add(key, AS_PATH_ELEMENT.escape(list[i].getName()) + "/index.html", counter);
            } catch (IOException ignored) {
            }
        }
        try (PrintWriter pw = new PrintWriter(new FileWriter(new File(dir, "index.html")))) {
            html.setPrintWriter(pw);
            html.dump();
        }

        if (counter != null) {
            total[YESTERDAY] = counter[YESTERDAY];
            total[YESTERDAY + 1] = counter[YESTERDAY + 1];
        }
        total[TODAY] = lastInMonth[TODAY];
        total[TODAY + 1] = lastInMonth[TODAY + 1];

        printTotal(new File(dir, "total.raw"), total, calendar.getTime());
    }

    private void updateHtmlTop() throws IOException {
        File dir = _htmlBase;
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        File[] list = dir.listFiles(new YearFileFilter());

        list = resortFileList(list, -1);

        long[] counter = null;
        long[] total = new long[12];
        long[] lastInYear = new long[12];

        BaseStatisticsHtml html = new BaseStatisticsHtml();
        html.setSorted(false);
        html.setTitle("dCache Statistics of dCache Domain : " + _domainName);
        html.setKeyType("Year");
        html.setAuthor(DEFAULT_AUTHOR);
        html.setHeader(new TopDirectoryHeader());

        for (int i = 0, n = list.length; i < n; i++) {
            File x = new File(list[i], "total.raw");
            if (!x.exists()) {
                continue;
            }
            try (BufferedReader br = new BufferedReader(new FileReader(x))) {
                String line = br.readLine();
                if ((line == null) || (line.length() < 12)) {
                    continue;
                }

                StringTokenizer st = new StringTokenizer(line);

                st.nextToken();
                String key = _yearOfCalendar.format(
                      convertDate(new Date(Long.parseLong(st.nextToken()))));
                counter = new long[12];
                for (int j = 0; j < counter.length; j++) {
                    counter[j] = Long.parseLong(st.nextToken());
                }
                if (i == 0) {
                    System.arraycopy(counter, 0, lastInYear, 0, counter.length);
                }
                add(total, counter);
                html.add(key, list[i].getName() + File.separator + "index.html", counter);
            } catch (IOException ignored) {
            }
        }
        try (PrintWriter pw = new PrintWriter(new FileWriter(new File(dir, "index.html")))) {
            html.setPrintWriter(pw);
            html.dump();
        }

        if (counter != null) {
            total[YESTERDAY] = counter[YESTERDAY];
            total[YESTERDAY + 1] = counter[YESTERDAY + 1];
        }
        total[TODAY] = lastInYear[TODAY];
        total[TODAY + 1] = lastInYear[TODAY + 1];

        printTotal(new File(dir, "total.raw"), total, new Date());
    }

    private File[] resortFileList(File[] list, int direction) {
        Comparator<File> comparator = (f1, f2) -> direction * f1.getName().compareTo(f2.getName());
        return asList(list).stream().sorted(comparator).toArray(File[]::new);
    }

    private static class MonthFileFilter implements FileFilter {

        @Override
        public boolean accept(File file) {
            return file.isDirectory() && (file.getName().length() == 2);
        }
    }

    private static class YearFileFilter implements FileFilter {

        @Override
        public boolean accept(File file) {
            return file.isDirectory() && (file.getName().length() == 4);
        }
    }

    @SuppressWarnings("unused")
    public static final String hh_set_html_body = "<bodyString>; eg: \"<body background=/images/bg.svg>\"";

    @SuppressWarnings("unused")
    public String ac_set_html_body_$_1(Args args) {
        _bodyString = args.argv(0);
        return "";
    }

    @SuppressWarnings("unused")
    public static final String hh_create_html = "[<year> [<month> [[<day>]]]";

    @SuppressWarnings("unused")
    public String ac_create_html_$_0_3(Args args) throws IOException {

        if (args.argc() == 3) {
            int year = Integer.parseInt(args.argv(0));
            int month = Integer.parseInt(args.argv(1));
            int day = Integer.parseInt(args.argv(2));

            Calendar calendar = new GregorianCalendar();
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, month - 1);
            calendar.set(Calendar.DAY_OF_MONTH, day);

            prepareDailyHtmlFiles(calendar);
        } else if (args.argc() == 2) {
            int year = Integer.parseInt(args.argv(0));
            int month = Integer.parseInt(args.argv(1));

            Calendar calendar = new GregorianCalendar();
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, month - 1);
            calendar.set(Calendar.DAY_OF_MONTH, 1);

            updateHtmlMonth(calendar);
        } else if (args.argc() == 1) {
            int year = Integer.parseInt(args.argv(0));

            Calendar calendar = new GregorianCalendar();
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, 1);
            calendar.set(Calendar.DAY_OF_MONTH, 1);

            updateHtmlYear(calendar);
        } else if (args.argc() == 0) {
            updateHtmlTop();
        }
        return "Done";
    }

    @SuppressWarnings("unused")
    public static final String hh_create_stat = "[<outputFileName>]";

    @SuppressWarnings("unused")
    public String ac_create_stat_$_0_1(Args args) {
        if (args.argc() == 0) {
            _nucleus.newThread(
                  new Runnable() {
                      @Override
                      public void run() {
                          try {
                              LOGGER.info("Starting internal Manual run");
                              synchronized (PoolStatisticsV0.this) {
                                  _recentPoolStatistics = null;
                              }
                              Map<String, Map<String, long[]>> map = createStatisticsMap();
                              synchronized (PoolStatisticsV0.this) {
                                  _recentPoolStatistics = map;
                              }
                              LOGGER.info("Finishing internal Manual run");
                          } catch (Exception e) {
                              LOGGER.info("Aborting internal Manual run {}", e.toString());
                          }
                      }
                  },
                  "internal"
            ).start();
            return "Thread started for internal run";
        } else {
            final File file = new File(args.argv(0));
            _nucleus.newThread(() -> {
                try {
                    LOGGER.info("Starting Manual run for file : {}", file);
                    createHourlyRawFile(file, new GregorianCalendar());
                    LOGGER.info("Finishing Manual run for file : {}", file);
                } catch (Exception e) {
                    LOGGER.info("Aborting Manual run for file : {} {}", file, e.toString());
                }
            }, file.toString()).start();

            return "Thread started for : " + args.argv(0);
        }
    }

    private interface Iteratable {

        void mapEntry(String pool, String className, long[] counters);
    }

    private interface HtmlDrawable {

        void draw(PrintWriter pw);
    }

    private static class PatternIterator implements Iteratable {

        private final Pattern _pattern;
        private final StringBuffer _sb;
        private final long[] _sum = new long[16];
        private int _mx;

        private PatternIterator(Pattern pattern) {
            _pattern = pattern;
            _sb = new StringBuffer();
        }

        @Override
        public void mapEntry(String poolName, String className, long[] counters) {
            StringBuilder sb = new StringBuilder();
            sb.append(poolName).append(" ").append(className).append(" ");
            for (long counter : counters) {
                sb.append(counter);
                sb.append(" ");
            }
            String line = sb.toString();
            if (!_pattern.matcher(line).matches()) {
                return;
            }
            _sb.append(line).append("\n");
            for (int i = 0, n = counters.length, m = _sum.length; (i < n) && (i < m); i++) {
                if (_sum[i] > -1) {
                    _sum[i] += counters[i];
                }
            }
            _mx = Math.max(_mx, counters.length);
        }

        public long[] getCounters() {
            long[] result = new long[_mx];
            System.arraycopy(_sum, 0, result, 0, _mx);
            return result;
        }

        public String toString() {
            return _sb.toString();
        }

        public StringBuffer getStringBuffer() {
            return _sb;
        }
    }

    @SuppressWarnings("unused")
    public static final String hh_show = "[<pattern>]";

    @SuppressWarnings("unused")
    public String ac_show_$_0_1(Args args) {
        Map<String, Map<String, long[]>> map;
        synchronized (this) {
            map = _recentPoolStatistics;
        }
        if (map == null) {
            return "Pool Statistics not available yet";
        } else if (args.argc() == 0) {
            StringBuffer st = new StringBuffer();
            dumpStatistics(map, st);
            return st.toString();
        }
        Pattern p = Pattern.compile(args.argv(0));
        PatternIterator pi = new PatternIterator(p);
        dumpStatistics(map, pi);
        StringBuffer sb = pi.getStringBuffer();
        long[] counters = pi.getCounters();
        sb.append("* *");
        for (long counter : counters) {
            sb.append(" ").append(counter);
        }
        return sb.toString();
    }

    private Map<String, Map<String, long[]>> createStatisticsMap()
          throws InterruptedException, IOException, NoRouteToCellException {

        Map<String, Map<String, long[]>> pool = getPoolRepositoryStatistics();
        Map<String, Map<String, long[]>> billing = getBillingStatistics();

        return mergeStorageClassMaps(pool, billing);
    }

    private static class DataStore {

        private Map<String, Map<String, long[]>> _data;
        private final Map<String, Object> _attributes = new HashMap<>();
        private int _minCount = 64;
        private int _maxCount;

        public DataStore(Map<String, Map<String, long[]>> data) {
            _data = data;
        }

        public DataStore(File file) throws IOException {
            load(file);
        }

        public Map<String, long[]> getLinearMap() {
            final Map<String, long[]> map = new HashMap<>();

            dumpStatistics(_data,
                  (pool, className, counters) -> map.put(pool + " " + className, counters));

            return map;
        }

        public Iterator<Map.Entry<String, Object>> iterator() {
            return _attributes.entrySet().iterator();
        }

        public void setTime(Date date) {
            add("timestamp", date.getTime());
            add("date", date);
        }

        private void add(String key, Object value) {
            _attributes.put(key, value);
        }

        public Map<String, Map<String, long[]>> getMap() {
            return _data;
        }

        public void store(File file) throws IOException {
            try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
                pw.println("#");
                for (Map.Entry<String, Object> entry : _attributes.entrySet()) {
                    pw.println("# " + entry.getKey() + "=" + entry.getValue());
                }
                pw.println("#");
                dumpStatistics(_data, pw);
            }
        }

        private void scanAttributes(String line) {
            if (line.length() < 3) {
                return;
            }
            line = line.substring(2).trim();
            if (line.isEmpty()) {
                return;
            }
            int indx = line.indexOf('=');
            if ((indx == -1) || (indx == (line.length() - 1))) {
                _attributes.put(line, "");
            } else if (indx != 0) {
                _attributes.put(line.substring(0, indx).trim(),
                      line.substring(indx + 1).trim());
            }
        }

        private void load(File f) throws IOException {

            HashMap<String, Map<String, long[]>> map = new HashMap<>();

            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                for (String line = br.readLine(); line != null; line = br.readLine()) {
                    if ((!line.isEmpty()) && (line.charAt(0) == '#')) {
                        scanAttributes(line);
                        continue;
                    }

                    StringTokenizer st = new StringTokenizer(line);

                    if (!st.hasMoreTokens()) {
                        continue;
                    }
                    String poolName = st.nextToken();
                    if (!st.hasMoreTokens()) {
                        continue;
                    }
                    String className = st.nextToken();

                    long[] counter = new long[64];
                    int i = 0;
                    for (int n = counter.length; (i < n) && st.hasMoreTokens(); i++) {
                        counter[i] = Long.parseLong(st.nextToken());
                    }
                    _minCount = Math.min(_minCount, i);
                    _maxCount = Math.max(_maxCount, i);
                    long[] res = new long[i];
                    System.arraycopy(counter, 0, res, 0, i);
                    Map<String, long[]> classMap = map.get(poolName);
                    if (classMap == null) {
                        map.put(poolName, classMap = new HashMap<>());
                    }
                    classMap.put(className, res);
                }
                _data = map;
            }
        }
    }

    private void createHourlyRawFile(File outputFile, Calendar calendar)
          throws InterruptedException, IOException, NoRouteToCellException {

        if (outputFile.exists() || !outputFile.getParentFile().canWrite()) {
            throw new IOException("File exists or directory not writable : " + outputFile);
        }
        DataStore store = new DataStore(createStatisticsMap());
        store.setTime(calendar.getTime());
        store.store(outputFile);
    }

    private static void dumpStatistics(Map<String, Map<String, long[]>> result,
          Iteratable mapentry) {

        for (Map.Entry<String, Map<String, long[]>> entry : result.entrySet()) {
            String poolName = entry.getKey();

            Map<String, long[]> classes = entry.getValue();

            for (Map.Entry<String, long[]> classEntry : classes.entrySet()) {
                String className = classEntry.getKey();

                long[] values = classEntry.getValue();

                mapentry.mapEntry(poolName, className, values);
            }
        }
    }

    private void printIndex(File file, String title) throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
            pw.println("<html><head><title>" + title + "</title></head>");
            pw.println(_bodyString);
            pw.print("<center><h2>");
            pw.print(_domainName);
            pw.println("</h2></center>");
            pw.println("<hr>");
            pw.print("<pre>   <a href=\"/docs/statisticsHelp.html\">Help</a>");
            pw.print("      <a href=\"/\">Birds Home</a>");
            pw.print("      <a href=\"../../../index.html\">Top</a>");
            pw.print("      <a href=\"../../index.html\">This Year</a>");
            pw.println("      <a href=\"../index.html\">This Month</a></pre>");
            pw.println("<hr>");
            pw.println("<center><h1>" + title + "</h1></center>");
            pw.println("<center>");
            pw.println("<h3><a href=\"pools.html\">Pools</a></h3>");
            pw.println("<h3><a href=\"classes.html\">Classes</a></h3>");
            pw.println("<h3><a href=\"total.drw\">Raw</a></h3>");
            pw.println("</center>");
            pw.println("</body></html>");
        }
    }

    private void prepareDailyHtmlFiles(Calendar calendar) {

        File diffFile = getTodayDiffPath(calendar);
        if (!diffFile.exists()) {
            LOGGER.warn("prepareDailyHtmlFiles : File not found : {}", diffFile);
            return;
        }
        File dir = getHtmlPath(calendar);
        if (!dir.exists()) {
            //noinspection ResultOfMethodCallIgnored
            dir.mkdirs();
        }

        try {
            // copy the raw file into the html directory
            Files.copy(diffFile.toPath(), new File(dir, "total.drw").toPath(),
                  StandardCopyOption.REPLACE_EXISTING);
            // load the raw data file
            Map<String, Map<String, long[]>> map = new DataStore(diffFile).getMap();
            // create todays html files
            prepareDailyHtml(map, dir, calendar.getTime());
            // prepare the daily index.html file
            printIndex(new File(dir, "index.html"), getNiceDayOfCalendar(calendar));

        } catch (IOException ee) {
            LOGGER.warn("Can't prepare Html directory : " + dir, ee);
        }
    }

    private Map<String, Map<String, long[]>>[] prepareDailyHtml(
          Map<String, Map<String, long[]>> poolMap,
          File pathBase,
          Date date
    ) throws IOException {

        Iterator<Map.Entry<String, Map<String, long[]>>> entries = poolMap.entrySet().iterator();
        Map<String, Map<String, long[]>> classMap2 = new HashMap<>();
        DayDirectoryHeader header = new DayDirectoryHeader(date, _dayOfCalendar);

        BaseStatisticsHtml allPoolsHtml = new BaseStatisticsHtml();
        allPoolsHtml.setSorted(true);
        allPoolsHtml.setTitle("Pools");
        allPoolsHtml.setKeyType("PoolNames");
        allPoolsHtml.setAuthor(DEFAULT_AUTHOR);
        allPoolsHtml.setHeader(header);

        long[] total = null;

        while (entries.hasNext()) {
            Map.Entry<String, Map<String, long[]>> entry = entries.next();
            String poolName = entry.getKey();
            Map<String, long[]> classMap = entry.getValue();
            Iterator<Map.Entry<String, long[]>> classEntries =
                  classMap.entrySet().iterator();

            long[] sum = null;

            BaseStatisticsHtml html = new BaseStatisticsHtml();
            html.setSorted(true);
            html.setTitle("Pool : " + poolName);
            html.setKeyType("ClassNames");
            html.setAuthor(DEFAULT_AUTHOR);
            html.setHeader(header);
            while (classEntries.hasNext()) {
                Map.Entry<String, long[]> classEntry = classEntries.next();
                String className = classEntry.getKey();

                long[] values = classEntry.getValue();

                if (sum == null) {
                    sum = new long[values.length];
                }
                add(sum, values);
                if (total == null) {
                    total = new long[values.length];
                }
                add(total, values);
                //
                // poolName, className, values
                //
                Map<String, long[]> poolMap2 = new HashMap<>();
                classMap2.putIfAbsent(className, poolMap2);

                poolMap2.put(poolName, values);

                //
                // statistics ...
                //
                html.add(className,
                      AS_PATH_ELEMENT.escape(AS_FILENAME.escape("class-" + className + ".html")),
                      values);
            }

            String filename = AS_FILENAME.escape("pool-" + poolName + ".html");

            if (sum != null) {
                allPoolsHtml.add(poolName, AS_PATH_ELEMENT.escape(filename), sum);
            }
            try (PrintWriter pw = new PrintWriter(new FileWriter(new File(pathBase, filename)))) {
                html.setPrintWriter(pw);
                html.dump();
            }
        }

        try (PrintWriter allPw = new PrintWriter(
              new FileWriter(new File(pathBase, "pools.html")))) {
            allPoolsHtml.setPrintWriter(allPw);
            allPoolsHtml.dump();
        }

        if (total != null) {
            printTotal(new File(pathBase, "total.raw"), total, date);
        }

        BaseStatisticsHtml allClassesHtml = new BaseStatisticsHtml();
        allClassesHtml.setSorted(true);
        allClassesHtml.setTitle("Classes");
        allClassesHtml.setAuthor(DEFAULT_AUTHOR);
        allClassesHtml.setKeyType("ClassNames");
        allClassesHtml.setHeader(header);

        entries = classMap2.entrySet().iterator();
        while (entries.hasNext()) {

            Map.Entry<String, Map<String, long[]>> entry = entries.next();

            String className = entry.getKey();

            Map<String, long[]> poolMap2;
            poolMap2 = entry.getValue();
            long[] sum = null;

            BaseStatisticsHtml html = new BaseStatisticsHtml();
            html.setSorted(true);
            html.setTitle("Class " + className);
            html.setAuthor(DEFAULT_AUTHOR);
            html.setKeyType("Pools");
            html.setHeader(header);

            for (Map.Entry<String, long[]> poolEntry : poolMap2.entrySet()) {
                String poolName = poolEntry.getKey();

                long[] values = poolEntry.getValue();

                if (sum == null) {
                    sum = new long[values.length];
                }
                add(sum, values);

                html.add(poolName, AS_PATH_ELEMENT.escape("pool-" + poolName + ".html"), values);
            }
            String filename = AS_FILENAME.escape("class-" + className + ".html");
            if (sum != null) {
                allClassesHtml.add(className, AS_PATH_ELEMENT.escape(filename), sum);
            }
            try (PrintWriter pw = new PrintWriter(new FileWriter(new File(pathBase, filename)))) {
                html.setPrintWriter(pw);
                html.dump();
            }
        }

        try (PrintWriter allPw = new PrintWriter(
              new FileWriter(new File(pathBase, "classes.html")))) {
            allClassesHtml.setPrintWriter(allPw);
            allClassesHtml.dump();
        }
        //noinspection unchecked
        return new Map[]{poolMap, classMap2};
    }

    private void printTotal(File filename, long[] total, Date date) throws IOException {
        try (PrintWriter dayTotal = new PrintWriter(new FileWriter(filename))) {
            String day = _dayOfCalendarKey.format(convertDate(date));
            dayTotal.print(day);
            dayTotal.print(" ");
            dayTotal.print(date.getTime());
            for (long aTotal : total) {
                dayTotal.print(" ");
                dayTotal.print(aTotal);
            }
            dayTotal.println("");
        }
    }

    private void add(long[] sum, long[] add) {
        int l = Math.min(sum.length, add.length);
        for (int i = 0; i < l; i++) {
            if (add[i] > -1) {
                sum[i] += add[i];
            }
        }
    }

    private static void dumpStatistics(Map<String, Map<String, long[]>> result,
          final PrintWriter pw) {
        dumpStatistics(result, (poolName, className, counters) -> {
            pw.print(poolName + " " + className + " ");
            for (long counter : counters) {
                pw.print(counter);
                pw.print(" ");
            }
            pw.println("");
        });
        pw.flush();
    }

    private void dumpStatistics(Map<String, Map<String, long[]>> result, final StringBuffer pw) {
        dumpStatistics(result, (poolName, className, counters) -> {
            pw.append(poolName).append(" ").append(className).append(" ");
            for (long counter : counters) {
                pw.append(counter);
                pw.append(" ");
            }
            pw.append("\n");
        });
    }

    private Map<String, Map<String, long[]>> mergeStorageClassMaps(
          Map<String, Map<String, long[]>> fromPools,
          Map<String, Map<String, long[]>> fromBilling) {

        HashMap<String, Map<String, long[]>> result = new HashMap<>();

        Iterator<String> pools = fromPools.keySet().iterator();
        pools.forEachRemaining(poolName -> {
            Map<String, long[]> resultClasses = result.get(poolName);
            if (resultClasses == null) {
                result.put(poolName, resultClasses = new HashMap<>());
            }

            Map<String, long[]> stat = fromPools.get(poolName);
            if (stat == null) {
                return;
            }

            for (Map.Entry<String, long[]> entry : stat.entrySet()) {

                long[] values = entry.getValue();
                if (values == null) {
                    continue;
                }

                long[] resultArray = resultClasses.get(entry.getKey());
                if (resultArray == null) {
                    resultClasses.put(entry.getKey(), resultArray = new long[10]);
                    int preset = fromBilling.get(poolName) == null ? -1 : 0;
                    for (int i = 2; i < 10; i++) {
                        resultArray[i] = preset;
                    }
                }
                System.arraycopy(values, 0, resultArray, 0, 2);
            }
        });

        pools = fromBilling.keySet().iterator();
        pools.forEachRemaining(poolName -> {
            Map<String, long[]> resultClasses = result.get(poolName);
            if (resultClasses == null) {
                result.put(poolName, resultClasses = new HashMap<>());
            }

            Map<String, long[]> stat = fromBilling.get(poolName);
            if (stat == null) {
                return;
            }

            for (Map.Entry<String, long[]> entry : stat.entrySet()) {
                long[] values = entry.getValue();
                if (values == null) {
                    continue;
                }

                long[] resultArray = resultClasses.get(entry.getKey());
                if (resultArray == null) {
                    resultClasses.put(entry.getKey(), resultArray = new long[10]);
                    int preset = fromPools.get(poolName) == null ? -1 : 0;
                    for (int i = 0; i < 2; i++) {
                        resultArray[i] = preset;
                    }
                }
                System.arraycopy(values, 0, resultArray, 2, 8);
            }
        });
        return result;
    }

    //
    // expected format from 'rep ls -s -binary'
    // Object[*]
    //   Object [2]
    //     0 String <storageClass>
    //     1 long[2]
    //          0  # of bytes in repository
    //          1  # of files in repository
    //
    private Map<String, Map<String, long[]>> getPoolRepositoryStatistics()
          throws InterruptedException,
          NoRouteToCellException,
          IOException {
        LOGGER.info("getPoolRepositoryStatistics : asking PoolManager for cell info");

        PoolManagerCellInfo info;
        try {
            info = _poolManager.sendAndWait(GET_CELL_INFO, PoolManagerCellInfo.class);
        } catch (CacheException e) {
            throw new IOException(e.getMessage(), e);
        }

        LOGGER.info("getPoolRepositoryStatistics :  PoolManager replied : {}", info);

        Map<String, Map<String, long[]>> map = new HashMap<>();
        for (Map.Entry<String, CellAddressCore> pool : info.getPoolMap().entrySet()) {
            CellAddressCore address = pool.getValue();
            try {
                LOGGER.info("getPoolRepositoryStatistics : asking {} for statistics", address);
                Object[] result =
                      _poolStub.sendAndWait(new CellPath(address), GET_REP_STATISTICS,
                            Object[].class);
                Map<String, long[]> classMap = new HashMap<>();
                for (Object entry : result) {
                    Object[] e = (Object[]) entry;
                    classMap.put((String) e[0], (long[]) e[1]);
                }
                LOGGER.info("getPoolRepositoryStatistics : {} replied with {}", address, classMap);
                map.put(pool.getKey(), classMap);

            } catch (InterruptedException ie) {
                LOGGER.warn("getPoolRepositoryStatistics : sendAndWait interrupted");
                throw ie;
            } catch (CacheException e) {
                LOGGER.warn("getPoolRepositoryStatistics : {} : {}", address, e.getMessage());
            }
        }
        return map;
    }

    private void resetBillingStatistics() {
        LOGGER.info("Resetting Billing statistics");
        _billing.notify(RESET_POOL_STATISTICS);
    }

    //
    // structur expected from 'billing' : get pool statistics
    //   map(String <poolName>, long[4])
    //     0 : # of transfers
    //     1 : # of restore
    //     2 : # of store
    //     3   # of total errors
    //
    // structur expected from 'billing' : get pool statistics <poolName>
    //
    //     storageClassMap(String <storageClass>, long[8]);
    //
    //     0 : # of transfers
    //     1 : # of restore
    //     2 : # of store
    //     3   # of total errors
    //     4 : bytes transferred IN
    //     5 : bytes transferred OUT
    //     6 : bytes restored
    //     7 : bytes stored
    //
    private Map<String, Map<String, long[]>> getBillingStatistics()
          throws InterruptedException,
          IOException, NoRouteToCellException {
        LOGGER.info("getBillingStatistics : asking billing for generic pool statistics");
        Map<String, Map<String, long[]>> generic;
        try {
            //noinspection unchecked
            generic = _billing.sendAndWait(GET_POOL_STATISTICS, Map.class);
        } catch (CacheException e) {
            throw new IOException(e.getMessage(), e);
        }
        LOGGER.info("getBillingStatistics :  billing replied with {}", generic);

        Map<String, Map<String, long[]>> map = new HashMap<>();
        for (String poolName : generic.keySet()) {
            try {
                LOGGER.info("getBillingStatistics : asking billing for [{}] statistics", poolName);
                //noinspection unchecked
                Map<String, long[]> result = _billing.sendAndWait(
                      GET_POOL_STATISTICS + " " + poolName, Map.class);
                LOGGER.info("getBillingStatistics : billing replied with {}", result);

                map.put(poolName, result);
            } catch (InterruptedException ie) {
                LOGGER.warn("'get pool statistics' : sendAndWait interrupted");
                throw ie;
            } catch (CacheException e) {
                LOGGER.warn("'get pool statistics' : {} : {}", poolName, e.getMessage());
            }
        }
        return map;
    }

    public static void main(String[] args) throws Exception {
        DataStore store = new DataStore(new File("xxinput"));
        store.store(new File("xxoutput"));
    }

    private static class DayDirectoryHeader implements HtmlDrawable {

        private final Date _date;
        private final DateTimeFormatter _dayOfCalendar;

        private DayDirectoryHeader(Date date, DateTimeFormatter dayOfCalendar) {
            _date = date;
            _dayOfCalendar = dayOfCalendar;
        }

        @Override
        public void draw(PrintWriter pw) {
            pw.print("<hr><pre>    ");
            pw.print("<a href=\"/docs/statisticsHelp.html\">Help</a>         ");
            pw.print("<a href=\"/\">Birds Home</a>         ");
            pw.print("<a href=\"../../../index.html\">Top</a>         ");
            pw.print("<a href=\"../../index.html\">This Year</a>         ");
            pw.print("<a href=\"../index.html\">This Month</a>         ");
            pw.print("<a href=\"index.html\">Today</a>         ");
            pw.print("<a href=\"pools.html\">Pools</a>         ");
            pw.print("<a href=\"classes.html\">Classes</a>         ");
            pw.print("<a href=\"total.drw\">Raw</a>");
            pw.println("</pre><hr>");
            pw.println("<center><h1>" +
                  _dayOfCalendar.format(convertDate(_date)) + "</h1></center>");
        }
    }

    private static class MonthDirectoryHeader implements HtmlDrawable {

        private MonthDirectoryHeader() {
        }

        @Override
        public void draw(PrintWriter pw) {
            pw.print("<hr><pre>    ");
            pw.print("<a href=\"/docs/statisticsHelp.html\">Help</a>         ");
            pw.print("<a href=\"/\">Birds Home</a>         ");
            pw.print("<a href=\"../../index.html\">Top</a>         ");
            pw.print("<a href=\"../index.html\">This Year</a>         ");
            pw.println("</pre><hr>");
        }
    }

    private static class YearDirectoryHeader implements HtmlDrawable {

        private YearDirectoryHeader() {
        }

        @Override
        public void draw(PrintWriter pw) {
            pw.print("<hr><pre>    ");
            pw.print("<a href=\"/docs/statisticsHelp.html\">Help</a>         ");
            pw.print("<a href=\"/\">Birds Home</a>         ");
            pw.print("<a href=\"../index.html\">Top</a>         ");
            pw.println("</pre><hr>");
        }
    }

    private static class TopDirectoryHeader implements HtmlDrawable {

        private TopDirectoryHeader() {
        }

        @Override
        public void draw(PrintWriter pw) {
            pw.print("<hr><pre>    ");
            pw.print("<a href=\"/docs/statisticsHelp.html\">Help</a>         ");
            pw.print("<a href=\"/\">Birds Home</a>         ");
            pw.println("</pre><hr>");
        }
    }

    private static class BaseStatisticsHtml {

        private final int _height = 10;
        private final int _absoluteWidth = 500;
        private final int _relativeWidth = 100;
        private final String[] _bgcolor = {"white", "#bebebe"};
        private PrintWriter _pw;
        private final String _imageBase = "/images/";
        private final String _poolYesterday = _imageBase + "greenbox.gif";
        private final String _poolToday = _imageBase + "redbox.gif";
        private final String _restore = _imageBase + "bluebox.gif";
        private final String _transferIn = _imageBase + "navybox.gif";
        private final String _transferOut = _imageBase + "yellowbox.gif";
        private final String _store = _imageBase + "orangebox.gif";
        private long _absoluteNorm = 1L;
        private Map<String, Object[]> _map;
        private long _maxCounterValue;
        private String _title = "Title";
        private String _author = "dCache Team";
        private final String _tableTitleColor = "#115259";
        private final String _keyType = "Key";
        private final String[] _tableTitles = {_keyType,
              "Absolute Values",
              "Data / MiB",
              "Relative Values"};

        private HtmlDrawable _header;

        private void setHeader(HtmlDrawable drawable) {
            _header = drawable;
        }

        public void setSorted(boolean sorted) {
            _map = sorted ? new TreeMap() : new LinkedHashMap();
            _maxCounterValue = 0L;
        }

        public void setKeyType(String keyType) {
            _tableTitles[0] = keyType;
        }

        public void setPrintWriter(PrintWriter pw) {
            _pw = pw;
        }

        public void reset() {
            _maxCounterValue = 0L;
        }

        public void add(String title, String link, long[] counter) {
            Object[] o = new Object[3];
            o[0] = title;
            o[1] = link;
            o[2] = counter;
            _maxCounterValue = Math.max(_maxCounterValue, getNorm(counter));
            _map.put(title, o);
        }

        public void setTitle(String title) {
            _title = title;
        }

        public void setAuthor(String author) {
            _author = author;
        }

        public void dump() {
            printHeader(_title);
            if (_header != null) {
                _header.draw(_pw);
            }
            printTitle(_title);
            printTableHeader();

            _absoluteNorm = _maxCounterValue;

            Iterator<Object[]> it = _map.values().iterator();
            for (int i = 0; it.hasNext(); i++) {
                Object[] o = it.next();
                printRow(
                      (String) o[0],
                      (String) o[1],
                      (long[]) o[2],
                      _bgcolor[i % 2]
                );
            }
            printTableTrailer();
            printTrailer(_author + "; Created : " + new Date().toString());
        }

        private void printTableHeader() {
            _pw.println("<center><table border=1 cellpadding=0 cellspacing=0 width=\"90%\">");

            _pw.print("<tr>");

            for (String title : _tableTitles) {
                _pw.print("<th bgcolor=\"");
                _pw.print(_tableTitleColor);
                _pw.print("\"><font color=white>");
                _pw.print(title);
                _pw.println("</font></th>");
            }
            _pw.println("</tr>");
        }

        private void printTableTrailer() {
            _pw.println("</table></center>");
            _pw.flush();
        }

        private void printHeader(String title) {
            _pw.print("<html><head><title>");
            _pw.print(title);
            _pw.println("</title></head>" + _bodyString);
            _pw.print("<center><h2>");
            _pw.print(_domainName);
            _pw.println("</h2></center>");
        }

        private void printTitle(String title) {
            _pw.print("<center><h1>");
            _pw.print(title);
            _pw.println("</h1></center>");
        }

        private void printTrailer(String author) {
            if (author != null) {
                _pw.print("<hr><address>");
                _pw.print(author);
                _pw.println("</address>");
            }
            _pw.println("</body></html>");
            _pw.flush();
        }

        private long getNorm(long[] counter) {
            return Math.max(
                  counter[YESTERDAY] + counter[RESTORE] + counter[TRANSFER_IN],
                  Math.max(
                        counter[TODAY],
                        counter[STORE] + counter[TRANSFER_OUT]
                  ));
        }

        private String printUnit(long counter) {
            if (counter <= 0) {
                return "0";
            }
            String unit = String.valueOf(BYTES.toMiB(counter));
            StringBuilder sb = new StringBuilder();
            for (int j = unit.length() - 1, c = 0; j >= 0; j--, c++) {
                if ((c > 0) && (c % 3 == 0)) {
                    sb.append('.');
                }
                sb.append(unit.charAt(j));
            }
            return sb.reverse().toString();
        }

        private void printRow(String title, String link, long[] counter, String bgColor) {
            int value;
            long norm = getNorm(counter);
            int content;
            printTR();

            _pw.print("<th rowspan=3 align=center bgcolor=\"");
            _pw.print(bgColor);
            _pw.print("\">");
            printHREF(title, link);
            _pw.println("</th>");

            printTD(bgColor, false);
            content = 0;
            value = (int) (((double) counter[YESTERDAY]) / ((double) _absoluteNorm)
                  * ((double) _absoluteWidth));
            content += printImage(_poolYesterday, value, _height);

            value = (int) (((double) counter[RESTORE]) / ((double) _absoluteNorm)
                  * ((double) _absoluteWidth));
            content += printImage(_restore, value, _height);

            value = (int) (((double) counter[TRANSFER_IN]) / ((double) _absoluteNorm)
                  * ((double) _absoluteWidth));
            content += printImage(_transferIn, value, _height);

            printTDEnd(content);

            printTD(bgColor, true);
            print(printUnit(counter[YESTERDAY]));
            print(" + " + printUnit(counter[RESTORE]), "red");
            print(" + " + printUnit(counter[TRANSFER_IN]), "red");
            printTDEnd(1);

            printTD(bgColor, false);
            content = 0;

            value = (int) (((double) counter[YESTERDAY]) / ((double) norm)
                  * ((double) _relativeWidth));
            content += printImage(_poolYesterday, value, _height);

            value = (int) (((double) counter[RESTORE]) / ((double) norm)
                  * ((double) _relativeWidth));
            content += printImage(_restore, value, _height);

            value = (int) (((double) counter[TRANSFER_IN]) / ((double) norm)
                  * ((double) _relativeWidth));
            content += printImage(_transferIn, value, _height);

            printTDEnd(content);

            printTREnd();

            printTR();

            printTD(bgColor, false);
            content = 0;
            value = (int) (((double) counter[TODAY]) / ((double) _absoluteNorm)
                  * ((double) _absoluteWidth));
            content += printImage(_poolToday, value, _height);

            printTDEnd(content);

            printTD(bgColor, true);
            print(printUnit(counter[TODAY]));
            printTDEnd(1);

            printTD(bgColor, false);
            content = 0;
            value = (int) (((double) counter[TODAY]) / ((double) norm) * ((double) _relativeWidth));
            content += printImage(_poolToday, value, _height);

            printTDEnd(content);

            printTREnd();

            printTR();

            printTD(bgColor, false);
            content = 0;
            value = (int) (((double) counter[TRANSFER_OUT]) / ((double) _absoluteNorm)
                  * ((double) _absoluteWidth));
            content += printImage(_transferOut, value, _height);

            value = (int) (((double) counter[STORE]) / ((double) _absoluteNorm)
                  * ((double) _absoluteWidth));
            content += printImage(_store, value, _height);

            printTDEnd(content);

            printTD(bgColor, true);
            print(printUnit(counter[TRANSFER_OUT]));
            print(" + " + printUnit(counter[STORE]));
            printTDEnd(1);

            printTD(bgColor, false);
            content = 0;
            value = (int) (((double) counter[TRANSFER_OUT]) / ((double) norm)
                  * ((double) _relativeWidth));
            content += printImage(_transferOut, value, _height);

            value = (int) (((double) counter[STORE]) / ((double) norm) * ((double) _relativeWidth));
            content += printImage(_store, value, _height);

            printTDEnd(content);

            printTREnd();
        }

        private void printTR() {
            _pw.println("<tr>");
        }

        private void printTREnd() {
            _pw.println("</tr>");
        }

        private int printImage(String imageFile, int width, int height) {
            if (width <= 0) {
                return 0;
            }
            _pw.print("<img src=\"");
            _pw.print(imageFile);
            _pw.print("\" width=");
            _pw.print(Math.max(width, 0));
            _pw.print(" height=");
            _pw.print(height);
            _pw.print(">");
            return 1;
        }

        private void printTD(String bgcolor, boolean center) {
            _pw.print("<td ");
            if (center) {
                _pw.print("align=center ");
            }
            _pw.print("bgcolor=\"");
            _pw.print(bgcolor);
            _pw.print("\">");
        }

        private void print(String string) {
            print(string, null);
        }

        private void print(String string, String color) {
            if (color != null) {
                _pw.print("<font color=\"");
                _pw.print(color);
                _pw.print("\">");
            }
            _pw.print(string);
            if (color != null) {
                _pw.print("</font>");
            }
        }

        private void printTDEnd(int content) {
            if (content == 0) {
                _pw.print("&nbsp;");
            }
            _pw.println("</td>");
        }

        private void printHREF(String title, String link) {
            _pw.print("<a href=\"");
            _pw.print(link);
            _pw.print("\">");
            _pw.print(title);
            _pw.println("</a>");
        }
    }
}
