package org.dcache.cells;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanInstantiationException;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.InvalidPropertyException;
import org.springframework.beans.PropertyAccessException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import java.beans.PropertyDescriptor;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.DomainContextAware;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.SetupInfoMessage;
import dmg.util.Args;
import dmg.util.CommandException;
import dmg.util.CommandThrowableException;

import org.dcache.util.ClassNameComparator;
import org.dcache.vehicles.BeanQueryAllPropertiesMessage;
import org.dcache.vehicles.BeanQueryMessage;
import org.dcache.vehicles.BeanQuerySinglePropertyMessage;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Universal cell for building complex cells from simpler components.
 *
 * This class aims at being a universal cell for dCache. It makes it
 * possible to construct complex cells from simpler cell
 * components. The Spring Framework is used to manage the lifetime of
 * the cell components, as well as the wiring between components. We
 * therfore borrow the term "bean" from the Spring Framework and refer
 * to cell components as beans.
 *
 * Beans can get access to core cell functionality by implementing one
 * or more of the following interfaces: CellInfoProvider,
 * CellCommunicationAware, ThreadFactoryAware, CellCommandListener,
 * and CellSetupAware. When instantiated through this class, those
 * interfaces are detected and the necessary wiring is performed
 * automatically.
 */
public class UniversalSpringCell
    extends AbstractCell
    implements BeanPostProcessor,
               EnvironmentAware
{
    private final static long WAIT_FOR_FILE_SLEEP = 30000;

    private final static Logger _log = LoggerFactory
            .getLogger(UniversalSpringCell.class);

    /**
     * Environment map this cell was instantiated in.
     */
    private Map<String,Object> _environment = Collections.emptyMap();

    /**
     * Spring application context. All beans are created through this
     * context.
     */
    private ConfigurableApplicationContext _context;

    /**
     * Registered info providers mapped to their bean names. Sorted to
     * maintain consistent ordering.
     */
    private final Map<CellInfoProvider,String> _infoProviders =
        new TreeMap<>(new ClassNameComparator());

    /**
     * List of registered setup providers. Sorted to maintain
     * consistent ordering.
     */
    private final Set<CellSetupProvider> _setupProviders =
        new TreeSet<>(new ClassNameComparator());

    /**
     * List of registered life cycle aware beans. Sorted to maintain
     * consistent ordering.
     */
    private final Set<CellLifeCycleAware> _lifeCycleAware =
        new TreeSet<>(new ClassNameComparator());

    /**
     * Cell name of the setup controller.
     */
    private String _setupController;

    /**
     * Setup class used for sending a setup to the setup controller.
     */
    private String _setupClass;

    /**
     * Setup to execute during start and to which to save the setup.
     */
    private File _setupFile;

    public UniversalSpringCell(String cellName, String arguments)
    {
        super(cellName, arguments);
    }

    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        _environment = environment;

        try {
            /* FIXME: The following is a bad hack to workaround a
             * cells problem: There are no explicit lifecycle calls in
             * cells and thus no other way to start the cell outside
             * the constructor.
             */
            doInit();
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @Override
    protected void executeInit()
        throws Exception
    {
        /* Process command line arguments.
         */
        Args args = getArgs();
        checkArgument(args.argc() > 0, "Configuration location missing");

        _setupController = args.getOpt("setupController");
        info("Setup controller set to "
             + (_setupController == null ? "none" : _setupController));
        _setupFile =
            (!args.hasOption("setupFile"))
            ? null
            : new File(args.getOpt("setupFile"));
        _setupClass = args.getOpt("setupClass");

        checkArgument(_setupController == null || _setupClass != null,
                "Setup class must be specified when a setup controller is used");

        /* To ensure that all required file systems are mounted, the
         * admin may specify some required files. We will block until
         * they become available.
         */
        waitForFiles();

        /* Instantiate Spring application context. This will
         * eagerly instantiate all beans.
         */
        try {
            _context =
                new UniversalSpringCellApplicationContext(getArgs());
        } catch (BeanInstantiationException e) {
            Throwable t = e.getMostSpecificCause();
            Throwables.propagateIfPossible(t);
            String msg = "Failed to instantiate class " + e.getBeanClass().getName() +
                    ": " + t.getMessage();
            throw new CommandThrowableException(msg, t);
        } catch (BeanCreationException e) {
            Throwable t = e.getMostSpecificCause();
            Throwables.propagateIfPossible(t);
            String msg = "Failed to create bean '" + e.getBeanName() +
                    "' : " + t.getMessage();
            throw new CommandThrowableException(msg, t);
        }

        /* Cell threading is configurable through arguments to
         * UniversalSpringCell. The executors have to be created as
         * beans in the Spring file, however the names of the beans
         * are provided as cell arguments.
         */
        setupCellExecutors(args.getOpt("callbackExecutor"),
                           args.getOpt("messageExecutor"));

        /* This is a NOP except if somebody subclassed
         * UniversalSpringCell.
         */
        init();

        /* The timeout task is essential to handle cell message
         * timeouts.
         */
        startTimeoutTask();

        /* The setup may be provided as static configuration in the
         * domain context, as a setup file on disk or through a setup
         * controller cell.
         */
        executeSetup();

        /* Now that everything is instantiated and configured, we can
         * start the cell.
         */
        start();

        /* Run the final initialisation hooks.
         */
        for (CellLifeCycleAware bean: _lifeCycleAware) {
            bean.afterStart();
        }
    }

    /**
     * Closes the application context, which will shutdown all beans.
     */
    @Override
    public void cleanUp()
    {
        super.cleanUp();
        for (CellLifeCycleAware bean: _lifeCycleAware) {
            bean.beforeStop();
        }
        if (_context != null) {
            _context.close();
            _context = null;
        }
        _infoProviders.clear();
        _setupProviders.clear();
        _lifeCycleAware.clear();
    }

    private void setupCellExecutors(String callbackExecutor, String messageExecutor)
    {
        if (callbackExecutor != null) {
            Object executor = getBean(callbackExecutor);
            checkState(executor instanceof ThreadPoolExecutor,
                    "No such bean: " + callbackExecutor);
            getNucleus().setCallbackExecutor((ThreadPoolExecutor) executor);
        }

        if (messageExecutor != null) {
            Object executor = getBean(messageExecutor);
            checkState(executor instanceof ThreadPoolExecutor,
                    "No such bean: " + messageExecutor);
            getNucleus().setMessageExecutor((ThreadPoolExecutor) executor);
        }
    }

    private File firstMissing(File[] files)
    {
        for (File file: files) {
            if (!file.exists()) {
                return file;
            }
        }
        return null;
    }

    private void waitForFiles()
        throws InterruptedException
    {
        String s = getArgs().getOpt("waitForFiles");
        if (s != null && !s.trim().isEmpty()) {
            String[] paths = s.trim().split(":");
            File[] files = new File[paths.length];
            for (int i = 0; i < paths.length; i++) {
                files[i] = new File(paths[i]);
            }

            File missing;
            while ((missing = firstMissing(files)) != null) {
                warn(String.format("File missing: %s; sleeping %d seconds", missing, WAIT_FOR_FILE_SLEEP / 1000));
                Thread.sleep(WAIT_FOR_FILE_SLEEP);
            }
        }
    }

    private void executeSetup()
        throws IOException, CommandException
    {
        executeDefinedSetup();

        if( _setupFile != null && _setupFile.isFile() ) {
            for (CellSetupProvider provider: _setupProviders) {
                provider.beforeSetup();
            }

            execFile(_setupFile);

            for (CellSetupProvider provider: _setupProviders) {
                provider.afterSetup();
            }
        }
    }

    /**
     * Returns the singleton bean with a given name. Returns null if
     * such a bean does not exist.
     */
    private Object getBean(String name)
    {
        try {
            if (_context != null && _context.isSingleton(name)) {
                return _context.getBean(name);
            }
        } catch (NoSuchBeanDefinitionException e) {
        }
        return null;
    }

    /**
     * Returns the names of beans that depend of a given bean.
     */
    private List<String> getDependentBeans(String name)
    {
        if (_context == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(_context.getBeanFactory().getDependentBeans(name));
        }
    }

    /**
     * Returns the collection of singleton bean names.
     */
    private List<String> getBeanNames()
    {
        if (_context == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(_context.getBeanFactory().getSingletonNames());
        }
    }

    /**
     * Collects information from all registered info providers.
     */
    @Override
    public void getInfo(PrintWriter pw)
    {
        ConfigurableListableBeanFactory factory = _context.getBeanFactory();
        for (Map.Entry<CellInfoProvider,String> entry: _infoProviders.entrySet()) {
            CellInfoProvider provider = entry.getKey();
            String name = entry.getValue();
            try {
                BeanDefinition definition = factory.getBeanDefinition(name);
                String description = definition.getDescription();
                if (description != null) {
                    pw.println(String.format("--- %s (%s) ---",
                                             name, description));
                } else {
                    pw.println(String.format("--- %s ---", name));
                }
                provider.getInfo(pw);
                pw.println();
            } catch (NoSuchBeanDefinitionException e) {
                error("Failed to query bean definition for " + name);
            }
        }
    }

    /**
     * Collects information about the cell and returns these in a
     * CellInfo object. Information is collected from the CellAdapter
     * base class and from all beans implementing CellInfoProvider.
     */
    @Override
    public CellInfo getCellInfo()
    {
        CellInfo info = super.getCellInfo();
        for (CellInfoProvider provider : _infoProviders.keySet()) {
            info = provider.getCellInfo(info);
        }
        return info;
    }

    /**
     * Collects setup information from all registered setup providers.
     */
    protected void printSetup(PrintWriter pw)
    {
        pw.println("#\n# Created by " + getCellName() + "("
                   + getNucleus().getCellClass() + ") at " + (new Date()).toString()
                   + "\n#");
        for (CellSetupProvider provider: _setupProviders) {
            provider.printSetup(pw);
        }
    }

    public static final String hh_save = "[-sc=<setupController>|none] [-file=<filename>] # saves setup to disk or setup controller";
    public String ac_save(Args args)
        throws IOException, IllegalArgumentException, NoRouteToCellException
    {
        String controller = args.getOpt("sc");
        String file = args.getOpt("file");

        if ("none".equals(controller)) {
            controller = null;
            file = _setupFile.getPath();
        } else if (file == null && controller == null) {
            controller = _setupController;
            file = _setupFile.getPath();
        }

        checkArgument(file != null || controller != null,
                "Either a setup controller or setup file must be specified");

        if (controller != null) {
            checkState(!Strings.isNullOrEmpty(_setupClass),
                    "Cannot save to a setup controller since the cell has no setup class");

            try {
                StringWriter sw = new StringWriter();
                printSetup(new PrintWriter(sw));

                SetupInfoMessage info =
                    new SetupInfoMessage("put", getCellName(),
                                         _setupClass, sw.toString());

                sendMessage(new CellMessage(new CellPath(controller), info));
            } catch (NoRouteToCellException e) {
                throw new NoRouteToCellException("Failed to send setup to " + controller + ": " + e.getMessage());
            }
        }

        if (file != null) {
            File path = new File(file).getAbsoluteFile();
            File directory = path.getParentFile();
            File temp = File.createTempFile(path.getName(), null, directory);
            temp.deleteOnExit();

            try (PrintWriter pw = new PrintWriter(new FileWriter(temp))) {
                printSetup(pw);
            }


            renameWithBackup(temp, path);
        }

        return "";
    }

    private static void renameWithBackup(File source, File dest)
        throws IOException
    {
        File backup = new File(dest.getPath() + ".bak");

        if (dest.exists()) {
            if (!dest.isFile()) {
                throw new IOException("Cannot rename " + dest + ": Not a file");
            }
            if (backup.exists()) {
                if (!backup.isFile()) {
                    throw new IOException("Cannot delete " + backup + ": Not a file");
                }
                if (!backup.delete()) {
                    throw new IOException("Failed to delete " + backup);
                }
            }
            if (!dest.renameTo(backup)) {
                throw new IOException("Failed to rename " + dest);
            }
        }
        if (!source.renameTo(dest)) {
            throw new IOException("Failed to rename" + source);
        }
    }

    private void execFile(File setup)
        throws IOException, CommandException
    {
        BufferedReader br = new BufferedReader(new FileReader(setup));
        String line;
        try {
            int lineCount = 0;
            while ((line = br.readLine()) != null) {
                ++lineCount;

                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                if (line.charAt(0) == '#') {
                    continue;
                }
                try {
                    command(new Args(line));
                } catch (CommandException e) {
                    throw new CommandException("Error at " + setup + ":" +
                            lineCount + ": " + e.getMessage());
                }
            }
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                // ignored
            }
        }
    }

    public static final String hh_reload = "-yes";
    public static final String fh_reload =
        "This command destroys the current setup and replaces it" +
        "by the setup on disk.";
    public String ac_reload(Args args)
        throws IOException, CommandException
    {
        if (!args.hasOption("yes")) {
            return
                " This command destroys the current setup\n" +
                " and replaces it by the setup on disk\n" +
                " Please use 'reload -yes' if you really want\n" +
                " to do that.";
        }

        if (_setupFile != null && !_setupFile.exists()) {
            return String.format("Setup file [%s] does not exist", _setupFile);
        }

        executeSetup();

        return "";
    }

    /**
     * Should be renamed to 'info' when command interpreter learns
     * to handle overloaded commands.
     */
    public static final String hh_infox = "<bean>";
    public String ac_infox_$_1(Args args)
    {
        String name = args.argv(0);
        Object bean = getBean(name);
        if (CellInfoProvider.class.isInstance(bean)) {
            StringWriter s = new StringWriter();
            PrintWriter pw = new PrintWriter(s);
            ((CellInfoProvider)bean).getInfo(pw);
            return s.toString();
        }
        return "No such bean: " + name;
    }

    public static final String hh_bean_ls = "# lists running beans";
    public String ac_bean_ls(Args args)
    {
        String format = "%-30s %s\n";
        try (Formatter s = new Formatter(new StringBuilder())) {
            ConfigurableListableBeanFactory factory = _context.getBeanFactory();
            s.format(format, "Bean", "Description");
            s.format(format, "----", "-----------");
            for (String name : getBeanNames()) {
                if (!name.startsWith("org.springframework.")) {
                    try {
                        BeanDefinition definition = factory.getBeanDefinition(name);
                        String description = definition.getDescription();
                        s.format(format, name,
                                        (description != null ? description : "-"));
                    } catch (NoSuchBeanDefinitionException e) {
                        debug("Failed to query bean definition for " + name);
                    }
                }
            }

            return s.toString();
        }
    }

    public static final String hh_bean_dep = "# shows bean dependencies";
    public String ac_bean_dep(Args args)
    {
        String format = "%-30s %s\n";
        try (Formatter s = new Formatter(new StringBuilder())) {
            s.format(format, "Bean", "Used by");
            s.format(format, "----", "-------");
            for (String name : getBeanNames()) {
                s.format(format, name, Joiner.on(",").join(getDependentBeans(name)));
            }

            return s.toString();
        }
    }

    /**
     * If given a simple bean name, returns that bean (equivalent to
     * calling getBean). If given a compound name using the syntax of
     * BeanWrapper, then the respective property value is
     * returned. E.g., getBeanProperty("foo") returns the bean named
     * "foo", whereas getBeanProperty("foo.bar") returns the value of
     * the "bar" property of bean "foo".
     */
    private Object getBeanProperty(String s)
    {
        String[] a = s.split("\\.", 2);
        Object o = getBean(a[0]);
        if (o != null && a.length == 2) {
            BeanWrapper bean = new BeanWrapperImpl(o);
            o = bean.isReadableProperty(a[1])
                ? bean.getPropertyValue(a[1])
                : null;
        }
        return o;
    }

    private CharSequence valueToString(Object value)
    {
        if (value == null) {
            return "";
        } else if (value.getClass().isArray()) {
            Class<?> componentType = value.getClass().getComponentType();
            if (componentType == Boolean.TYPE) {
                return Arrays.toString((boolean[]) value);
            } else if (componentType == Byte.TYPE) {
                return Arrays.toString((byte[]) value);
            } else if (componentType == Character.TYPE) {
                return Arrays.toString((char[]) value);
            } else if (componentType == Double.TYPE) {
                return Arrays.toString((double[]) value);
            } else if (componentType == Float.TYPE) {
                return Arrays.toString((float[]) value);
            } else if (componentType == Integer.TYPE) {
                return Arrays.toString((int[]) value);
            } else if (componentType == Long.TYPE) {
                return Arrays.toString((long[]) value);
            } else if (componentType == Short.TYPE) {
                return Arrays.toString((short[]) value);
            } else {
                return Arrays.deepToString((Object[]) value);
            }
        } else {
            return value.toString();
        }
    }

    public static final String hh_bean_properties =
        "<bean> # shows properties of a bean";
    public String ac_bean_properties_$_1(Args args)
    {
        String name = args.argv(0);
        Object o = getBeanProperty(name);
        if (o != null) {
            StringBuilder s = new StringBuilder();
            BeanWrapper bean = new BeanWrapperImpl(o);
            for (PropertyDescriptor p : bean.getPropertyDescriptors()) {
                if (!p.isHidden()) {
                    String property = p.getName();
                    if (bean.isReadableProperty(property)) {
                        Object value = bean.getPropertyValue(property);
                        s.append(property).append('=').append(valueToString(value));
                        if (!bean.isWritableProperty(property)) {
                            s.append(" [read-only]");
                        }
                        s.append('\n');
                    }
                }
            }
            return s.toString();
        }
        return "No such bean: " + name;
    }

    public static final String hh_bean_property =
        "<property-name> # shows property of a bean";
    public String ac_bean_property_$_1(Args args)
    {
        String name = args.argv(0);
        Object o = getBeanProperty(name);
        return (o != null) ? String.valueOf(valueToString(o)) : ("No such bean: " + name);
    }

    /** Returns a formated name of a message class. */
    protected String getMessageName(Class<?> c)
    {
        String name = c.getSimpleName();
        int length = name.length();
        if ((length > 7) && name.endsWith("Message")) {
            name = name.substring(0, name.length() - 7);
        } else if ((length > 3) && name.endsWith("Msg")) {
            name = name.substring(0, name.length() - 3);
        }

        return name;
    }

    public static final String hh_bean_messages =
        "[<bean>] # shows message types handled by beans";
    public String ac_bean_messages_$_0_1(Args args)
    {
        switch (args.argc()) {
        case 0:
            Multimap<String,Class<? extends Serializable>> nameToClassMap = ArrayListMultimap.create();
            for (String name: getBeanNames()) {
                Object bean = getBean(name);
                if (CellMessageReceiver.class.isInstance(bean)) {
                    Collection<Class<? extends Serializable>> types =
                        _messageDispatcher.getMessageTypes(bean);
                    nameToClassMap.putAll(name, types);
                }
            }

            Multimap<Class<? extends Serializable>,String> classToNameMap = Multimaps.invertFrom(
                    nameToClassMap, ArrayListMultimap.<Class<? extends Serializable>,String>create());

            final String format = "%-40s %s\n";
            Formatter f = new Formatter(new StringBuilder());
            f.format(format, "Message", "Receivers");
            f.format(format, "-------", "---------");
            for (Map.Entry<Class<? extends Serializable>,Collection<String>> e: classToNameMap.asMap().entrySet()) {
                f.format(format,
                         getMessageName(e.getKey()),
                         Joiner.on(",").join(e.getValue()));
            }

            return f.toString();

        case 1:
            String name = args.argv(0);
            Object bean = getBean(name);
            if (CellMessageReceiver.class.isInstance(bean)) {
                StringBuilder s = new StringBuilder();
                Collection<Class<? extends Serializable>> types =
                    _messageDispatcher.getMessageTypes(bean);
                for (Class<? extends Serializable> t : types) {
                    s.append(getMessageName(t)).append('\n');
                }
                return s.toString();
            }
            return "No such bean: " + name;

        default:
            return "";
        }
    }

    public BeanQueryMessage messageArrived(BeanQueryAllPropertiesMessage message)
            throws CacheException
    {
        Map<String,Object> beans = Maps.newHashMap();
        for (String name : getBeanNames()) {
            beans.put(name, getBean(name));
        }
        message.setResult(serialize(beans));
        return message;
    }

    public BeanQueryMessage messageArrived(BeanQuerySinglePropertyMessage message)
            throws CacheException
    {
        Object o = getBeanProperty(message.getPropertyName());
        if (o == null) {
            throw new CacheException("No such property");
        }
        message.setResult(serialize(o));
        return message;
    }

    private final static Set<Class<?>> PRIMITIVES =
            Sets.<Class<?>>newHashSet(Byte.class, Byte.TYPE, Short.class, Short.TYPE,
                    Integer.class, Integer.TYPE, Long.class, Long.TYPE,
                    Float.class, Float.TYPE, Double.class, Double.TYPE,
                    Character.class, Character.TYPE, Boolean.class,
                    Boolean.TYPE, String.class);
    private final static Set<Class<?>> TERMINALS =
            Sets.<Class<?>>newHashSet(Class.class);

    private Object serialize(Set<Object> prune, Queue<Map.Entry<String,Object>> queue, Object o)
    {
        if (o == null || PRIMITIVES.contains(o.getClass())) {
            return o;
        } else if (TERMINALS.contains(o.getClass())) {
            return o.toString();
        } else if (o.getClass().isEnum()) {
            return o;
        } else if (o.getClass().isArray()) {
            int len = Array.getLength(o);
            List<Object> values = Lists.newArrayListWithCapacity(len);
            for (int i = 0; i < len; i++) {
                values.add(serialize(prune, queue, Array.get(o, i)));
            }
            return values;
        } else if (o instanceof Map) {
            Map<?,?> map = (Map<?,?>) o;
            Map<String,Object> values = Maps.newHashMapWithExpectedSize(map.size());
            for (Map.Entry<?,?> e: map.entrySet()) {
                values.put(String.valueOf(e.getKey()), serialize(prune, queue, e
                        .getValue()));
            }
            return values;
        } else if (o instanceof Set) {
            Collection<?> collection = (Collection<?>) o;
            Set<Object> values = Sets.newHashSetWithExpectedSize(collection.size());
            for (Object entry: collection) {
                values.add(serialize(prune, queue, entry));
            }
            return values;
        } else if (o instanceof Collection) {
            Collection<?> collection = (Collection<?>) o;
            List<Object> values = Lists.newArrayListWithCapacity(collection.size());
            for (Object entry: collection) {
                values.add(serialize(prune, queue, entry));
            }
            return values;
        } else if (o instanceof Iterable) {
            Iterable<?> collection = (Iterable<?>) o;
            List<Object> values = Lists.newArrayList();
            for (Object entry: collection) {
                values.add(serialize(prune, queue, entry));
            }
            return values;
        } else if (prune.contains(o)) {
            return o.toString();
        } else {
            prune.add(o);

            Map<String,Object> values = Maps.newHashMap();
            BeanWrapper bean = new BeanWrapperImpl(o);
            for (PropertyDescriptor p: bean.getPropertyDescriptors()) {
                if (!p.isHidden()) {
                    String property = p.getName();
                    if (bean.isReadableProperty(property)) {
                        try {
                            values.put(property, bean.getPropertyValue(property));
                        } catch (InvalidPropertyException | PropertyAccessException e) {
                            _log.debug("Failed to read {} of object of class {}: {}",
                                    property, o.getClass(), e.getMessage());
                        }
                    }
                }
            }
            if (values.isEmpty()) {
                return o.toString();
            }
            queue.addAll(values.entrySet());
            return values;
        }
    }

    /**
     * Breadth-first serialisation.
     *
     * Prunes the object tree to produce a tree even if o is a DAG or
     * contains cycles. Using a breadth-first search tends to produce
     * friendlier results when the object graph is pruned.
     */
    private Object serialize(Object o)
    {
        Set<Object> prune = Sets.newHashSet();
        Queue<Map.Entry<String,Object>> queue = new ArrayDeque<>();
        Object result = serialize(prune, queue, o);

        Map.Entry<String,Object> entry;
        while ((entry = queue.poll()) != null) {
            entry.setValue(serialize(prune, queue, entry.getValue()));
        }

        return result;
    }


    /**
     * Registers an info provider. Info providers contribute to the
     * result of the <code>getInfo</code> method.
     */
    public void addInfoProviderBean(CellInfoProvider bean, String name)
    {
        _infoProviders.put(bean, name);
    }

    /**
     * Add a message receiver. Message receiver receive messages via
     * message handlers (see CellMessageDispatcher).
     */
    public void addMessageReceiver(CellMessageReceiver bean)
    {
        addMessageListener(bean);
    }

    /**
     * Add a message sender. Message senders can send cell messages
     * via a cell endpoint.
     */
    public void addMessageSender(CellMessageSender bean)
    {
        bean.setCellEndpoint(this);
    }

    /**
     * Registers a setup provider. Setup providers contribute to the
     * result of the <code>save</code> method.
     */
    public void addSetupProviderBean(CellSetupProvider bean)
    {
        _setupProviders.add(bean);
    }

    /**
     * Registers a life cycle aware bean. Life cycle aware beans are
     * notified about cell start and stop events.
     */
    public void addLifeCycleAwareBean(CellLifeCycleAware bean)
    {
        _lifeCycleAware.add(bean);
    }

    /**
     * Registers a thread factory aware bean. Thread factory aware
     * bean provide hooks for registering thread factories. This
     * method registers the Cell nulceus thread factory on the bean.
     */
    public void addThreadFactoryAwareBean(ThreadFactoryAware bean)
    {
        bean.setThreadFactory(getNucleus());
    }


    /**
     * Part of the BeanPostProcessor implementation. Recognizes beans
     * implementing CellCommandListener, CellInfoProvider,
     * CellCommunicationAware, CellSetupProvider and
     * ThreadFactoryAware and performs the necessary wiring.
     */
    @Override
    public Object postProcessBeforeInitialization(Object bean,
                                                  String beanName)
        throws BeansException
    {
        if (bean instanceof CellCommandListener) {
            addCommandListener(bean);
        }

        if (bean instanceof CellInfoProvider) {
            addInfoProviderBean((CellInfoProvider) bean, beanName);
        }

        if (bean instanceof CellMessageReceiver) {
            addMessageReceiver((CellMessageReceiver) bean);
        }

        if (bean instanceof CellMessageSender) {
            addMessageSender((CellMessageSender) bean);
        }

        if (bean instanceof CellSetupProvider) {
            addSetupProviderBean((CellSetupProvider) bean);
        }

        if (bean instanceof CellLifeCycleAware) {
            addLifeCycleAwareBean((CellLifeCycleAware) bean);
        }

        if (bean instanceof ThreadFactoryAware) {
            addThreadFactoryAwareBean((ThreadFactoryAware) bean);
        }

        if (bean instanceof EnvironmentAware) {
            ((EnvironmentAware) bean).setEnvironment(_environment);
        }

        if (bean instanceof DomainContextAware) {
            ((DomainContextAware) bean).setDomainContext(getDomainContext());
        }

        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean,
                                                 String beanName)
        throws BeansException
    {
        return bean;
    }

    class UniversalSpringCellApplicationContext
        extends ClassPathXmlApplicationContext
    {
        UniversalSpringCellApplicationContext(Args args)
        {
            super(args.argv(0));
        }

        private ByteArrayResource getArgumentsResource()
        {
            Args args = new Args(getArgs());
            args.shift();

            Properties properties = new Properties();
            for (Map.Entry<String, Object> entry : _environment.entrySet()) {
                properties.setProperty(entry.getKey(), entry.getValue().toString());
            }
            for (Map.Entry<String, String> option : args.optionsAsMap().entrySet()) {
                properties.setProperty(option.getKey(), option.getValue());
            }
            String arguments =
                    args.toString().replaceAll("-\\$\\{[0-9]+\\}", "");
            properties.setProperty("arguments", arguments);


            /* Convert to byte array form such that we can make it
             * available as a Spring resource.
             */
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                properties.store(out, "");
            } catch (IOException e) {
                /* This should never happen with a ByteArrayOutputStream.
                 */
                throw Throwables.propagate(e);
            }
            final byte[] _domainContext = out.toByteArray();

            return new ByteArrayResource(_domainContext) {
                /**
                 * Fake file name to make
                 * PropertyPlaceholderConfigurer happy.
                 */
                @Override
                public String getFilename()
                {
                    return "arguments.properties";
                }
            };
        }

        @Override
        public Resource getResource(String location)
        {
            if (location.startsWith("arguments:")) {
                return getArgumentsResource();
            } else {
                return super.getResource(location);
            }
        }

        @Override
        protected void customizeBeanFactory(DefaultListableBeanFactory beanFactory)
        {
            super.customizeBeanFactory(beanFactory);
            beanFactory.addBeanPostProcessor(UniversalSpringCell.this);
        }

        @Override
        public synchronized ConfigurableEnvironment getEnvironment() {
            ConfigurableEnvironment environment = super.getEnvironment();

            Args args = getArgs();

            if(args.hasOption("profiles")) {
                String[] profiles = args.getOption("profiles").split(",");

                if(!Arrays.equals(profiles, environment.getActiveProfiles())) {
                    environment.setActiveProfiles(profiles);
                }
            }

            return environment;
        }
    }
}
