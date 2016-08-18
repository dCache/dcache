package org.dcache.cells;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.AbstractNestablePropertyAccessor;
import org.springframework.beans.BeanInstantiationException;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.InvalidPropertyException;
import org.springframework.beans.NotReadablePropertyException;
import org.springframework.beans.PropertyAccessException;
import org.springframework.beans.PropertyAccessorUtils;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import javax.annotation.concurrent.GuardedBy;

import java.beans.PropertyDescriptor;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellArgsAware;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoAware;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.cells.nucleus.DomainContextAware;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.CommandException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandPanicException;
import dmg.util.CommandThrowableException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.util.Args;
import org.dcache.util.cli.CommandExecutor;
import org.dcache.vehicles.BeanQueryAllPropertiesMessage;
import org.dcache.vehicles.BeanQueryMessage;
import org.dcache.vehicles.BeanQuerySinglePropertyMessage;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(UniversalSpringCell.class);

    private static final long WAIT_FOR_FILE_SLEEP = 30000;

    private static final Set<Class<?>> PRIMITIVE_TYPES =
            Sets.newHashSet(Byte.class, Byte.TYPE, Short.class, Short.TYPE,
                            Integer.class, Integer.TYPE, Long.class, Long.TYPE,
                            Float.class, Float.TYPE, Double.class, Double.TYPE,
                            Character.class, Character.TYPE, Boolean.class,
                            Boolean.TYPE, String.class);
    private static final Class<?>[] TERMINAL_TYPES = new Class<?>[] { Class.class, ApplicationContext.class };
    private static final Class<?>[] HIDDEN_TYPES = new Class<?>[] { ApplicationContext.class, AutoCloseable.class };

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
    private final Map<String, CellInfoProvider> _infoProviders =
        new TreeMap<>();

    /**
     * List of registered setup providers. Sorted to maintain
     * consistent ordering.
     */
    private final Map<String, CellSetupProvider> _setupProviders =
        new TreeMap<>();

    /**
     * List of registered life cycle aware beans. Sorted to maintain
     * consistent ordering.
     */
    private final Map<String, CellLifeCycleAware> _lifeCycleAware =
        new TreeMap<>();

    /**
     * Command interpreter for processing setup files.
     */
    private final CommandInterpreter _setupInterpreter = new CommandInterpreter();

    /**
     * Setup to execute during start and to which to save the setup.
     */
    private File _setupFile;

    private SetupManager _setupManager;

    public UniversalSpringCell(String cellName, String arguments)
    {
        super(cellName, arguments);
    }

    @Override
    protected Serializable executeCommand(CommandExecutor command, Args args) throws CommandException
    {
        if (command.getImplementation().isAnnotationPresent(CellSetupProvider.AffectsSetup.class)) {
            return _setupManager.execute(command, args);
        }
        return super.executeCommand(command, args);
    }

    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        _environment = environment;
    }

    @Override
    protected void startUp() throws Exception
    {
        super.startUp();

        /* Process command line arguments.
         */
        Args args = getArgs();
        checkArgument(args.argc() > 0, "Configuration location missing");

        _setupFile =
                (!args.hasOption("setupFile"))
                ? null
                : new File(args.getOption("setupFile"));

        if (_setupFile != null) {
            addCommandListener(new SetupCommandListener());
        }

        /* The setup may be provided as a setup file on disk or through zookeeper.
         */
        if (args.hasOption("setupNode")) {
            _setupManager = new ZooKeeperSetupManager(_setupFile, args.getOption("setupNode"));
        } else if (_setupFile != null) {
            _setupManager = new LocalSetupManager(_setupFile);
        } else {
            _setupManager = new InMemorySetupManager();
        }

        /* To ensure that all required file systems are mounted, the
         * admin may specify some required files. We will block until
         * they become available.
         */
        waitForFiles();

        /* Instantiate Spring application context. This will
         * eagerly instantiate all beans.
         */
        createContext();

        /* The setup may be provided as static configuration in the
         * domain context.
         */
        executeSetupContext();

        /* Starting the setup manager loads the external setup.
         */
        _setupManager.start();
    }

    @Override
    protected void started()
    {
        /* Run the final initialisation hooks.
         */
        for (CellLifeCycleAware bean: _lifeCycleAware.values()) {
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
        if (_setupManager != null) {
            _setupManager.close();
        }
        for (CellLifeCycleAware bean: _lifeCycleAware.values()) {
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
                LOGGER.warn("File missing: {}; sleeping {} seconds", missing, WAIT_FOR_FILE_SLEEP / 1000);
                Thread.sleep(WAIT_FOR_FILE_SLEEP);
            }
        }
    }

    private byte[] loadSetup(File file) throws CommandException
    {
        try {
            byte[] data = readAllBytes(file.toPath());
            testSetup(file.toString(), data);
            return data;
        } catch (IOException e) {
            throw new CommandException("Failed to load " + file.toPath() + ": "
                    + e.getMessage(), e);
        }
    }

    private void testSetup(String source, byte[] data)
            throws CommandException
    {
        CommandInterpreter mockInterpreter = new CommandInterpreter();
        _setupProviders.values().stream().map(CellSetupProvider::mock).forEach(mockInterpreter::addCommandListener);
        executeSetup(mockInterpreter, source, data);
    }

    private void executeSetup(String source, byte[] data)
            throws CommandException
    {
        try {
            _lifeCycleAware.values().forEach(CellLifeCycleAware::beforeSetup);
            try {
                executeSetup(_setupInterpreter, source, data);
            } finally {
                _lifeCycleAware.values().forEach(CellLifeCycleAware::afterSetup);
            }
        } catch (RuntimeException e) {
            kill();
            throw new CommandPanicException("Possible bug detected during setup execution " +
                                            "and service must be restarted: " + e.toString(), e);
        }
    }

    private void executeSetup(CommandInterpreter interpreter, String source, byte[] data)
            throws CommandException
    {
        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(new ByteArrayInputStream(data), UTF_8));
            int lineCount = 1;
            for (String line = in.readLine(); line != null; line = in.readLine(), lineCount++) {
                line = line.trim();
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                try {
                    interpreter.command(new Args(line));
                } catch (CommandPanicException e) {
                    throw new CommandPanicException("Error at " + source + ":" + lineCount + ": " + e.getMessage(), e);
                } catch (CommandException e) {
                    throw new CommandThrowableException("Error at " + source + ":" + lineCount + ": " + e.getMessage(), e);
                }
            }
        } catch (IOException e) {
            Throwables.propagate(e); // Should not be possible
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
        /* getInfo is called during cell initialization, but the context isn't fully
         * initialized until cell initialization is done.
         */
        ConfigurableApplicationContext context = _context;
        if (context != null) {
            ConfigurableListableBeanFactory factory = context.getBeanFactory();
            for (Map.Entry<String, CellInfoProvider> entry : _infoProviders.entrySet()) {
                String name = entry.getKey();
                CellInfoProvider provider = entry.getValue();
                try {
                    BeanDefinition definition = factory.getBeanDefinition(name);
                    String description = definition.getDescription();
                    if (description != null) {
                        pw.println(String.format("--- %s (%s) ---",
                                                 name, description));
                    } else {
                        pw.println(String.format("--- %s ---", name));
                    }
                } catch (NoSuchBeanDefinitionException e) {
                    pw.println(String.format("--- %s ---", name));
                }
                provider.getInfo(pw);
                pw.println();
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
        for (CellInfoProvider provider : _infoProviders.values()) {
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
        for (CellSetupProvider provider: _setupProviders.values()) {
            provider.printSetup(pw);
        }
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

    public class SetupCommandListener implements CellCommandListener
    {
        @Command(name = "save", hint = "save service configuration")
        public class SaveCommand implements Callable<String>
        {
            @Option(name = "file", usage = "Path of setup file.")
            File file = _setupFile;

            @Override
            public String call()
                    throws IOException, IllegalArgumentException
            {
                File path = file.getAbsoluteFile();
                File directory = path.getParentFile();
                File temp = File.createTempFile(path.getName(), null, directory);
                temp.deleteOnExit();

                try (PrintWriter pw = new PrintWriter(new FileWriter(temp))) {
                    printSetup(pw);
                }

                renameWithBackup(temp, path);

                return "";
            }
        }

        @Command(name = "reload",
                 hint = "reload service configuration",
                 description = "This command destroys the current setup and replaces it " +
                         "by the setup on disk.")
        public class ReloadCommand implements Callable<String>
        {
            @Option(name = "yes", required = true,
                    usage = "Confirms that the current setup should be destroyed and replaced " +
                            "with the one on disk.")
            boolean confirmed;

            @Override
            public String call() throws IOException, CommandException, IllegalArgumentException
            {
                checkArgument(confirmed, "Required option is missing.");
                if (_setupFile != null && !_setupFile.exists()) {
                    return String.format("Setup file [%s] does not exist", _setupFile);
                }
                _setupManager.load(_setupFile);
                return "";
            }
        }
    }

    @Command(name = "infox", hint = "show status information about bean")
    public class InfoxCommand implements Callable<String>
    {
        @Argument(metaVar = "bean")
        String name;

        @Override
        public String call()
        {
            Object bean = getBean(name);
            if (CellInfoProvider.class.isInstance(bean)) {
                StringWriter s = new StringWriter();
                PrintWriter pw = new PrintWriter(s);
                ((CellInfoProvider)bean).getInfo(pw);
                return s.toString();
            }
            return "No such bean: " + name;
        }
    }

    @Command(name = "bean ls", hint = "list running beans",
             description = "Lists the bean in this service. Services are composed of components " +
                     "called beans. Each bean implements a part of a service.")
    public class BeanLsCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            String format = "%-30s %s\n";
            try (Formatter s = new Formatter(new StringBuilder())) {
                ConfigurableListableBeanFactory factory = _context.getBeanFactory();
                s.format(format, "Bean", "Description");
                s.format(format, "----", "-----------");
                for (String name : factory.getBeanDefinitionNames()) {
                    if (!name.startsWith("org.springframework.")) {
                        BeanDefinition definition = factory.getBeanDefinition(name);
                        String description = definition.getDescription();
                        s.format(format, name,
                                 (description != null ? description : "-"));
                    }
                }

                return s.toString();
            }
        }
    }

    @Command(name = "bean dep", hint = "show bean dependencies",
             description = "Shows dependencies between beans. This information is mostly useful " +
                     "as a debugging aid.")
    public class BeanDepCommand implements Callable<String>
    {
        @Override
        public String call()
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
            BeanWrapper bean = new RestrictedBeanWrapper(o);
            o = bean.isReadableProperty(a[1])
                ? bean.getPropertyValue(a[1])
                : null;
        }
        return o;
    }

    @Command(name = "bean properties", hint = "show properties of a bean",
             description = "Shows the properties of a bean and their values. Each bean exposes " +
                     "some properties that can be read with this command.")
    public class BeanPropertiesCommand implements Callable<String>
    {
        @Argument(metaVar = "bean", usage = "Name of bean or property reference.")
        String name;

        @Override
        public String call() throws InvalidPropertyException
        {
            Object o = getBeanProperty(name);
            if (o != null) {
                StringBuilder s = new StringBuilder();
                BeanWrapper bean = new RestrictedBeanWrapper(o);
                for (PropertyDescriptor p : bean.getPropertyDescriptors()) {
                    String property = p.getName();
                    if (bean.isReadableProperty(property)) {
                        s.append(property).append('=');
                        try {
                            s.append(org.dcache.util.Strings.toString(bean.getPropertyValue(property)));
                            if (!bean.isWritableProperty(property)) {
                                s.append(" [read-only]");
                            }
                        } catch (InvalidPropertyException e) {
                            s.append(" [invalid]");
                        }
                        s.append('\n');
                    }
                }
                return s.toString();
            }
            return "No such bean: " + name;
        }
    }

    @Command(name = "bean property", hint = "show property of a bean")
    public class BeanPropertyCommand implements Callable<String>
    {
        @Argument(metaVar = "property")
        String name;

        @Override
        public String call() throws InvalidPropertyException
        {
            Object o = getBeanProperty(name);
            return (o != null) ? org.dcache.util.Strings.toMultilineString(o) : ("No such property: " + name);
        }
    }


    /** Returns a formatted name of a message class. */
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

    @Command(name = "bean messages", hint = "show message types handled by beans",
             description = "Shows messages processed by each bean. dCache services communicate " +
                     "by message passing. Each bean may be able to handle zero or more messages and " +
                     "this command shows which messages.")
    public class BeanMessagesCommand implements Callable<String>
    {
        @Argument(required = false, metaVar = "bean",
                  usage = "Bean name. If omitted, the information is displayed for all beans.")
        String name;

        @Override
        public String call()
        {
            if (name == null) {
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
                        nameToClassMap, ArrayListMultimap.create());

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
            } else {
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
            }
        }
    }

    public BeanQueryMessage messageArrived(BeanQueryAllPropertiesMessage message)
            throws CacheException
    {
        Map<String,Object> beans = Maps.newHashMap();
        ConfigurableListableBeanFactory factory = _context.getBeanFactory();
        for (String name : factory.getBeanDefinitionNames()) {
            if (!name.startsWith("org.springframework.")) {
                beans.put(name, getBean(name));
            }
        }
        message.setResult(serialize(beans));
        return message;
    }

    public BeanQueryMessage messageArrived(BeanQuerySinglePropertyMessage message)
            throws CacheException
    {
        String[] a = message.getPropertyName().split("\\.", 2);
        String name = a[0];
        Object o = null;
        if (_context != null && _context.isSingleton(name) && _context.containsBeanDefinition(name)) {
            o = _context.getBean(name);
            if (a.length == 2) {
                String propertyName = a[1];
                BeanWrapper bean = new RestrictedBeanWrapper(o);
                o = bean.isReadableProperty(propertyName)
                        ? bean.getPropertyValue(propertyName)
                        : null;
            }
        }
        if (o == null) {
            throw new CacheException("No such property");
        }
        message.setResult(serialize(o));
        return message;
    }

    /**
     * Returns true if {@code clazz} is assignable to any of the classes in {@code classes}, false
     * otherwise.
     */
    private boolean isAssignableTo(Class<?> clazz, Class<?>... classes)
    {
        for (Class<?> aClass : classes) {
            if (aClass.isAssignableFrom(clazz)) {
                return true;
            }
        }
        return false;
    }

    private Object serialize(Set<Object> prune, Queue<Map.Entry<String,Object>> queue, Object o)
    {
        if (o == null || PRIMITIVE_TYPES.contains(o.getClass())) {
            return o;
        } else if (isAssignableTo(o.getClass(), TERMINAL_TYPES)) {
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
            BeanWrapper bean = new RestrictedBeanWrapper(o);
            for (PropertyDescriptor p: bean.getPropertyDescriptors()) {
                String property = p.getName();
                if (bean.isReadableProperty(property)) {
                    try {
                        values.put(property, bean.getPropertyValue(property));
                    } catch (InvalidPropertyException | PropertyAccessException e) {
                        LOGGER.debug("Failed to read {} of object of class {}: {}",
                                property, o.getClass(), e.getMessage());
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
        _infoProviders.put(name, bean);
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
     * Add provider of CellInfo to a CellInfoAware bean.
     */
    public void addCellInfoAware(CellInfoAware bean)
    {
        bean.setCellInfoSupplier(this::getCellInfo);
    }

    /**
     * Add the cell's identity
     */
    public void addCellIdentity(CellIdentityAware bean)
    {
        bean.setCellAddress(getNucleus().getThisAddress());
    }

    /**
     * Registers a setup provider. Setup providers contribute to the
     * result of the <code>save</code> method.
     */
    public void addSetupProviderBean(CellSetupProvider bean, String name)
    {
        _setupProviders.put(name, bean);
        _setupInterpreter.addCommandListener(bean);
    }

    /**
     * Registers a life cycle aware bean. Life cycle aware beans are
     * notified about cell start and stop events.
     */
    public void addLifeCycleAwareBean(CellLifeCycleAware bean, String name)
    {
        _lifeCycleAware.put(name, bean);
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

        if (bean instanceof CellInfoAware) {
            addCellInfoAware((CellInfoAware) bean);
        }

        if (bean instanceof CellIdentityAware) {
            addCellIdentity((CellIdentityAware) bean);
        }

        if (bean instanceof CellSetupProvider) {
            addSetupProviderBean((CellSetupProvider) bean, beanName);
        }

        if (bean instanceof CellLifeCycleAware) {
            addLifeCycleAwareBean((CellLifeCycleAware) bean, beanName);
        }

        if (bean instanceof EnvironmentAware) {
            ((EnvironmentAware) bean).setEnvironment(_environment);
        }

        if (bean instanceof CellArgsAware) {
            ((CellArgsAware)bean).setCellArgs(getArgs());
        }

        if (bean instanceof DomainContextAware) {
            ((DomainContextAware) bean).setDomainContext(getDomainContext());
        }

        if (bean instanceof CellEventListener) {
            addCellEventListener((CellEventListener) bean);
        }

        if (bean instanceof CuratorFrameworkAware) {
            ((CuratorFrameworkAware) bean).setCuratorFramework(getCuratorFramework());
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

    private void createContext() throws CommandThrowableException
    {
        ClassPathXmlApplicationContext context;
        Args args = getArgs();
        try {
            context = new ClassPathXmlApplicationContext();
            context.setConfigLocations(args.argv(0));
            context.addBeanFactoryPostProcessor(
                    beanFactory -> beanFactory.addBeanPostProcessor(UniversalSpringCell.this));
            context.addBeanFactoryPostProcessor(
                    beanFactory -> beanFactory.registerSingleton(
                            "message-executor", (Executor) this::invokeOnMessageThread)
            );

            ConfigurableEnvironment environment = context.getEnvironment();
            environment.getPropertySources().addFirst(
                    new MapPropertySource("environment", _environment));
            environment.getPropertySources().addFirst(
                    new MapPropertySource("options", Maps.newHashMap(args.optionsAsMap())));
            environment.getPropertySources().addFirst(
                    new PropertySource<Object>("arguments")
                    {
                        private final String arguments;

                        {
                            Args args = new Args(getArgs());
                            args.shift();
                            arguments = args.toString().replaceAll("-\\$\\{[0-9]+\\}", "");
                        }

                        @Override
                        public Object getProperty(String name)
                        {
                            return name.equals("arguments") ? arguments : null;
                        }
                    }
            );
            if (args.hasOption("profiles")) {
                environment.setActiveProfiles(args.getOption("profiles").split(","));
            }
            context.refresh();
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

        _context = context;
    }

    /**
     * BeanWrapper that restricts access to certain private properties that should not be exposed.
     */
    private class RestrictedBeanWrapper extends BeanWrapperImpl
    {
        private RestrictedBeanWrapper(Object object)
        {
            super(object);
        }

        private RestrictedBeanWrapper(Object object, String nestedPath, Object rootObject)
        {
            super(object, nestedPath, rootObject);
        }

        @Override
        protected AbstractNestablePropertyAccessor getPropertyAccessorForPropertyPath(String propertyPath)
        {
            int pos = PropertyAccessorUtils.getFirstNestedPropertySeparatorIndex(propertyPath);
            if (pos > -1) {
                String nestedProperty = propertyPath.substring(0, pos);
                if (!isReadableProperty(nestedProperty)) {
                    throw new NotReadablePropertyException(getRootClass(), nestedProperty);
                }
            }
            return super.getPropertyAccessorForPropertyPath(propertyPath);
        }

        @Override
        public boolean isReadableProperty(String propertyName)
        {
            if (isAllowedName(propertyName)) {
                try {
                    PropertyDescriptor pd = getPropertyDescriptor(propertyName);
                    if (pd.getReadMethod() != null && !pd.isHidden() && isAllowedType(pd.getPropertyType())) {
                        return true;
                    }
                } catch (InvalidPropertyException e) {
                    try {
                        Object value = super.getPropertyValue(propertyName);
                        if (value == null || isAllowedType(value.getClass())) {
                            return true;
                        }
                    } catch (InvalidPropertyException ignored) {
                    }
                }
            }
            return false;
        }

        private boolean isAllowedType(Class<?> clazz)
        {
            return !isAssignableTo(clazz, HIDDEN_TYPES);
        }

        private boolean isAllowedName(String propertyName)
        {
            String s = propertyName.toLowerCase();
            return !s.contains("password") && !s.contains("username");
        }

        @Override
        public Object getPropertyValue(String propertyName) throws BeansException
        {
            if (!isReadableProperty(propertyName)) {
                throw new NotReadablePropertyException(getRootClass(), propertyName);
            }
            return super.getPropertyValue(propertyName);
        }

        @Override
        protected BeanWrapperImpl newNestedPropertyAccessor(Object object, String nestedPath) {
            return new RestrictedBeanWrapper(object, nestedPath, this);
        }
    }

    /**
     * Setup related operations are delegated to an implementation of this interface.
     */
    private interface SetupManager
    {
        /** Called on startup. Should load the initial setup. */
        void start() throws CommandException;

        /** Called on shutdown. Should release any resources held. */
        void close();

        /**
         * Called when a setup command is issued. The implementation should execute the given command
         * and optionally synchronize it with external representations.
         */
        Serializable execute(CommandExecutor command, Args args) throws CommandException;

        /**
         * Called when the admin issues the reload command. Implementations should parse
         * the given file and update the configuration accordingly.
         */
        void load(File file) throws CommandException;
    }

    /**
     * Setup manager without any external representation of the setup.
     */
    private class InMemorySetupManager implements SetupManager
    {
        private int version;

        @Override
        public void start() throws CommandException
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public synchronized Serializable execute(CommandExecutor command, Args args) throws CommandException
        {
            Serializable result = command.execute(args);
            notifyListeners();
            return result;
        }

        @Override
        public void load(File file) throws CommandException
        {
        }

        @GuardedBy("this")
        protected void notifyListeners()
        {
            version++;
            _lifeCycleAware.values().forEach(b -> b.setupChanged(version));
        }
    }

    /**
     * Setup manager that uses a local file as a configuration source.
     */
    private class LocalSetupManager extends InMemorySetupManager
    {
        private final File _file;

        public LocalSetupManager(File file)
        {
            _file = file;
        }

        @Override
        public void start() throws CommandException
        {
            if (_file != null && _file.isFile()) {
                load(_file);
            }
        }

        @Override
        public synchronized void load(File file) throws CommandException
        {
            executeSetup(file.toString(), loadSetup(file));
            notifyListeners();
        }
    }

    /**
     * Setup manager that uses an local file as an initial configuration source and afterwards
     * keeps the current setup in sync with a zookeeper node.
     */
    private class ZooKeeperSetupManager implements NodeCacheListener, SetupManager
    {
        private final CuratorFramework _curator;

        /**
         * Local file containing the persistent setup.
         */
        private final File _file;

        /**
         * Path to ZooKeeper node to keep in sync with the current setup.
         */
        private final String _node;

        /**
         * Cache of the ZooKeeper node identified by {@code _node}.
         */
        private final NodeCache _cache;

        /**
         * Stat of the last value loaded from the ZooKeeper node identified by {@code _node}.
         */
        private Stat _current;

        public ZooKeeperSetupManager(File file, String node)
        {
            _curator = getCuratorFramework();
            _file = file;
            _node = node;
            _cache = new NodeCache(_curator, _node);
            _cache.getListenable().addListener(this);
        }

        @Override
        public synchronized void start() throws CommandException
        {
            byte[] data;
            if (_file != null && _file.isFile()) {
                data = loadSetup(_file);
            } else {
                data = getCurrentSetup();
            }

            // Seed setup in zookeeper
            try {
                _curator.create().creatingParentContainersIfNeeded().forPath(_node, data);
            } catch (KeeperException.NodeExistsException e) {
                LOGGER.info("Not seeding setup in ZooKeeper as such a setup already exists.");
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new CommandThrowableException("Failed to create " +
                        "zookeeper node " + _node + ": " + e.getMessage(), e);
            }

            // Blocking start of the setup node cache - the service fails to start if zookeeper is unavailable
            try {
                _cache.start(true);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new CommandThrowableException("Failed while waiting for " +
                        "zookeeper: " + e.getMessage(), e);
            }

            // Load initial setup from zookeeper
            ChildData currentData = _cache.getCurrentData();
            if (currentData == null) {
                throw new CommandPanicException("Setup reload failed because " + _node + " in ZooKeeper disappeared. Service must be restarted.");
            }
            apply(currentData);
        }

        @Override
        public void close()
        {
            CloseableUtils.closeQuietly(_cache);
        }

        @Override
        public synchronized Serializable execute(CommandExecutor command, Args args) throws CommandException
        {
            Serializable result = command.execute(args);

            /* REVISIT: _current is null if start was not called yet - this happens because the setup context is
             * executed before starting the setup manager.
             */
            if (_current != null) {
                try {
                    byte[] data = getCurrentSetup();
                    _current = _curator.setData().withVersion(_current.getVersion()).forPath(_node, data);
                } catch (BadVersionException e) {
                    try {
                        _cache.rebuild();
                    } catch (Exception suppressed) {
                        e.addSuppressed(e);
                    }
                    rollback(e);
                    throw new CommandThrowableException(
                            "Setup command failed due to concurrent update in ZooKeeper.", e);
                } catch (NoNodeException e) {
                    rollback(e);
                    throw new CommandPanicException(
                            "Setup command failed because " + _node + " in ZooKeeper disappeared. Service must be restarted.", e);
                } catch (KeeperException e) {
                    rollback(e);
                    throw new CommandThrowableException(
                            "Setup command fail due to ZooKeeper failure: " + e.getMessage(), e);
                } catch (InterruptedException e) {
                    rollback(e);
                    throw new CommandThrowableException("Setup command was interrupted.", e);
                } catch (Exception e) {
                    rollback(e);
                    throw new CommandPanicException("Setup command failed unexpectedly: " + e, e);
                }
            }
            return result;
        }

        @Override
        public void load(File file) throws CommandException
        {
            try {
                _curator.setData().forPath(_node, loadSetup(file));
            } catch (NoNodeException e) {
                throw new CommandPanicException("Setup reload failed because " + _node + " in ZooKeeper disappeared. Service must be restarted.", e);
            } catch (KeeperException e) {
                throw new CommandThrowableException("Setup reload fail due to ZooKeeper failure: " + e.getMessage(), e);
            } catch (InterruptedException e) {
                throw new CommandThrowableException("Setup reload was interrupted.", e);
            } catch (Exception e) {
                throw new CommandPanicException("Setup reload failed unexpectedly: " + e, e);
            }
        }

        @Override
        public synchronized void nodeChanged() throws Exception
        {
            ChildData newData = _cache.getCurrentData();
            if (newData == null) {
                LOGGER.error("Setup node " + _node + " in ZooKeeper disappeared. Service must be restarted.");
                kill();
            } else if (newData.getStat().getVersion() > _current.getVersion()) {
                apply(newData);
            }
        }

        private void rollback(Exception cause) throws CommandPanicException
        {
            try {
                ChildData currentData = _cache.getCurrentData();
                if (currentData == null) {
                    kill();
                    throw new CommandPanicException("Setup node " + _node + " in ZooKeeper disappeared.");
                }
                apply(currentData);
            } catch (CommandException e) {
                e.addSuppressed(cause);
                throw new CommandPanicException("Saving setup to ZooKeeper failed (" + cause.getMessage() + "); however the rollback failed too (" + e.getMessage() + "). Service must be restarted.", e);
            }
        }

        private byte[] getCurrentSetup()
        {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                printSetup(pw);
                pw.close();
                sw.close();
                return sw.getBuffer().toString().getBytes(UTF_8);
            } catch (IOException e) {
                throw Throwables.propagate(e); // Should not be possible
            }
        }

        @GuardedBy("this")
        private void apply(ChildData setup) throws CommandException
        {
            testSetup("zookeeper:" + _node, setup.getData());
            executeSetup("zookeeper:" + _node, setup.getData());
            _current = setup.getStat();
            _lifeCycleAware.values().forEach(b -> b.setupChanged(_current.getVersion()));
        }
    }
}
