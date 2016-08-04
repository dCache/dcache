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

import java.beans.PropertyDescriptor;
import java.io.BufferedReader;
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
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.cells.nucleus.DomainContextAware;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.cells.services.SetupInfoMessage;
import dmg.util.CommandException;
import dmg.util.CommandThrowableException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.util.Args;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(UniversalSpringCell.class);

    private static final long WAIT_FOR_FILE_SLEEP = 30000;

    private static final Set<Class<?>> PRIMITIVE_TYPES =
            Sets.<Class<?>>newHashSet(Byte.class, Byte.TYPE, Short.class, Short.TYPE,
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
        LOGGER.info("Setup controller set to "
                + (_setupController == null ? "none" : _setupController));
        _setupFile =
            (!args.hasOption("setupFile"))
            ? null
            : new File(args.getOpt("setupFile"));
        _setupClass = args.getOpt("setupClass");

        checkArgument(_setupController == null || _setupClass != null,
                "Setup class must be specified when a setup controller is used");

        if (_setupFile != null || _setupClass != null) {
            addCommandListener(new SetupCommandListener());
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

        try {
        /* Cell threading is configurable through arguments to
         * UniversalSpringCell. The executors have to be created as
         * beans in the Spring file, however the names of the beans
         * are provided as cell arguments.
         */
            setupCellExecutors(args.getOpt("messageExecutor"));
        } catch (IllegalStateException e) {
            LOGGER.debug("Aborting cell initialization due to illegal state exception while setting executors.");
            return;
        }

        /* This is a NOP except if somebody subclassed
         * UniversalSpringCell.
         */
        init();

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

    private void setupCellExecutors(String messageExecutor)
    {
        if (messageExecutor != null) {
            Object executor = getBean(messageExecutor);
            checkState(executor instanceof ExecutorService,
                       "No such bean: " + messageExecutor);
            getNucleus().setMessageExecutor((ExecutorService) executor);
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
                LOGGER.warn("File missing: {}; sleeping {} seconds", missing, WAIT_FOR_FILE_SLEEP / 1000);
                Thread.sleep(WAIT_FOR_FILE_SLEEP);
            }
        }
    }

    private void executeSetup() throws CommandException
    {
        executeDefinedSetup();

        if( _setupFile != null && _setupFile.isFile() ) {
            for (CellSetupProvider provider: _setupProviders.values()) {
                provider.beforeSetup();
            }

            try {
                execFile(_setupFile);
            } catch (IOException e) {
                throw new CommandException("Failed to load " + _setupFile.toPath() +
                        ": " + e.getMessage(), e);
            }

            for (CellSetupProvider provider: _setupProviders.values()) {
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

    public class SetupCommandListener implements CellCommandListener
    {
        @Command(name = "save", hint = "save service configuration")
        public class SaveCommand implements Callable<String>
        {
            @Option(name = "sc", usage = "Cell address of setup controller.")
            String controller;

            @Option(name = "file", usage = "Path of setup file.")
            File file;

            @Override
            public String call()
                    throws IOException, IllegalArgumentException
            {
                if ("none".equals(controller)) {
                    controller = null;
                    file = _setupFile;
                } else if (file == null && controller == null) {
                    controller = _setupController;
                    file = _setupFile;
                }

                checkArgument(file != null || controller != null,
                              "Either a setup controller or setup file must be specified.");

                if (controller != null) {
                    checkState(!Strings.isNullOrEmpty(_setupClass),
                               "Cannot save to a setup controller since the cell has no setup class.");

                    StringWriter sw = new StringWriter();
                    printSetup(new PrintWriter(sw));

                    SetupInfoMessage info =
                            new SetupInfoMessage("put", getCellName(),
                                                 _setupClass, sw.toString());

                    sendMessage(new CellMessage(new CellPath(controller), info));
                }

                if (file != null) {
                    File path = file.getAbsoluteFile();
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
            public String call() throws CommandException, IllegalArgumentException
            {
                checkArgument(confirmed, "Required option is missing.");
                if (_setupFile != null && !_setupFile.exists()) {
                    return String.format("Setup file [%s] does not exist", _setupFile);
                }
                executeSetup();
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
                            s.append(org.dcache.commons.util.Strings.toString(bean.getPropertyValue(property)));
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
            return (o != null) ? org.dcache.commons.util.Strings.toMultilineString(o) : ("No such property: " + name);
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
     * Registers a setup provider. Setup providers contribute to the
     * result of the <code>save</code> method.
     */
    public void addSetupProviderBean(CellSetupProvider bean, String name)
    {
        _setupProviders.put(name, bean);
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

        if (bean instanceof CellSetupProvider) {
            addSetupProviderBean((CellSetupProvider) bean, beanName);
        }

        if (bean instanceof CellLifeCycleAware) {
            addLifeCycleAwareBean((CellLifeCycleAware) bean, beanName);
        }

        if (bean instanceof EnvironmentAware) {
            ((EnvironmentAware) bean).setEnvironment(_environment);
        }

        if (bean instanceof DomainContextAware) {
            ((DomainContextAware) bean).setDomainContext(getDomainContext());
        }

        if (bean instanceof CellEventListener) {
            addCellEventListener((CellEventListener) bean);
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
            context.setConfigLocations(new String[]{args.argv(0)});
            context.addBeanFactoryPostProcessor(
                    beanFactory -> beanFactory.addBeanPostProcessor(UniversalSpringCell.this));

            ConfigurableEnvironment environment = context.getEnvironment();
            environment.getPropertySources().addFirst(
                    new MapPropertySource("environment", _environment));
            environment.getPropertySources().addFirst(
                    new MapPropertySource("options", Maps.<String,Object>newHashMap(args.optionsAsMap())));
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
}
