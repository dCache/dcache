package org.dcache.util;

import java.util.Date;
import java.util.Dictionary;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Set;
import java.util.TreeSet;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.lang.reflect.InvocationTargetException;
import java.beans.PropertyDescriptor;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.SetupInfoMessage;
import dmg.util.Args;
import dmg.util.CommandException;

import org.dcache.services.AbstractCell;
import org.dcache.cell.CellMessageReceiver;
import org.dcache.cell.CellMessageSender;
import org.dcache.cell.ThreadFactoryAware;
import org.dcache.cell.CellInfoProvider;
import org.dcache.cell.CellSetupProvider;
import org.dcache.cell.CellCommandListener;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

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
    implements BeanPostProcessor
{
    /**
     * Spring application context. All beans are created through this
     * context.
     */
    private ConfigurableApplicationContext _context;

    /**
     * List of registered info providers. Sorted to maintain
     * consistent ordering.
     */
    private final Set<CellInfoProvider> _infoProviders =
        new TreeSet<CellInfoProvider>(new ClassNameComparator());

    /**
     * List of registered setup providers. Sorted to maintain
     * consistent ordering.
     */
    private final Set<CellSetupProvider> _setupProviders =
        new TreeSet<CellSetupProvider>(new ClassNameComparator());

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
    private String _setupFile;

    public UniversalSpringCell(String cellName, String arguments)
        throws InterruptedException, ExecutionException
    {
        super(cellName, arguments);
        doInit();
    }

    @Override
    protected void init()
        throws InterruptedException, IOException, CommandException
    {
        /* Process command line arguments.
         */
        Args args = getArgs();
        if (args.argc() == 0)
            throw new IllegalArgumentException("Configuration location missing");

        _setupController = args.getOpt("setupController");
        info("Setup controller set to "
             + (_setupController == null ? "none" : _setupController));
        _setupFile = args.getOpt("setupFile");
        _setupClass = args.getOpt("setupClass");

        if (_setupController != null && _setupClass == null)
            throw new IllegalArgumentException("Setup class must be specified when a setup controller is used");

        /* Instantiate Spring application context. This will
         * eagerly instantiate all beans.
         */
        _context =
            new UniversalSpringCellApplicationContext(getArgs());

        /* Execute both the defined setup and the setup file.
         */
        if (_definedSetup != null) {
            executeDomainContext(_definedSetup);
        }

        if (_setupFile != null) {
            File file = new File(_setupFile);
            while (!file.exists()) {
                error("Setup does not exists; waiting");
                Thread.sleep(30000);
            }

            execFile(file);
        }

        /* Now that everything is instantiated and configured, we can
         * start the cell and run the final initialisation hooks.
         */
        start();
        for (CellSetupProvider provider: _setupProviders) {
            provider.afterSetupExecuted();
        }
    }

    /**
     * Closes the application context, which will shutdown all beans.
     */
    public void cleanUp()
    {
        if (_context != null) {
            _context.close();
            _context = null;
        }
        _infoProviders.clear();
        _setupProviders.clear();
    }

    /**
     * Collects information from all registered info providers.
     */
    public void getInfo(PrintWriter pw)
    {
        for (CellInfoProvider provider : _infoProviders)
            provider.getInfo(pw);
    }

    /**
     * Collects information about the cell and returns these in a
     * CellInfo object. Information is collected from the CellAdapter
     * base class and from all beans implementing CellInfoProvider.
     */
    public CellInfo getCellInfo()
    {
        CellInfo info = super.getCellInfo();
        for (CellInfoProvider provider : _infoProviders)
            info = provider.getCellInfo(info);
        return info;
    }

    /**
     * Collects setup information from all registered setup providers.
     */
    protected void printSetup(PrintWriter pw)
    {
        pw.println("#\n# Created by " + getCellName() + "("
                   + getClass().getName() + ") at " + (new Date()).toString()
                   + "\n#");
        for (CellSetupProvider provider: _setupProviders)
            provider.printSetup(pw);
    }

    public final String hh_save = "[-sc=<setupController>|none] [-file=<filename>] # saves setup to disk or setup controller";
    public String ac_save(Args args)
        throws IOException, IllegalArgumentException, NoRouteToCellException
    {
        String controller = args.getOpt("sc");
        String file = args.getOpt("file");

        if ("none".equals(controller)) {
            controller = null;
            file = _setupFile;
        } else if (file == null && controller == null) {
            controller = _setupController;
            file = _setupFile;
        }

        if (file == null && controller == null) {
            throw new IllegalArgumentException("Either a setup controller or setup file must be specified");
        }

        if (controller != null) {
            if (_setupClass == null || _setupClass.equals(""))
                throw new IllegalStateException("Cannot save to a setup controller since the cell has no setup class");

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

            PrintWriter pw = new PrintWriter(new FileWriter(temp));
            try {
                printSetup(pw);
            } finally {
                pw.close();
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
                if (line.length() == 0)
                    continue;
                if (line.charAt(0) == '#')
                    continue;
                try {
                    command(new Args(line));
                } catch (CommandException e) {
                    throw new CommandException("Error at line " + lineCount
                                               + ": " + e.getMessage());
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

    /**
     * Should be renamed to 'info' when command interpreter learns
     * to handle overloaded commands.
     */
    public static final String hh_infox = "<bean>";
    public String ac_infox_$_1(Args args)
    {
        String name = args.argv(0);
        try {
            if (_context == null && _context.isSingleton(name)) {
                Object bean = _context.getBean(name);
                if (CellInfoProvider.class.isInstance(bean)) {
                    StringWriter s = new StringWriter();
                    PrintWriter pw = new PrintWriter(s);
                    ((CellInfoProvider)bean).getInfo(pw);
                    return s.toString();
                }
                return "No information available";
            }
        } catch (NoSuchBeanDefinitionException e) {
        }
        return "No such bean: " + name;
    }

    private String arrayToString(Object[] a)
    {
        if (a.length == 0)
            return "";

        StringBuilder s = new StringBuilder(a[0].toString());
        for (int i = 1; i < a.length; i++) {
            s.append(',').append(a[i]);
        }
        return s.toString();
    }

    public static final String hh_bean_ls = "# lists running beans";
    public String ac_bean_ls(Args args)
    {
        Formatter s = new Formatter(new StringBuilder());
        if (_context != null) {
            final String format = "%-30s %s\n";
            ConfigurableListableBeanFactory factory = _context.getBeanFactory();

            s.format(format, "Bean", "Used by");
            for (String name : factory.getSingletonNames()) {
                Object bean = factory.getBean(name);
                String[] usedby = factory.getDependentBeans(name);
                s.format(format, name, arrayToString(usedby));
            }
        }
        return s.toString();
    }

    public static final String hh_bean_properties =
        "<bean> # shows properties of a bean";
    public String ac_bean_properties_$_1(Args args)
    {
        String name = args.argv(0);
        try {
            if (_context != null && _context.isSingleton(name)) {
                StringBuilder s = new StringBuilder();
                BeanWrapper bean = new BeanWrapperImpl(_context.getBean(name));
                for (PropertyDescriptor p : bean.getPropertyDescriptors()) {
                    if (!p.isHidden()) {
                        String property = p.getName();
                        if (bean.isReadableProperty(property)) {
                            Object value = bean.getPropertyValue(property);
                            s.append(property).append('=').append(value);
                            if (!bean.isWritableProperty(property)) {
                                s.append(" [read-only]");
                            }
                            s.append('\n');
                        }
                    }
                }
                return s.toString();
            }
        } catch (NoSuchBeanDefinitionException e) {
        }
        return "No such bean: " + name;
    }

    public String ac_bean_restart(Args args)
    {
        return "";
    }

    /**
     * Registers an info provider. Info providers contribute to the
     * result of the <code>getInfo</code> method.
     */
    public void addInfoProviderBean(CellInfoProvider bean)
    {
        _infoProviders.add(bean);
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
    public Object postProcessBeforeInitialization(Object bean,
                                                  String beanName)
        throws BeansException
    {
        if (CellCommandListener.class.isInstance(bean)) {
            addCommandListener(bean);
        }

        if (CellInfoProvider.class.isInstance(bean)) {
            addInfoProviderBean((CellInfoProvider)bean);
        }

        if (CellMessageReceiver.class.isInstance(bean)) {
            addMessageReceiver((CellMessageReceiver)bean);
        }

        if (CellMessageSender.class.isInstance(bean)) {
            addMessageSender((CellMessageSender)bean);
        }

        if (CellSetupProvider.class.isInstance(bean)) {
            addSetupProviderBean((CellSetupProvider)bean);
        }

        if (ThreadFactoryAware.class.isInstance(bean)) {
            addThreadFactoryAwareBean((ThreadFactoryAware)bean);
        }
        return bean;
    }

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

        private void mergeDictionary(Properties properties,
                                     Dictionary dictionary)
        {
            Enumeration e = dictionary.keys();
            while (e.hasMoreElements()) {
                Object key = e.nextElement();
                Object value = dictionary.get(key);
                properties.setProperty(key.toString(), value.toString());
            }
        }

        private ByteArrayResource getDomainContextResource()
        {
            Args args = (Args)getArgs().clone();
            args.shift();

            Properties properties = new Properties();
            String arguments =
                args.toString().replaceAll("-?\\$\\{.*\\}", "");
            properties.setProperty("arguments", arguments);
            mergeDictionary(properties, getDomainContext());
            mergeDictionary(properties, args.options());

            /* Convert to byte array form such that we can make it
             * available as a Spring resource.
             */
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                properties.store(out, "");
            } catch (IOException e) {
                /* This should never happen with a ByteArrayOutputStream.
                 */
                throw new RuntimeException("Unexpected exception", e);
            }
            final byte[] _domainContext = out.toByteArray();

            return new ByteArrayResource(_domainContext) {
                /**
                 * Fake file name to make
                 * PropertyPlaceholderConfigurer happy.
                 */
                public String getFilename()
                {
                    return "domaincontext.properties";
                }
            };
        }

        public Resource getResource(String location)
        {
            if (location.startsWith("domaincontext:")) {
                return getDomainContextResource();
            } else {
                return super.getResource(location);
            }
        }

        protected void customizeBeanFactory(DefaultListableBeanFactory beanFactory)
        {
            beanFactory.addBeanPostProcessor(UniversalSpringCell.this);
        }
    }
}