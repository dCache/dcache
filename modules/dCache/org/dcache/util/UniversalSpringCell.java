package org.dcache.util;

import java.util.Dictionary;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Set;
import java.util.TreeSet;
import java.util.Formatter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.beans.PropertyDescriptor;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.CellAdapter;
import dmg.util.Args;

import org.dcache.services.AbstractCell;
import org.dcache.cell.CellEndpoint;
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
    implements CellEndpoint,
               BeanPostProcessor
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

    public UniversalSpringCell(String cellName, String arguments)
        throws InterruptedException
    {
        super(cellName, arguments, true);

        Args args = getArgs();
        if (args.argc() == 0)
            throw new IllegalArgumentException("Configuration location missing");
        /* Execute initialisation in a different thread allocated from
         * the correct thread group.
         */
        Thread thread = getNucleus().newThread(new Runnable() {
                public void run()
                {
                    UniversalSpringCell.this.init();
                }
            }, "init");

        thread.start();

        if (args.getOpt("wait") != null)
            thread.join();
    }

    private void init()
    {
        try {
            _context =
                new UniversalSpringCellApplicationContext(getArgs());
        } catch (Throwable t) {
            fatal("Failed to initalise cell: " + t.getMessage());
            kill();
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

    public final String hh_save = "[filename] # saves setup to disk";
    public String ac_save_$_0_1(Args args)
    {
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
        try {
            if (_context.isSingleton(name)) {
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
            s.append(',').append(a);
        }
        return s.toString();
    }

    public static final String hh_bean_ls = "# lists running beans";
    public String ac_bean_ls(Args args)
    {
        final String format = "%-30s %s\n";
        ConfigurableListableBeanFactory factory = _context.getBeanFactory();
        Formatter s = new Formatter(new StringBuilder());

        s.format(format, "Bean", "Used by");
        for (String name : factory.getSingletonNames()) {
            Object bean = factory.getBean(name);
            String[] usedby = factory.getDependentBeans(name);
            s.format(format, name, arrayToString(usedby));
        }

        return s.toString();
    }

    public static final String hh_bean_properties =
        "<bean> # shows properties of a bean";
    public String ac_bean_properties_$_1(Args args)
    {
        String name = args.argv(0);
        try {
            if (_context.isSingleton(name)) {
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

    public void ac_bean_restart(Args args)
    {

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