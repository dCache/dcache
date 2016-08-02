package org.dcache.pool.classic;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellArgsAware;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.command.Argument;
import dmg.util.command.Command;

import org.dcache.pool.movers.MoverProtocol;

public class MoverMapTransferService extends AbstractMoverProtocolTransferService
        implements CellCommandListener, EnvironmentAware
{
    private final ConcurrentMap<String, Class<? extends MoverProtocol>> _movermap = new ConcurrentHashMap<>();
    private Map<String,Object> _environment;

    @Override
    public void setEnvironment(Map<String,Object> environment)
    {
        _environment = ImmutableMap.copyOf(environment);
    }

    private Class<? extends MoverProtocol> getMoverProtocolClass(ProtocolInfo info) throws ClassNotFoundException
    {
        String protocolName = info.getProtocol() + "-" + info.getMajorVersion();
        Class<? extends MoverProtocol> moverClass = _movermap.get(protocolName);
        if (moverClass == null) {
            String moverClassName =
                    "org.dcache.pool.movers." + info.getProtocol() + "Protocol_" + info.getMajorVersion();
            moverClass = Class.forName(moverClassName).asSubclass(MoverProtocol.class);
            Class<? extends MoverProtocol> oldClass = _movermap.putIfAbsent(protocolName, moverClass);
            if (oldClass != null) {
                moverClass = oldClass;
            }
        }
        return moverClass;
    }

    @Override
    protected MoverProtocol createMoverProtocol(ProtocolInfo info) throws
            NoSuchMethodException, InstantiationException, IllegalAccessException,
            InvocationTargetException, ClassNotFoundException
    {
        Class<? extends MoverProtocol> moverClass = getMoverProtocolClass(info);
        Constructor<? extends MoverProtocol> moverCon = moverClass.getConstructor(CellEndpoint.class);
        MoverProtocol mover = moverCon.newInstance(getCellEndpoint());
        if (mover instanceof EnvironmentAware) {
            ((EnvironmentAware)mover).setEnvironment(_environment);
        }
        if (mover instanceof CellArgsAware) {
            ((CellArgsAware)mover).setCellArgs(getArgs());
        }
        return mover;
    }


    @Command(name = "movermap define",
            description = "Adds a transfer protocol mapping")
    class DefineCommand implements Callable<String>
    {
        @Argument(index = 0, valueSpec = "PROTOCOL-MAJOR",
                usage = "Protocol identification string")
        String protocol;

        @Argument(index = 1, metaVar = "moverclassname",
                usage = "A class implementing the MoverProtocol interface.")
        String moverClassName;

        @Override
        public String call() throws ClassNotFoundException
        {
            _movermap.put(protocol, Class.forName(moverClassName).asSubclass(MoverProtocol.class));
            return "";
        }
    }

    @Command(name = "movermap undefine",
            description = "Removes a transfer protocol mapping")
    class UndefineCommand implements Callable<String>
    {
        @Argument(valueSpec = "PROTOCOL-MAJOR",
                usage = "Protocol identification string")
        String protocol;

        @Override
        public String call()
        {
            _movermap.remove(protocol);
            return "";
        }
    }

    @Command(name = "movermap ls",
            description = "Lists all defined protocol mappings.")
    class ListCommand implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Class<? extends MoverProtocol>> entry : _movermap.entrySet()) {
                sb.append(entry.getKey()).append(" -> ").append(entry.getValue().getName()).append("\n");
            }
            return sb.toString();
        }
    }
}
