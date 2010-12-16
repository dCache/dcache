package org.dcache.services.pinmanager1;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.ReflectionUtils;
import org.dcache.cells.MessageCallback;
import org.dcache.cells.CellStub;

import statemap.FSMContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SMCTask implements MessageCallback<Message>
{
    private final static Logger _log =
        LoggerFactory.getLogger(SMCTask.class);

    private FSMContext _fsm;
    protected CellEndpoint _cell;
    protected String _cellName;

    public SMCTask(CellEndpoint cell)
    {
        _cell = cell;
        _cellName = _cell.getCellInfo().getCellName();
    }

    protected void setContext(FSMContext o)
    {
        _fsm = o;
    }

    private synchronized void transition(String name, Object ...arguments)
    {
        _log.warn("transition("+ name+", "+java.util.Arrays.deepToString(arguments)+")");
        try {
            Class[] parameterTypes = new Class[arguments.length];
            for (int i = 0; i < arguments.length; i++)
                parameterTypes[i] = arguments[i].getClass();
            Method m =
                ReflectionUtils.resolve(_fsm.getClass(), name, parameterTypes);
            _log.warn("transition() invoking method "+m);
            if (m != null)
                m.invoke(_fsm, arguments);
        } catch (IllegalAccessException e) {
            // We are not allowed to call this method. Better escalate it.
            _log.warn(e.toString(), e);
            throw new RuntimeException("Bug detected", e);
        } catch (InvocationTargetException e) {
            // The context is not supposed to throw exceptions, so
            // smells like a bug.
            _log.warn(e.toString(), e);
            throw new RuntimeException("Bug detected", e);
        } catch (statemap.TransitionUndefinedException e) {
            _log.warn(e.toString(), e);
            throw new RuntimeException("State machine is incomplete", e);
        }
    }

    public void success(Message message)
    {
        transition("success", message);
    }

    public void failure(int rc, Object error)
    {
        _log.warn("Error: {} [{}]", error, rc);
        transition("failure", rc, error);
    }

    public void noroute()
    {
        transition("noroute");
    }

    public void timeout()
    {
        transition("timeout");
    }

    public void send(CellStub stub, Message message)
    {
        stub.send(message, Message.class, this);
    }

    public void send(CellStub stub, Message message, String destination)
    {
        stub.send(new CellPath(destination), message, Message.class, this);
    }

    public String getCellName()
    {
        return _cellName;
    }
}

