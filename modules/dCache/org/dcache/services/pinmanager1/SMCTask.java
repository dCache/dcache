package org.dcache.services.pinmanager1;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.ReflectionUtils;

import statemap.FSMContext;

class SMCTask implements CellMessageAnswerable
{
    private FSMContext _fsm;
    protected CellAdapter _cell;

    public SMCTask(CellAdapter cell)
    {
        _cell = cell;
    }

    protected void setContext(FSMContext o)
    {
        _fsm = o;
    }

    private void transition(String name, Object ...arguments)
    {
        _cell.esay("transition("+ name+", "+java.util.Arrays.deepToString(arguments)+")");
        try {
            Class[] parameterTypes = new Class[arguments.length];
            for (int i = 0; i < arguments.length; i++)
                parameterTypes[i] = arguments[i].getClass();
            Method m =
                ReflectionUtils.resolve(_fsm.getClass(), name, parameterTypes);
             _cell.esay("transition() invoking method "+m);
            if (m != null)
                m.invoke(_fsm, arguments);
        } catch (IllegalAccessException e) {
            // We are not allowed to call this method. Better escalate it.
            _cell.esay(e);
            throw new RuntimeException("Bug detected", e);
        } catch (InvocationTargetException e) {
            // The context is not supposed to throw exceptions, so
            // smells like a bug.
            _cell.esay(e);
            if(e.getTargetException() != null) {
                _cell.esay(e.getTargetException());
            }
            throw new RuntimeException("Bug detected", e);
        } catch (statemap.TransitionUndefinedException e) {
            _cell.esay(e);
            throw new RuntimeException("State machine is incomplete", e);
        }
    }

    public void answerArrived(CellMessage question, CellMessage answer)
    {
        transition("answerArrived", answer.getMessageObject());
    }

    public void answerTimedOut(CellMessage request)
    {
        _cell.esay("Message timed out: " + request);
        transition("timeout");
    }

    public void exceptionArrived(CellMessage request, Exception exception)
    {
        _cell.esay("Error: " + exception);
        transition("exceptionArrived", exception);
    }

    public void sendMessage(CellPath path, Message message, long timeout)
    {
        _cell.sendMessage(new CellMessage(path, message),
                          true, true, this, timeout);
    }

    public void sendMessage(CellPath path, Message message)
        throws NoRouteToCellException
    {
        _cell.sendMessage(new CellMessage(path, message));
    }

    public String getCellName() {
        return _cell.getNucleus().getCellName();
    }
}

