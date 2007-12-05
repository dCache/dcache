package org.dcache.services.pinmanager;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.io.NotSerializableException;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;

import statemap.FSMContext;

abstract class SMCDriver implements CellMessageAnswerable
{
    public SMCDriver()
    {
    }

    abstract protected FSMContext getContext();

    protected void transition(String name, Object ...arguments)
    {
        try {
            FSMContext context = getContext();
            Class[] parameterTypes = new Class[arguments.length];
            for (int i = 0; i < arguments.length; i++)
                parameterTypes[i] = arguments[i].getClass();
            Method m =
                ReflectionUtils.resolve(context.getClass(), name, parameterTypes);
            m.invoke(context, arguments);
        } catch (NoSuchMethodException e) {
            // FSM is not interested in the message. No problem.
        } catch (IllegalAccessException e) {
            // We are not allowed to call this method. Better escalate it.
            throw new RuntimeException("Bug detected", e);
        } catch (InvocationTargetException e) {
            // The context is not supposed to throw exceptions, so
            // smells like a bug.
            throw new RuntimeException("Bug detected", e);
        } catch (statemap.TransitionUndefinedException e) {
            throw new RuntimeException("State machine is incomplete", e);
        }
    }

    public void answerArrived(CellMessage question, CellMessage answer)
    {
        transition("answerArrived", answer.getMessageObject());
    }

    public void answerTimedOut(CellMessage request)
    {
        transition("timeout");
    }

    public void exceptionArrived(CellMessage request, Exception exception)
    {
        transition("exceptionArrived", exception);
    }
}

