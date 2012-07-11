package org.dcache.gplazma.loader.cli;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/**
 *  The main class of a simple CLI for querying about gPlazma plugins
 */
public class Main {

    public static void main( String[] args) {
        if( args.length == 0) {
            throw new IllegalArgumentException("Need command name");
        }

        Class<?> className;
        try {
            className = Class.forName( args[0]);
        } catch (ClassNotFoundException e) {
            System.err.println("Unknown command " + args[0] );
            System.exit( 1);
            return;
        }

        if( !Command.class.isAssignableFrom( className)) {
            System.err.println("Command " + args[0] + " is not a command");
            System.exit(1);
        }

        @SuppressWarnings("unchecked") // we checked
        Class<? extends Command> commandName = (Class<? extends Command>) className;

        Constructor<? extends Command> constructor;
        try {
            constructor = commandName.getConstructor();
        } catch (SecurityException e) {
            System.err.println( "Cannot run command");
            System.exit( 1);
            return;
        } catch (NoSuchMethodException e) {
            System.err.println( "Cannot create command");
            System.exit( 1);
            return;
        }

        Command command;
        try {
            command = constructor.newInstance();
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        } catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }

        int rc = command.run( Arrays.copyOfRange( args, 1, args.length));

        System.exit( rc);
    }

}
