//______________________________________________________________________________
//
// This code is stolen from org.dcache.services.AbstractCell
//
//  $Id$ 
//  $Author$
//______________________________________________________________________________
package gov.fnal.srm.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.io.PrintWriter;

public class OptionParser { 
        
        private Args _args;
        public Args getArgs() { return _args;} 

        public OptionParser(Args args) { 
                this._args = args;
        }

        public  String getOption(Option option) throws IllegalArgumentException { 
                String s;
                s = getArgs().getOpt(option.name());
//                if (s != null && (s.length() > 0 || !option.required()))
//                        return s;
                if (s != null ) { 
                        if (s.length()==0 && !option.required()) 
                                return "true";    // to support switch type options
                        else 
                                return s;
                }
                if (option.required())
                        throw new IllegalArgumentException(option.name()
                                                           + " is a required argument");
                
                return option.defaultValue();
        }


        public <T>  void parseOptions(T t) {
                for (Class c = t.getClass(); c != null; c = c.getSuperclass()) {
                        for (Field field : c.getDeclaredFields()) {
                                Option option = field.getAnnotation(Option.class);
                                try {
                                        if (option != null) {
                                                field.setAccessible(true);
                                                String s = getOption(option);
                                                Object value;
                                                if (s != null && s.length() > 0) {
                                                        try {
                                                                value = toType(s, field.getType());
                                                                field.set(t, value);
                                                        } 
                                                        catch (ClassCastException e) {
                                                                throw new IllegalArgumentException("Cannot convert '" + s + "' to " + field.getType(), e);
                                                        }
                                                } 
                                                else {
                                                        value = field.get(t);
                                                }
                                                
                                                if (option.log()) {
                                                        String description = option.description();
                                                        String unit = option.unit();
                                                        if (description.length() == 0)
                                                                description = option.name();
                                                        if (unit.length() > 0) {
                                                                System.out.println("-"+option.name()+" "+ description + " set to " + value + " " + unit);
                                                        } 
                                                        else {
                                                                System.out.println("-"+option.name()+" " +description + " set to " + value);
                                                        }
                                                }
                                        }
                                } 
                                catch (SecurityException e) {
                                        throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                                } 
                                catch (IllegalAccessException e) {
                                        throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                                }
                                catch (Exception e) { 
                                        e.printStackTrace();
                                }

                        }
                }
        }
        
        /**
         * Writes information about all options (Option annotated fields)
         * to a writer.
         */

        protected <T> void writeOptions(T t, PrintWriter out) {
                for (Class c = t.getClass(); c != null; c = c.getSuperclass()) {
                        for (Field field : c.getDeclaredFields()) {
                                Option option = field.getAnnotation(Option.class);
                                try {
                                        if (option != null) {
                                                if (option.log()) {
                                                        field.setAccessible(true);
                                                        Object value = field.get(t);
                                                        String description = option.description();
                                                        String unit = option.unit();
                                                        if (description.length() == 0)
                                                                description = option.name();
                                                        out.println(description + " is " + value + " " + unit);
                                                }
                                        }
                                } 
                                catch (SecurityException e) {
                                        throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                                } 
                                catch (IllegalAccessException e) {
                                        throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                                }
                        }
                }
        }
        
        
        
        @SuppressWarnings("unchecked")
                static public <T> T toType(final Object object, final Class<T> type) {
                T result = null;
                if (object == null) {
                        //initalize primitive types:
                        if (type == Boolean.TYPE) {
                                result = ((Class<T>) Boolean.class).cast(false);
                        } 
                        else if (type == Byte.TYPE) {
                                result = ((Class<T>) Byte.class).cast(0);
                        } 
                        else if (type == Character.TYPE) {
                                result = ((Class<T>) Character.class).cast(0);
                        } 
                        else if (type == Double.TYPE) {
                                result = ((Class<T>) Double.class).cast(0.0);
                        } 
                        else if (type == Float.TYPE) {
                                result = ((Class<T>) Float.class).cast(0.0);
                        } 
                        else if (type == Integer.TYPE) {
                                result = ((Class<T>) Integer.class).cast(0);
                        } 
                        else if (type == Long.TYPE) {
                                result = ((Class<T>) Long.class).cast(0);
                        } else if (type == Short.TYPE) {
                                result = ((Class<T>) Short.class).cast(0);
                        }
                } 
                else {
                        final String so = object.toString();
                        //custom type conversions:
                        if (type == BigInteger.class) {
                                result = type.cast(new BigInteger(so));
                        } 
                        else if (type == Boolean.class || type == Boolean.TYPE) {
                                Boolean r = null;
                                if ("1".equals(so) || "true".equalsIgnoreCase(so) || "yes".equalsIgnoreCase(so) || "on".equalsIgnoreCase(so) || "enabled".equalsIgnoreCase(so)) {
                                        r = Boolean.TRUE;
                                } 
                                else if ("0".equals(object) || "false".equalsIgnoreCase(so) || "no".equalsIgnoreCase(so) || "off".equalsIgnoreCase(so) || "disabled".equalsIgnoreCase(so)) {
                                        r = Boolean.FALSE;
                                } 
                                else {
                                        r = Boolean.valueOf(so);
                                }
                                if (type == Boolean.TYPE) {
                                        result = ((Class<T>) Boolean.class).cast(r); //avoid ClassCastException through autoboxing
                                } 
                                else {
                                        result = type.cast(r);
                                }
                        } 
                        else if (type == Byte.class || type == Byte.TYPE) {
                                Byte i = Byte.valueOf(so);
                                if (type == Byte.TYPE) {
                                        result = ((Class<T>) Byte.class).cast(i); //avoid ClassCastException through autoboxing
                                } 
                                else {
                                        result = type.cast(i);
                                }
                        } 
                        else if (type == Character.class || type == Character.TYPE) {
                                Character i = new Character(so.charAt(0));
                                if (type == Character.TYPE) {
                                        result = ((Class<T>) Character.class).cast(i); //avoid ClassCastException through autoboxing
                                } 
                                else {
                                        result = type.cast(i);
                                }
                        } 
                        else if (type == Double.class || type == Double.TYPE) {
                                Double i = Double.valueOf(so);
                                if (type == Double.TYPE) {
                                        result = ((Class<T>) Double.class).cast(i); //avoid ClassCastException through autoboxing
                                } 
                                else {
                                        result = type.cast(i);
                                }
                        } 
                        else if (type == Float.class || type == Float.TYPE) {
                                Float i = Float.valueOf(so);
                                if (type == Float.TYPE) {
                                        result = ((Class<T>) Float.class).cast(i); //avoid ClassCastException through autoboxing
                                } 
                                else {
                                        result = type.cast(i);
                                }
                        } 
                        else if (type == Integer.class || type == Integer.TYPE) {
                                Integer i = null;
                                if (!"null".equals(so)) { 
                                        i = Integer.valueOf(so);
                                }
                                if (type == Integer.TYPE) {
                                        result = ((Class<T>) Integer.class).cast(i); //avoid ClassCastException through autoboxing
                                } 
                                else {
                                        result = type.cast(i);
                                }
                        } 
                        else if (type == Long.class || type == Long.TYPE) {
                                Long i = Long.valueOf(so);
                                if (type == Long.TYPE) {
                                        result = ((Class<T>) Long.class).cast(i); //avoid ClassCastException through autoboxing
                                } 
                                else {
                                        result = type.cast(i);
                                }
                        } 
                        else if (type == Short.class || type == Short.TYPE) {
                                Short i = Short.valueOf(so);
                                if (type == Short.TYPE) {
                                        result = ((Class<T>) Short.class).cast(i); //avoid ClassCastException through autoboxing
                                } 
                                else {
                                        result = type.cast(i);
                                }
                        } 
                        else {
                                try {
                                        Constructor<T> constructor =
                                                type.getConstructor(String.class);
                                        result = constructor.newInstance(object);
                                } 
                                catch (NoSuchMethodException e) {
                                        //hard cast:
                                        result = type.cast(object);
                                } 
                                catch (SecurityException e) {
                                        //hard cast:
                                        result = type.cast(object);
                                } 
                                catch (InstantiationException e) {
                                        //hard cast:
                                        result = type.cast(object);
                                } 
                                catch (IllegalAccessException e) {
                                        //hard cast:
                                        result = type.cast(object);
                                } catch (InvocationTargetException e) {
                                        //hard cast:
                                        result = type.cast(object);
                                }
                        }
                }
                return result;
        }
        
}