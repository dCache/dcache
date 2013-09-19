/*
 * ShellCommandExecuter.java
 *
 * Created on January 28, 2003, 1:40 PM
 */

package org.dcache.srm.util;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.StringTokenizer;

/**
 *
 * @author  timur
 */

public class ShellCommandExecuter implements Runnable
{
    public static int execute(String command)
    {
        return execute(command,new PrintWriter(System.out),new PrintWriter(System.err));
    }
    
    public static int execute(String command,Writer out, Writer error)
    {

         //System.out.println("execute("+command+")");
         Process proc;
         InputStream StdErr;
         InputStream StdOut;

        try
        {
            proc = Runtime.getRuntime().exec(command);
            StdErr = proc.getErrorStream();
            StdOut  = proc.getInputStream();
        }
        catch(IOException ioe)
        {
            ioe.printStackTrace();
            return 1;
        }

        BufferedReader OutReader =
            new BufferedReader(new InputStreamReader(StdOut));
        new ShellCommandExecuter(OutReader,out);
        BufferedReader ErrReader =
            new BufferedReader(new InputStreamReader(StdErr));
        new ShellCommandExecuter(ErrReader,error);
        int exit_value=1;
        try
        {
            exit_value =  proc.waitFor();
        }
        catch(InterruptedException ie)
        {
        }
        //System.out.println(" exit value is "+ exit_value);
        return exit_value;
    }
    public static int execute(String [] command,Writer out, Writer error)
    {

         //System.out.println("execute("+command+")");
         Process proc;
         InputStream StdErr;
         InputStream StdOut;

        try
        {
            proc = Runtime.getRuntime().exec(command);
            StdErr = proc.getErrorStream();
            StdOut  = proc.getInputStream();
        }
        catch(IOException ioe)
        {
            ioe.printStackTrace();
            return 1;
        }

        BufferedReader OutReader =
            new BufferedReader(new InputStreamReader(StdOut));
        new ShellCommandExecuter(OutReader,out);
        BufferedReader ErrReader =
            new BufferedReader(new InputStreamReader(StdErr));
        new ShellCommandExecuter(ErrReader,error);
        int exit_value=1;
        try
        {
            exit_value =  proc.waitFor();
        }
        catch(InterruptedException ie)
        {
        }
        //System.out.println(" exit value is "+ exit_value);
        return exit_value;
    }

    public static String[] executeAndReturnOutput(String command)
    {

         //System.out.println("executeAndReturnOutput("+command+")");
         Process proc;
         InputStream StdErr;
         InputStream StdOut;

        try
        {
            proc = Runtime.getRuntime().exec(command);
            StdErr = proc.getErrorStream();
            StdOut  = proc.getInputStream();
        }
        catch(IOException ioe)
        {
            ioe.printStackTrace();
            return null;
        }
               
        StringWriter string_writer = new StringWriter();
        BufferedReader OutReader =
            new BufferedReader(new InputStreamReader(StdOut));
        new ShellCommandExecuter(OutReader,string_writer);
        BufferedReader ErrReader =
            new BufferedReader(new InputStreamReader(StdErr));
        new ShellCommandExecuter(ErrReader,new PrintWriter(System.err));
        try
        {
            proc.waitFor();
        }
        catch(InterruptedException ie)
        {
        }
        //System.out.println(" exit value is "+ exit_value);
        StringTokenizer tokenizer = new StringTokenizer(string_writer.getBuffer().toString());
        int len = tokenizer.countTokens();
        String result[] = new String[len];
        for(int i =0; i<len;++i)
        {
            result[i] = tokenizer.nextToken();
        }
        return result;
    }

    BufferedReader reader;
    BufferedReader ErrReader;
    boolean error;
    private Writer out;
    
    private  ShellCommandExecuter(BufferedReader reader,Writer out)
    {
        
        this.reader = reader;
        this.out = out;
        new Thread(this).start();
    }


  @Override
  public void run()
  {
      try
      {
            String line;
            while((line = reader.readLine()) != null)
            {
                out.write(line);
                out.write('\n');
                out.flush();
            }
      }
      catch(IOException e)
      {
            e.printStackTrace();
      } 
  }
}
