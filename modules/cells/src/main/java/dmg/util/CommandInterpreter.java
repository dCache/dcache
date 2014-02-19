package dmg.util;

import java.io.Serializable;
import dmg.util.command.AcCommandScanner;

import org.dcache.util.Args;

/**
 *   Scans a specified object and makes a special set
 *   of methods available for dynamic invocation on
 *   command strings.
 *
 *   <pre>
 *      method syntax :
 *      1)  public Object ac_&lt;key1&gt;_..._&lt;keyN&gt;(Args args)
 *      2)  public Object ac_&lt;key1&gt;_..._&lt;keyN&gt;_$_n(Args args)
 *      3)  public Object ac_&lt;key1&gt;_..._&lt;keyN&gt;_$_n_m(Args args)
 *   </pre>
 *   The first syntax requires a command string which exactly matches
 *   the specified <code>key1</code> to <code>keyN</code>.
 *   No extra arguments are excepted.
 *   The second syntax allows exactly <code>n</code> extra arguments
 *   following a matching sequence of keys. The third
 *   syntax allows between <code>n</code> and <code>m</code>
 *   extra arguments following a matching sequence of keys.
 *   Each ac_ method may have a corresponding one line help hint
 *   with the following signature.
 *   <pre>
 *       String hh_&lt;key1&gt;_..._&lt;keyN&gt; = "..." ;
 *   </pre>
 *   The assigned string should only present the
 *   additional arguments and shouldn't repeat the command part
 *   itself.
 *   A full help can be made available with the signature
 *   <pre>
 *       String fh_&lt;key1&gt;_..._&lt;keyN&gt; = "..." ;
 *   </pre>
 *   The assigned String should contain a detailed multiline
 *   description of the command. This text is returned
 *   as a result of the <code>help ... </code> command.
 *   Consequently <code>help</code> is a reserved keyword
 *   and can't be used as first key.
 *   <p>
 *
 */
public class CommandInterpreter extends org.dcache.util.cli.CommandInterpreter
{
    public CommandInterpreter()
    {
        addCommandScanner(new AcCommandScanner());
    }

    public CommandInterpreter(Object commandListener)
    {
        super(commandListener);
        addCommandScanner(new AcCommandScanner());
    }

    /**
     * Is a convenient method of <code>command(Args args)</code>.
     * All Exceptions are catched and converted to a meaningful
     * String except the CommandExitException which allows the
     * corresponding object to signal a kind
     * of final state. This method should be overwritten to
     * customize the behavior on different Exceptions.
     * This method <strong>never</strong> returns the null
     * pointer even if the underlying <code>command</code>
     * method does so.
     */
    public String command(String str) throws CommandExitException {
        if (str.trim().isEmpty()) {
            return "";
        }
        try {
            Object o = command(new Args(str));
            if (o == null) {
                return "";
            }
            return (String) o;
        } catch (CommandSyntaxException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Syntax Error : ").append(e.getMessage()).append("\n");
            String help  = e.getHelpText();
            if (help != null) {
                sb.append("Help : \n");
                sb.append(help);
            }
            return sb.toString();
        } catch (CommandExitException e) {
            throw e;
        } catch (CommandThrowableException e) {
            StringBuilder sb = new StringBuilder();
            sb.append(e.getMessage()).append("\n");
            Throwable t = e.getTargetException();
            sb.append(t.getClass().getName()).append(" : ").append(t.getMessage()).append("\n");
            return sb.toString();
        } catch (CommandPanicException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Panic : ").append(e.getMessage()).append("\n");
            Throwable t = e.getTargetException();
            sb.append(t.getClass().getName()).append(" : ").append(t.getMessage()).append("\n");
            return sb.toString();
        } catch (CommandException e) {
            return "??? : " + e.toString();
        }
    }
}
