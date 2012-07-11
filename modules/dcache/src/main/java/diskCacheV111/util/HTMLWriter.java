package diskCacheV111.util;

import java.util.Date;
import java.util.Map;
import java.io.Writer;
import java.io.PrintWriter;
import java.io.OutputStream;

/**
 * Specialised HTML page writer for the dCache monitoring pages.
 *
 * Most of the dCache monitoring pages share a common look and
 * feel, which is encapsulated by this class.
 */
public class HTMLWriter extends PrintWriter
{
    protected       int           _row;
    protected final Map<String,Object> _context;
    protected final Writer        _writer;

    /**
     * Construct a new instance. The instance is bound to a cell
     * nucleus context, represented by a dictionary. Various settings
     * are taken from the dictionary.
     *
     * @param out     An output stream to write to
     * @param context Cell nucleus context
     */
    public HTMLWriter(OutputStream out, Map<String,Object> context)
    {
        super(out);
        _context = context;
        _writer = null;
    }

    /**
     * Construct a new instance. The instance is bound to a cell
     * nucleus context, represented by a dictionary. Various settings
     * are taken from the dictionary.
     *
     * @param writer  A writer to write to
     * @param context Cell nucleus context
     */
    public HTMLWriter(Writer writer, Map<String,Object> context)
    {
        super(writer);
        _context = context;
        _writer = writer;
    }

    /**
     * Adds the HTML header (containing stylesheet links, title, etc.)
     * and the page header (containing the dCache birds and the
     * heading).
     *
     * Must be called before anything else is added to the page.
     *
     * @param css   HREF to a CSS stylesheet
     * @param title String to use as title and heading
     */
    public void addHeader(String css, String title)
    {
        println("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">");
        println("<html>");
        println("<head>");
        println("<meta http-equiv='Content-Type' content='text/html; charset=iso-8859-1'>");
        println("<link type='text/css' rel='stylesheet' href='/styles/common.css'>");
        println("<link type='text/css' rel='stylesheet' href='" + css + "'>");
        println("<title>" + title + "</title>");
        println("<script type='text/javascript' src='/scripts/sorting/common.js'></script>");
        println("<script type='text/javascript' src='/scripts/sorting/css.js'></script>");
        println("<script type='text/javascript' src='/scripts/sorting/standardista-table-sorting.js'></script>");
        println("</head>");
        println("<body>");
        println("<div id='header'>");
        println("<div id='bird_small'>");
        println("<a href='/'><img src='/images/eagleredtrans.gif'></a>");
        println("<br/>Birds Home</div>");
        println("<div id='bird_large'><img src='/images/eagle-grey.gif'></div>");
        println("</div>");
        println("<div id='main'>");
        println("<h1>" + title + "</h1>");
    }

    /**
     * Adds the page footer (containing the creator string and
     * creation time) and closes the HTML page.
     *
     * Must be called after everything else was added to the page.
     *
     * @param creator Typically the class name and revision of the caller.
     */
    public void addFooter(String creator)
    {
        println("</div>");
        println("<div id=\"footer\">");
        println("<hr/>");
        printf("<address>%s at %s</address>\n", creator, new Date());
        println("</div>");
        println("</body></html>");
        close();
    }

    /**
     * Opens a new table.
     *
     * If provided, the arguments are interpreted as a list of pairs
     * containing first the CSS class name and then the heading for
     * the column. These will be added in a THEAD section.
     */
    public void beginTable(String c, String... columns)
    {
        _row = 0;
        if (c != null) {
            printf("<table class='%s'>\n", c);
        } else {
            println("<table>");
        }
        if (columns.length > 0) {
            beginTHead();
            for (int i = 0; i < columns.length; i += 2) {
                th(columns[i], columns[i + 1]);
            }
            endTHead();
        }
    }

    /**
     * Closes a table previously opened with beginTable().
     */
    public void endTable()
    {
        if (_row > 0) {
            println("</tbody>");
        }
        println("</table>");
    }

    /**
     * Opens a new table row.
     *
     * If one or more CSS classes are provided as arguments, the row
     * number modulo the number of classes is used to determine which
     * class to use for the row. For example, beginRow("even", "odd")
     * will use the class "even" for all even numbered rows, and "odd"
     * for all odd numbered rows.
     *
     * @param c CSS classes
     */
    public void beginRow(String... c)
    {
        if (_row == 0) {
            println("<tbody>");
        }
        if (c.length == 0) {
            println("<tr>");
        } else {
            String s = c[_row % c.length];
            if (s != null) {
                printf("<tr class='%s'>\n", s);
            } else {
                println("<tr>");
            }
        }
    }

    /**
     * Closes a row opened with beginRow() and increments the row
     * counter.
     */
    public void endRow()
    {
        endRow(true);
    }

    /**
     * Closes a row opened with beginRow().
     *
     * @param increment Increment row counter when true.
     */
    public void endRow(boolean increment)
    {
        println("</tr>");
        if (increment) {
            _row++;
        }
    }

    /**
     * Opens a THEAD section and a row.
     */
    public void beginTHead()
    {
        println("<thead>");
        println("<tr>");
    }

    /**
     * Closes a row and THEAD section opened with beginTHead();
     */
    public void endTHead()
    {
        println("</tr>");
        println("</thead>");
    }

    /**
     * Adds a TD cell. The content will be formed by concatenating the
     * string form of the given objects.
     *
     * @param c The CSS class of the cell, if non-null.
     * @param content The content to put inside the cell.
     */
    public void td(String c, Object... content)
    {
        td(1, 1, c, content);
    }

    /**
     * Adds a TD cell. The content will be formed by concatenating the
     * string form of the given objects.
     *
     * @param colspan The colspan of the cell.
     * @param c       The CSS class of the cell, if non-null.
     * @param content The content to put inside the cell.
     */
    public void td(int colspan, String c, Object... content)
    {
        td(1, colspan, c, content);
    }

    /**
     * Adds a TD cell. The content will be formed by concatenating the
     * string form of the given objects.
     *
     * @param rowspan The rowspan of the cell.
     * @param colspan The colspan of the cell.
     * @param c       The CSS class of the cell, if non-null.
     * @param content The content to put inside the cell.
     */
    public void td(int rowspan, int colspan, String c, Object... content)
    {
        print("<td");
        if (c != null) {
            printf(" class=\"%s\"", c);
        }
        if (rowspan > 1) {
            printf(" rowspan=\"%d\"", rowspan);
        }
        if (colspan > 1) {
            printf(" colspan=\"%d\"", colspan);
        }
        print(">");
        for (Object o : content) {
            print(o);
        }
        println("</td>");
    }

    /**
     * Adds a TH cell. The content will be formed by concatenating the
     * string form of the given objects.
     *
     * @param c The CSS class of the cell, if non-null.
     * @param content The content to put inside the cell.
     */
    public void th(String c, Object... content)
    {
        th(1, 1, c, content);
    }

    /**
     * Adds a TH cell. The content will be formed by concatenating the
     * string form of the given objects.
     *
     * @param colspan The colspan of the cell.
     * @param c       The CSS class of the cell, if non-null.
     * @param content The content to put inside the cell.
     */
    public void th(int colspan, String c, Object... content)
    {
        th(1, colspan, c, content);
    }

    /**
     * Adds a TH cell. The content will be formed by concatenating the
     * string form of the given objects.
     *
     * @param rowspan The rowspan of the cell.
     * @param colspan The colspan of the cell.
     * @param c       The CSS class of the cell, if non-null.
     * @param content The content to put inside the cell.
     */
    public void th(int rowspan, int colspan, String c, Object... content)
    {
        print("<th");
        if (c != null) {
            printf(" class=\"%s\"", c);
        }
        if (rowspan > 1) {
            printf(" rowspan=\"%d\"", rowspan);
        }
        if (colspan > 1) {
            printf(" colspan=\"%d\"", colspan);
        }
        print(">");
        for (Object o : content) {
            print(o);
        }
        println("</th>");
    }
}
