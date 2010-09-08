package org.dcache.util.expression;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.parboiled.Parboiled;
import org.parboiled.BasicParseRunner;
import org.parboiled.support.ParsingResult;

import org.dcache.util.expression.Expression;
import org.dcache.util.expression.ExpressionParser;
import org.dcache.util.expression.SymbolTable;
import org.dcache.util.expression.UnknownIdentifierException;
import org.dcache.util.expression.TypeMismatchException;

public class ExpressionTest
{
    private SymbolTable symbols;

    @Before
    public void setup()
    {
        symbols = new SymbolTable();
    }

    private Expression createExpression(String s)
    {
        ExpressionParser parser =
            Parboiled.createParser(ExpressionParser.class);
        ParsingResult<Expression> result =
            BasicParseRunner.run(parser.Top(), s);
        return result.resultValue;
    }

    private Expression createCheckedExpression(String s)
        throws TypeMismatchException, UnknownIdentifierException
    {
        Expression expression = createExpression(s);
        expression.check(symbols);
        return expression;
    }

    private Object evaluate(String s)
        throws TypeMismatchException, UnknownIdentifierException
    {
        Expression expression = createExpression(s);
        expression.check(symbols);
        return expression.evaluate(symbols);
    }

    @Test
    public void testLiterals()
        throws Exception
    {
        assertEquals(-1, evaluate("-1"));
        assertEquals(1, evaluate("1"));
        assertEquals(1.1, evaluate("1.1"));
        assertEquals(true, evaluate("true"));
        assertEquals(false, evaluate("false"));
        assertEquals("foo", evaluate("\"foo\""));
        assertEquals("foo", evaluate("'foo'"));

        assertEquals(1000L, evaluate("1k"));
        assertEquals(1000L, evaluate("1 K"));
        assertEquals(1000000L, evaluate("1 M"));
        assertEquals(1000000000L, evaluate("1 G"));
        assertEquals(1000000000000L, evaluate("1 T"));
        assertEquals(1000000000000000L, evaluate("1 P"));
        assertEquals(1L << 10, evaluate("1 Ki"));
        assertEquals(1L << 20, evaluate("1 Mi"));
        assertEquals(1L << 30, evaluate("1 Gi"));
        assertEquals(1L << 40, evaluate("1 Ti"));
        assertEquals(1L << 50, evaluate("1 Pi"));
    }

    @Test
    public void testOperators()
        throws Exception
    {
        assertEquals(2, evaluate("1+1"));
        assertEquals(0, evaluate("1-1"));
        assertEquals(4, evaluate("2*2"));
        assertEquals(2.5, evaluate("5/2"));
        assertEquals(1, evaluate("5%2"));
        assertEquals(27, evaluate("3**3"));

        assertEquals(true, evaluate("1 < 2"));
        assertEquals(false, evaluate("2 < 2"));
        assertEquals(true, evaluate("2 <= 2"));
        assertEquals(true, evaluate("2 == 2"));
        assertEquals(false, evaluate("2 != 2"));
        assertEquals(false, evaluate("2 > 2"));
        assertEquals(true, evaluate("2 >= 2"));

        assertEquals(true, evaluate("true == true"));
        assertEquals(false, evaluate("true != true"));

        assertEquals(true, evaluate("'bla' == \"bla\""));
        assertEquals(false, evaluate("'bla' == 'false'"));
        assertEquals(false, evaluate("'bla' != \"bla\""));
        assertEquals(true, evaluate("'bla' != 'false'"));

        assertEquals(-1, evaluate("--+-++1"));
        assertEquals(1, evaluate("-+-++1"));

        assertEquals(1, evaluate("true ? 1 : 2"));
        assertEquals(2, evaluate("false ? 1 : 2"));

        assertEquals(true, evaluate("true ? true : false"));
        assertEquals(false, evaluate("false ? true : false"));

        assertEquals("a", evaluate("true ? 'a' : 'b'"));
        assertEquals("b", evaluate("false ? 'a' : 'b'"));

        assertEquals(true, evaluate("'blabla' =~ '(bla)+'"));
        assertEquals(false, evaluate("'blabla' !~ '(bla)+'"));

        assertEquals(true, evaluate("true and true"));
        assertEquals(false, evaluate("true and false"));

        assertEquals(true, evaluate("true or false"));
        assertEquals(false, evaluate("false or false"));

        assertEquals(true, evaluate("not false"));
        assertEquals(false, evaluate("not true"));
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerIfCondition()
        throws Exception
    {
        evaluate("1 ? 2 :3");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerIfValues()
        throws Exception
    {
        evaluate("true ? 2 : false");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerNumberEqualsString()
        throws Exception
    {
        evaluate("2.1 == '1'");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerNumberAndNumber()
        throws Exception
    {
        evaluate("2.1 and 1");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerNotNumber()
        throws Exception
    {
        evaluate("not 2.1");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerUminusString()
        throws Exception
    {
        evaluate("-'bla'");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerBooleanLEBoolean()
        throws Exception
    {
        evaluate("true <= true");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerNumberPlusBoolean()
        throws Exception
    {
        evaluate("1+false");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerNumberPlusString()
        throws Exception
    {
        evaluate("1+'one'");
    }

    @Test(expected=TypeMismatchException.class)
    public void testTypeCheckerMatchNumber()
        throws Exception
    {
        evaluate("1 =~ 2");
    }
}