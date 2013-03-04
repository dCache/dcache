package org.dcache.util.expression;

import org.junit.Before;
import org.junit.Test;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.BasicParseRunner;
import org.parboiled.support.ParsingResult;

import static org.junit.Assert.assertEquals;

public class ExpressionTest
{
    private SymbolTable symbols;

    @Before
    public void setup()
    {
        symbols = new SymbolTable();
        symbols.put("number", 1.0);
    }

    private Expression createExpression(String s)
    {
        ExpressionParser parser =
            Parboiled.createParser(ExpressionParser.class);
        ParsingResult<Expression> result =
            new BasicParseRunner(parser.Top()).run(s);
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
        assertEquals(-1.0, evaluate("-1"));
        assertEquals(1.0, evaluate("1"));
        assertEquals(1.1, evaluate("1.1"));
        assertEquals(true, evaluate("true"));
        assertEquals(false, evaluate("false"));
        assertEquals("foo", evaluate("\"foo\""));
        assertEquals("foo", evaluate("'foo'"));

        assertEquals(1000.0, evaluate("1k"));
        assertEquals(1000.0, evaluate("1 K"));
        assertEquals(1000000.0, evaluate("1 M"));
        assertEquals(1000000000.0, evaluate("1 G"));
        assertEquals(1000000000000.0, evaluate("1 T"));
        assertEquals(1000000000000000.0, evaluate("1 P"));
        assertEquals(Math.scalb(1.0, 10), evaluate("1 Ki"));
        assertEquals(Math.scalb(1.0, 20), evaluate("1 Mi"));
        assertEquals(Math.scalb(1.0, 30), evaluate("1 Gi"));
        assertEquals(Math.scalb(1.0, 40), evaluate("1 Ti"));
        assertEquals(Math.scalb(1.0, 50), evaluate("1 Pi"));
    }

    @Test
    public void testOperators()
        throws Exception
    {
        assertEquals(2.0, evaluate("1+1"));
        assertEquals(0.0, evaluate("1-1"));
        assertEquals(4.0, evaluate("2*2"));
        assertEquals(2.5, evaluate("5/2"));
        assertEquals(1.0, evaluate("5%2"));
        assertEquals(27.0, evaluate("3**3"));

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

        assertEquals(-1.0, evaluate("--+-++1"));
        assertEquals(1.0, evaluate("-+-++1"));

        assertEquals(1.0, evaluate("true ? 1 : 2"));
        assertEquals(2.0, evaluate("false ? 1 : 2"));

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

    @Test
    public void testIdentifierWithTrailingSpace()
        throws Exception
    {
        assertEquals(1.0, evaluate("number "));
    }
}
