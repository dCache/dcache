package org.dcache.util.expression;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.parboiled.trees.ImmutableTreeNode;

import java.util.Arrays;

import static org.dcache.util.expression.Token.NUMBER_LITERAL;
import static org.dcache.util.expression.Type.UNKNOWN;

/**
 * Homogeneous AST node for the expression language.
 */
public class Expression extends ImmutableTreeNode<Expression>
{
    private final double _number;
    private final String _string;
    private final Token _token;
    private Type _type = UNKNOWN;

    public Expression(double number) {
        super(ImmutableList.of());
        _token = NUMBER_LITERAL;
        _number = number;
        _string = null;
    }


    public Expression(Token token, String string) {
        super(ImmutableList.of());
        _token = token;
        _number = 0.0;
        _string = string;
    }

    public Expression(Token token, Expression ... operands) {
        super(Arrays.asList(operands));
        _token = token;
        _number = 0.0;
        _string = null;
    }

    public Type check(SymbolTable symbols)
        throws TypeMismatchException, UnknownIdentifierException
    {
        TypeChecker checker = new TypeChecker(symbols);
        return checker.check(this);
    }

    public Object evaluate(SymbolTable symbols)
    {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(symbols);
        return evaluator.evaluate(this);
    }

    public boolean evaluateBoolean(SymbolTable symbols)
    {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(symbols);
        return evaluator.evaluateBoolean(this);
    }

    public double evaluateDouble(SymbolTable symbols)
    {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(symbols);
        return evaluator.evaluateDouble(this);
    }

    public String evaluateString(SymbolTable symbols)
    {
        ExpressionEvaluator evaluator = new ExpressionEvaluator(symbols);
        return evaluator.evaluateString(this);
    }

    public Type getType()
    {
        return _type;
    }

    public void setType(Type type)
    {
        _type = type;
    }

    public double getNumber()
    {
        return _number;
    }

    public String getString()
    {
        return _string;
    }

    public Token getToken()
    {
        return _token;
    }

    public Expression get(int child)
    {
        return getChildren().get(child);
    }

    @Override
    public String toString() {
        switch (_token) {
        case NUMBER_LITERAL:
            return String.valueOf(_number);
        case STRING_LITERAL:
            return "\"" + _string + "\"";
        case IDENTIFIER:
            return _string;
        default:
            return "(" + _token.label + " " + Joiner.on(" ").join(getChildren()) + ")";
        }
    }
}
