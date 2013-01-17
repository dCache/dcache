package org.dcache.util.expression;

import java.util.Set;

import static org.dcache.util.expression.Type.*;
import static org.dcache.util.expression.Token.*;

/**
 * Type checker and annotator for expressions.
 */
public class TypeChecker
{
    private final SymbolTable _symbols;

    public TypeChecker(SymbolTable symbols)
    {
        _symbols = symbols;
    }

    private Type ensureSame(Type actual, Type expected)
        throws TypeMismatchException
    {
        if (actual != expected) {
            throw new TypeMismatchException("Type mismatch: Expected " + expected + ", but found " + actual);
        }
        return actual;
    }

    public void check(Expression expression, Type expected)
        throws TypeMismatchException, UnknownIdentifierException
    {
        ensureSame(check(expression), expected);
    }

    public Type check(Expression expression)
        throws TypeMismatchException, UnknownIdentifierException
    {
        switch (expression.getToken()) {
        case NUMBER_LITERAL:
            expression.setType(DOUBLE);
            break;
        case STRING_LITERAL:
            expression.setType(STRING);
            break;
        case IDENTIFIER:
            String identifier = expression.getString();
            Symbol symbol = _symbols.get(identifier);
            if (symbol == null) {
                throw new UnknownIdentifierException(identifier);
            }
            expression.setType(symbol.getType());
            break;
        case TRUE:
            expression.setType(BOOLEAN);
            break;
        case FALSE:
            expression.setType(BOOLEAN);
            break;
        case IF:
            ensureSame(check(expression.get(0)), BOOLEAN);
            expression.setType(ensureSame(check(expression.get(1)),
                                          check(expression.get(2))));
            break;

        case EQ:
        case NE:
            ensureSame(check(expression.get(0)),
                       check(expression.get(1)));
            expression.setType(BOOLEAN);
            break;

        case AND:
        case OR:
            ensureSame(check(expression.get(0)), BOOLEAN);
            ensureSame(check(expression.get(1)), BOOLEAN);
            expression.setType(BOOLEAN);
            break;

        case NOT:
            ensureSame(check(expression.get(0)), BOOLEAN);
            expression.setType(BOOLEAN);
            break;

        case UMINUS:
            ensureSame(check(expression.get(0)), DOUBLE);
            expression.setType(DOUBLE);
            break;

        case LT:
        case LE:
        case GT:
        case GE:
            ensureSame(check(expression.get(0)), DOUBLE);
            ensureSame(check(expression.get(1)), DOUBLE);
            expression.setType(BOOLEAN);
            break;

        case MINUS:
        case PLUS:
        case MULT:
        case DIV:
        case MOD:
        case POWER:
            ensureSame(check(expression.get(0)), DOUBLE);
            ensureSame(check(expression.get(1)), DOUBLE);
            expression.setType(DOUBLE);
            break;

        case MATCH:
        case NOT_MATCH:
            ensureSame(check(expression.get(0)), STRING);
            ensureSame(check(expression.get(1)), STRING);
            expression.setType(BOOLEAN);
            break;
        }
        return expression.getType();
    }
}
