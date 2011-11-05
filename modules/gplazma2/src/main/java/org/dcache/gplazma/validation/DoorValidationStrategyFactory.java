package org.dcache.gplazma.validation;

/**
 *
 * @author timur
 */
public class DoorValidationStrategyFactory extends ValidationStrategyFactory {

    @Override
    public ValidationStrategy newValidationStrategy() {
        return new DoorValidationStrategy();
    }
}
