package org.dcache.util.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import org.junit.Test;

public class ConfigurationPropertiesAnnotatedKeyTests {

    public static final String PROPERTY_KEY_DEPRECATED = "property.deprecated";
    public static final String PROPERTY_KEY_FORBIDDEN = "property.forbidden";
    public static final String PROPERTY_KEY_OBSOLETE = "property.obsolete";
    public static final String PROPERTY_KEY_NOT_FOR_SERVICES = "property.not_for_services";
    public static final String PROPERTY_KEY_ONE_OF = "property.one-of";
    public static final String PROPERTY_KEY_ANY_OF = "property.any-of";
    public static final String PROPERTY_KEY_NOT_ANNOTATED = "property.not_annotated";
    public static final String PROPERTY_KEY_DEP_AND_NOT = "property.deprecated_and_not";
    public static final String PROPERTY_KEY_SCOPED_OBSOLETE = "scope/property.obsolete.scoped";
    public static final String PROPERTY_KEY_WITH_AWKWARD_NAME = "this-that+the.other";

    public static final String ANNOTATION_FOR_DEPRECATED = "(deprecated)";
    public static final String ANNOTATION_FOR_FORBIDDEN = "(forbidden)";
    public static final String ANNOTATION_FOR_OBSOLETE = "(obsolete)";
    public static final String ANNOTATION_FOR_NOT_FOR_SERVICES = "(not-for-services)";
    public static final String ANNOTATION_FOR_ONE_OF = "(one-of?true|false)";
    public static final String ANNOTATION_FOR_ANY_OF = "(any-of?none|null|notreally)";
    public static final String ANNOTATION_FOR_NOT_ANNOTATED = "";
    public static final String ANNOTATION_FOR_DEP_AND_NOT = "(deprecated,not-for-services)";

    public static final String DECLARATION_KEY_DEPRECATED =
          ANNOTATION_FOR_DEPRECATED + PROPERTY_KEY_DEPRECATED;
    public static final String DECLARATION_KEY_FORBIDDEN =
          ANNOTATION_FOR_FORBIDDEN + PROPERTY_KEY_FORBIDDEN;
    public static final String DECLARATION_KEY_OBSOLETE =
          ANNOTATION_FOR_OBSOLETE + PROPERTY_KEY_OBSOLETE;
    public static final String DECLARATION_KEY_NOT_FOR_SERVICES =
          ANNOTATION_FOR_NOT_FOR_SERVICES + PROPERTY_KEY_NOT_FOR_SERVICES;
    public static final String DECLARATION_KEY_ONE_OF = ANNOTATION_FOR_ONE_OF + PROPERTY_KEY_ONE_OF;
    public static final String DECLARATION_KEY_ANY_OF = ANNOTATION_FOR_ANY_OF + PROPERTY_KEY_ANY_OF;
    public static final String DECLARATION_KEY_NOT_ANNOTATED =
          ANNOTATION_FOR_NOT_ANNOTATED + PROPERTY_KEY_NOT_ANNOTATED;
    public static final String DECLARATION_KEY_DEP_AND_NOT =
          ANNOTATION_FOR_DEP_AND_NOT + PROPERTY_KEY_DEP_AND_NOT;
    public static final String DECLARATION_KEY_SCOPED_OBSOLETE =
          ANNOTATION_FOR_OBSOLETE + PROPERTY_KEY_SCOPED_OBSOLETE;
    public static final String DECLARATION_KEY_AWKWARD_NAME_DEPRECATED =
          ANNOTATION_FOR_DEPRECATED + PROPERTY_KEY_WITH_AWKWARD_NAME;

    public static final AnnotatedKey ANNOTATION_NOT_ANNOTATED =
          new AnnotatedKey(DECLARATION_KEY_NOT_ANNOTATED, "");
    public static final AnnotatedKey ANNOTATION_DEPRECATED =
          new AnnotatedKey(DECLARATION_KEY_DEPRECATED, "");
    public static final AnnotatedKey ANNOTATION_FORBIDDEN =
          new AnnotatedKey(DECLARATION_KEY_FORBIDDEN, "");
    public static final AnnotatedKey ANNOTATION_OBSOLETE =
          new AnnotatedKey(DECLARATION_KEY_OBSOLETE, "");
    public static final AnnotatedKey ANNOTATION_ONE_OF =
          new AnnotatedKey(DECLARATION_KEY_ONE_OF, "true");
    public static final AnnotatedKey ANNOTATION_ANY_OF =
          new AnnotatedKey(DECLARATION_KEY_ANY_OF, "none");
    public static final AnnotatedKey ANNOTATION_NOT_FOR_SERVICES =
          new AnnotatedKey(DECLARATION_KEY_NOT_FOR_SERVICES, "");
    public static final AnnotatedKey ANNOTATION_DEP_AND_NOT =
          new AnnotatedKey(DECLARATION_KEY_DEP_AND_NOT, "");
    public static final AnnotatedKey ANNOTATION_SCOPED_OBSOLETE =
          new AnnotatedKey(DECLARATION_KEY_SCOPED_OBSOLETE, "");
    public static final AnnotatedKey ANNOTATION_AWKWARD_NAME_DEPRECATED =
          new AnnotatedKey(DECLARATION_KEY_AWKWARD_NAME_DEPRECATED, "");

    public static final Set<Annotation> DEPRECATED = EnumSet.of(Annotation.DEPRECATED);
    public static final Set<Annotation> OBSOLETE = EnumSet.of(Annotation.OBSOLETE);
    public static final Set<Annotation> FORBIDDEN = EnumSet.of(Annotation.FORBIDDEN);
    public static final Set<Annotation> NOT_FOR_SERVICES = EnumSet.of(Annotation.NOT_FOR_SERVICES);
    public static final Set<Annotation> ONE_OF = EnumSet.of(Annotation.ONE_OF);
    public static final Set<Annotation> ANY_OF = EnumSet.of(Annotation.ANY_OF);
    public static final Set<Annotation> DEPRECATED_OBSOLETE = EnumSet.of(Annotation.DEPRECATED,
          Annotation.OBSOLETE);
    public static final Set<Annotation> DEPRECATED_FORBIDDEN = EnumSet.of(Annotation.DEPRECATED,
          Annotation.FORBIDDEN);
    public static final Set<Annotation> DEPRECATED_NOT_FOR_SERVICES = EnumSet.of(
          Annotation.DEPRECATED, Annotation.NOT_FOR_SERVICES);
    public static final Set<Annotation> DEPRECATED_ONE_OF = EnumSet.of(Annotation.DEPRECATED,
          Annotation.ONE_OF);
    public static final Set<Annotation> DEPRECATED_ANY_OF = EnumSet.of(Annotation.DEPRECATED,
          Annotation.ANY_OF);
    public static final Set<Annotation> OBSOLETE_FORBIDDEN = EnumSet.of(Annotation.OBSOLETE,
          Annotation.FORBIDDEN);
    public static final Set<Annotation> OBSOLETE_NOT_FOR_SERVICES = EnumSet.of(Annotation.OBSOLETE,
          Annotation.NOT_FOR_SERVICES);
    public static final Set<Annotation> OBSOLETE_ONE_OF = EnumSet.of(Annotation.OBSOLETE,
          Annotation.ONE_OF);
    public static final Set<Annotation> OBSOLETE_ANY_OF = EnumSet.of(Annotation.OBSOLETE,
          Annotation.ANY_OF);
    public static final Set<Annotation> FORBIDDEN_NOT_FOR_SERVICES = EnumSet.of(
          Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES);
    public static final Set<Annotation> FORBIDDEN_ONE_OF = EnumSet.of(Annotation.FORBIDDEN,
          Annotation.ONE_OF);
    public static final Set<Annotation> FORBIDDEN_ANY_OF = EnumSet.of(Annotation.FORBIDDEN,
          Annotation.ANY_OF);
    public static final Set<Annotation> NOT_FOR_SERVICES_ONE_OF = EnumSet.of(
          Annotation.NOT_FOR_SERVICES, Annotation.ONE_OF);
    public static final Set<Annotation> NOT_FOR_SERVICES_ANY_OF = EnumSet.of(
          Annotation.NOT_FOR_SERVICES, Annotation.ANY_OF);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_FORBIDDEN = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.FORBIDDEN);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_NOT_FOR_SERVICES = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.NOT_FOR_SERVICES);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_ONE_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.ONE_OF);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_ANY_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.ANY_OF);
    public static final Set<Annotation> DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES = EnumSet.of(
          Annotation.DEPRECATED, Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES);
    public static final Set<Annotation> DEPRECATED_FORBIDDEN_ONE_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.FORBIDDEN, Annotation.ONE_OF);
    public static final Set<Annotation> DEPRECATED_FORBIDDEN_ANY_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.FORBIDDEN, Annotation.ANY_OF);
    public static final Set<Annotation> DEPRECATED_NOT_FOR_SERVICES_ONE_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.NOT_FOR_SERVICES, Annotation.ONE_OF);
    public static final Set<Annotation> DEPRECATED_NOT_FOR_SERVICES_ANY_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.NOT_FOR_SERVICES, Annotation.ANY_OF);
    public static final Set<Annotation> OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES = EnumSet.of(
          Annotation.OBSOLETE, Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES);
    public static final Set<Annotation> OBSOLETE_FORBIDDEN_ONE_OF = EnumSet.of(Annotation.OBSOLETE,
          Annotation.FORBIDDEN, Annotation.ONE_OF);
    public static final Set<Annotation> OBSOLETE_FORBIDDEN_ANY_OF = EnumSet.of(Annotation.OBSOLETE,
          Annotation.FORBIDDEN, Annotation.ANY_OF);
    public static final Set<Annotation> OBSOLETE_NOT_FOR_SERVICES_ONE_OF = EnumSet.of(
          Annotation.OBSOLETE, Annotation.NOT_FOR_SERVICES, Annotation.ONE_OF);
    public static final Set<Annotation> OBSOLETE_NOT_FOR_SERVICES_ANY_OF = EnumSet.of(
          Annotation.OBSOLETE, Annotation.NOT_FOR_SERVICES, Annotation.ANY_OF);
    public static final Set<Annotation> FORBIDDEN_NOT_FOR_SERVICES_ONE_OF = EnumSet.of(
          Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES, Annotation.ONE_OF);
    public static final Set<Annotation> FORBIDDEN_NOT_FOR_SERVICES_ANY_OF = EnumSet.of(
          Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES, Annotation.ANY_OF);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.FORBIDDEN,
          Annotation.NOT_FOR_SERVICES);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.FORBIDDEN, Annotation.ONE_OF);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_FORBIDDEN_ANY_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.FORBIDDEN, Annotation.ANY_OF);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.NOT_FOR_SERVICES,
          Annotation.ONE_OF);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ANY_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.NOT_FOR_SERVICES,
          Annotation.ANY_OF);
    public static final Set<Annotation> DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES,
          Annotation.ONE_OF);
    public static final Set<Annotation> DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES,
          Annotation.ANY_OF);
    public static final Set<Annotation> OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF = EnumSet.of(
          Annotation.OBSOLETE, Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES,
          Annotation.ONE_OF);
    public static final Set<Annotation> OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF = EnumSet.of(
          Annotation.OBSOLETE, Annotation.FORBIDDEN, Annotation.NOT_FOR_SERVICES,
          Annotation.ANY_OF);

    public static final Set<Annotation> DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.FORBIDDEN,
          Annotation.NOT_FOR_SERVICES, Annotation.ONE_OF);
    public static final Set<Annotation> DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF = EnumSet.of(
          Annotation.DEPRECATED, Annotation.OBSOLETE, Annotation.FORBIDDEN,
          Annotation.NOT_FOR_SERVICES, Annotation.ANY_OF);

    @Test
    public void testNotAnnotatedGetPropertyName() {
        assertEquals(PROPERTY_KEY_NOT_ANNOTATED, ANNOTATION_NOT_ANNOTATED.getPropertyName());
    }

    @Test
    public void testNotAnnotatedIsDeprecated() {
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testNotAnnotatedHasAnyOf() {
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(OBSOLETE));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(FORBIDDEN));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(ONE_OF));

        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_OBSOLETE));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));

        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));

        assertFalse(
              ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(
              ANNOTATION_NOT_ANNOTATED.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));

        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnyOf(
              DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
    }

    @Test
    public void testNotAnnotatedIsForbidden() {
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testNotAnnotatedIsObsolete() {
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testNotAnnotatedIsNotForServices() {
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testNotAnnotatedIsOneOf() {
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnnotation(Annotation.ONE_OF));
    }

    @Test
    public void testNotAnnotatedHasAnnotations() {
        assertFalse(ANNOTATION_NOT_ANNOTATED.hasAnnotations());
    }

    @Test
    public void testNotAnnotatedGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_NOT_ANNOTATED,
              ANNOTATION_NOT_ANNOTATED.getAnnotationDeclaration());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testDeprecatedAndObsoleteAnnotatedDeclarationCreation() {
        new AnnotatedKey("(deprecated,obsolete)foo", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeprecatedAndForbiddenAnnotatedDeclarationCreation() {
        new AnnotatedKey("(deprecated,forbidden)foo", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testObsoleteAndForbiddenAnnotatedDeclarationCreation() {
        new AnnotatedKey("(obsolete,forbidden)foo", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeprecatedObsoleteAndForbiddenAnnotatedDeclarationCreation() {
        new AnnotatedKey("(deprecated,obsolete,forbidden)foo", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownAnnotatedDeclarationCreation() {
        new AnnotatedKey("(gobbledygook)foo", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTwoUnknownAnnotatedDeclarationCreation() {
        new AnnotatedKey("(gobbledygook,fandango)foo", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAnnotationTakesParameterButNoneGiven() {
        new AnnotatedKey("(one-of)foo", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAnnotationDoesntTakeParameterButParameterGiven() {
        new AnnotatedKey("(obsolete:foo)foo", "");
    }


    @Test
    public void testDeprecatedGetPropertyName() {
        assertEquals(PROPERTY_KEY_DEPRECATED, ANNOTATION_DEPRECATED.getPropertyName());
    }

    @Test
    public void testAwkwardNameGetPropertyName() {
        assertEquals(PROPERTY_KEY_WITH_AWKWARD_NAME,
              ANNOTATION_AWKWARD_NAME_DEPRECATED.getPropertyName());
    }

    @Test
    public void testDeprecatedHasAnyOf() {
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(FORBIDDEN));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(ANY_OF));

        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_ONE_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_ANY_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_ANY_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(NOT_FOR_SERVICES_ANY_OF));

        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_ANY_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_DEPRECATED.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(
              DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_DEPRECATED.hasAnyOf(
              DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
    }

    @Test
    public void testDeprecatedIsDeprecated() {
        assertTrue(ANNOTATION_DEPRECATED.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testDeprecatedIsForbidden() {
        assertFalse(ANNOTATION_DEPRECATED.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testDeprecatedIsObsolete() {
        assertFalse(ANNOTATION_DEPRECATED.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testDeprecatedIsNotForServices() {
        assertFalse(ANNOTATION_DEPRECATED.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testDeprecatedIsOneOf() {
        assertFalse(ANNOTATION_DEPRECATED.hasAnnotation(Annotation.ONE_OF));
    }

    @Test
    public void testDeprecatedHasAnnotations() {
        assertTrue(ANNOTATION_DEPRECATED.hasAnnotations());
    }

    @Test
    public void testDeprecatedGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_DEPRECATED, ANNOTATION_DEPRECATED.getAnnotationDeclaration());
    }


    @Test
    public void testAwkwardNameIsDeprecated() {
        assertTrue(ANNOTATION_AWKWARD_NAME_DEPRECATED.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testAwkwardNameIsForbidden() {
        assertFalse(ANNOTATION_AWKWARD_NAME_DEPRECATED.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testAwkwardNameIsObsolete() {
        assertFalse(ANNOTATION_AWKWARD_NAME_DEPRECATED.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testAwkwardNameIsNotForServices() {
        assertFalse(ANNOTATION_AWKWARD_NAME_DEPRECATED.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testAwkwardNameHasAnnotations() {
        assertTrue(ANNOTATION_AWKWARD_NAME_DEPRECATED.hasAnnotations());
    }

    @Test
    public void testAwkwardNameGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_DEPRECATED,
              ANNOTATION_DEPRECATED.getAnnotationDeclaration());
    }


    @Test
    public void testForbiddenGetPropertyName() {
        assertEquals(PROPERTY_KEY_FORBIDDEN, ANNOTATION_FORBIDDEN.getPropertyName());
    }

    @Test
    public void testForbiddenHasAnyOf() {
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(FORBIDDEN));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(ONE_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(ANY_OF));

        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_ONE_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_ANY_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_ANY_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));

        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_ANY_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_FORBIDDEN.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(
              ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(
              ANNOTATION_FORBIDDEN.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
    }

    @Test
    public void testForbiddenIsDeprecated() {
        assertFalse(ANNOTATION_FORBIDDEN.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testForbiddenIsForbidden() {
        assertTrue(ANNOTATION_FORBIDDEN.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testForbiddenIsObsolete() {
        assertFalse(ANNOTATION_FORBIDDEN.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testForbiddenIsNotForServices() {
        assertFalse(ANNOTATION_FORBIDDEN.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testForbiddenIsOneOf() {
        assertFalse(ANNOTATION_FORBIDDEN.hasAnnotation(Annotation.ONE_OF));
    }

    @Test
    public void testForbiddenHasAnnotations() {
        assertTrue(ANNOTATION_FORBIDDEN.hasAnnotations());
    }

    @Test
    public void testForbiddenGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_FORBIDDEN, ANNOTATION_FORBIDDEN.getAnnotationDeclaration());
    }


    @Test
    public void testObsoleteGetPropertyName() {
        assertEquals(PROPERTY_KEY_OBSOLETE, ANNOTATION_OBSOLETE.getPropertyName());
    }

    @Test
    public void testObsoleteHasAnyOf() {
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(FORBIDDEN));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(ONE_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(ANY_OF));

        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_ONE_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_ANY_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_ONE_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_ANY_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(NOT_FOR_SERVICES_ANY_OF));

        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_ANY_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(
              ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(
              ANNOTATION_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
    }

    @Test
    public void testObsoleteIsDeprecated() {
        assertFalse(ANNOTATION_OBSOLETE.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testObsoleteIsForbidden() {
        assertFalse(ANNOTATION_OBSOLETE.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testObsoleteIsObsolete() {
        assertTrue(ANNOTATION_OBSOLETE.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testObsoleteIsNotForServices() {
        assertFalse(ANNOTATION_OBSOLETE.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testObsoleteIsOneOf() {
        assertFalse(ANNOTATION_OBSOLETE.hasAnnotation(Annotation.ONE_OF));
    }

    @Test
    public void testObsoleteHasAnnotations() {
        assertTrue(ANNOTATION_OBSOLETE.hasAnnotations());
    }

    @Test
    public void testObsoleteGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_OBSOLETE, ANNOTATION_OBSOLETE.getAnnotationDeclaration());
    }


    @Test
    public void testNotForServicesGetPropertyName() {
        assertEquals(PROPERTY_KEY_NOT_FOR_SERVICES, ANNOTATION_NOT_FOR_SERVICES.getPropertyName());
    }

    @Test
    public void testNotForServicesHasAnyOf() {
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(FORBIDDEN));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(ONE_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(ANY_OF));

        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_ONE_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_ANY_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_ANY_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(NOT_FOR_SERVICES_ANY_OF));

        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_ANY_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(
              ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ANY_OF));
        assertTrue(
              ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(
              ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(
              ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(
              ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(
              ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(
              ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(
              DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnyOf(
              DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
    }

    @Test
    public void testNotForServicesIsDeprecated() {
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testNotForServicesIsForbidden() {
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testNotForServicesObsoleteIsObsolete() {
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testNotForServicesIsNotForServices() {
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testNotForServicesIsOneOf() {
        assertFalse(ANNOTATION_NOT_FOR_SERVICES.hasAnnotation(Annotation.ONE_OF));
    }

    @Test
    public void testNotForServiceHasAnnotations() {
        assertTrue(ANNOTATION_NOT_FOR_SERVICES.hasAnnotations());
    }

    @Test
    public void testNotForServicesGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_NOT_FOR_SERVICES,
              ANNOTATION_NOT_FOR_SERVICES.getAnnotationDeclaration());
    }


    @Test
    public void testOneOfGetPropertyName() {
        assertEquals(PROPERTY_KEY_ONE_OF, ANNOTATION_ONE_OF.getPropertyName());
    }

    @Test
    public void testOneOfIsDeprecated() {
        assertFalse(ANNOTATION_ONE_OF.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testOneOfHasAnyOf() {
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(FORBIDDEN));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(ANY_OF));

        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_ANY_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(NOT_FOR_SERVICES_ANY_OF));

        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ANY_OF));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertTrue(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_ONE_OF.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));

        assertTrue(
              ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(
              ANNOTATION_ONE_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
    }

    @Test
    public void testOneOfIsForbidden() {
        assertFalse(ANNOTATION_ONE_OF.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testOneOfIsObsolete() {
        assertFalse(ANNOTATION_ONE_OF.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testOneOfIsNotForServices() {
        assertFalse(ANNOTATION_ONE_OF.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testOneOfIsOneOf() {
        assertTrue(ANNOTATION_ONE_OF.hasAnnotation(Annotation.ONE_OF));
    }

    @Test
    public void testOneOfHasAnnotations() {
        assertTrue(ANNOTATION_ONE_OF.hasAnnotations());
    }

    @Test
    public void testOneOfGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_ONE_OF, ANNOTATION_ONE_OF.getAnnotationDeclaration());
    }


    @Test
    public void testAnyOfGetPropertyName() {
        assertEquals(PROPERTY_KEY_ANY_OF, ANNOTATION_ANY_OF.getPropertyName());
    }

    @Test
    public void testAnyOfIsDeprecated() {
        assertFalse(ANNOTATION_ANY_OF.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testAnyOfHasAnyOf() {
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(FORBIDDEN));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(ONE_OF));

        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_ONE_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));

        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));

        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(ANNOTATION_ANY_OF.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));

        assertTrue(
              ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ANY_OF));
        assertFalse(
              ANNOTATION_ANY_OF.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
    }

    @Test
    public void testAnyOfIsForbidden() {
        assertFalse(ANNOTATION_ANY_OF.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testAnyOfIsObsolete() {
        assertFalse(ANNOTATION_ANY_OF.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testAnyOfIsNotForServices() {
        assertFalse(ANNOTATION_ANY_OF.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testAnyOfIsAnyOf() {
        assertTrue(ANNOTATION_ANY_OF.hasAnnotation(Annotation.ANY_OF));
    }

    @Test
    public void testAnyOfHasAnnotations() {
        assertTrue(ANNOTATION_ANY_OF.hasAnnotations());
    }

    @Test
    public void testAnyOfGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_ANY_OF, ANNOTATION_ANY_OF.getAnnotationDeclaration());
    }


    @Test
    public void testDepAndNotGetPropertyName() {
        assertEquals(PROPERTY_KEY_DEP_AND_NOT, ANNOTATION_DEP_AND_NOT.getPropertyName());
    }

    @Test
    public void testDepAndNotHasAnyOf() {
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED));
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnyOf(OBSOLETE));
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnyOf(FORBIDDEN));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnyOf(ONE_OF));

        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_OBSOLETE));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_ONE_OF));
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnyOf(OBSOLETE_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnyOf(FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));

        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));

        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));

        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnyOf(
              DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
    }

    @Test
    public void testDepAndNotIsDeprecated() {
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testDepAndNotIsForbidden() {
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testDepAndNotObsoleteIsObsolete() {
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testDepAndNotIsNotForServices() {
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testDepAndNotIsOneOf() {
        assertFalse(ANNOTATION_DEP_AND_NOT.hasAnnotation(Annotation.ONE_OF));
    }

    @Test
    public void testDepAndNotHasAnnotations() {
        assertTrue(ANNOTATION_DEP_AND_NOT.hasAnnotations());
    }

    @Test
    public void testDepAndNotGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_DEP_AND_NOT, ANNOTATION_DEP_AND_NOT.getAnnotationDeclaration());
    }


    @Test
    public void testScopedObsoleteGetPropertyName() {
        assertEquals(PROPERTY_KEY_SCOPED_OBSOLETE, ANNOTATION_SCOPED_OBSOLETE.getPropertyName());
    }

    @Test
    public void testScopedObsoleteHasAnyOf() {
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(EnumSet.noneOf(Annotation.class)));

        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(OBSOLETE));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(FORBIDDEN));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(ONE_OF));

        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_ONE_OF));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(NOT_FOR_SERVICES_ONE_OF));

        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_ONE_OF));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN_ONE_OF));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));

        assertTrue(
              ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_FORBIDDEN_ONE_OF));
        assertTrue(
              ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_OBSOLETE_NOT_FOR_SERVICES_ONE_OF));
        assertFalse(
              ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(DEPRECATED_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));

        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnyOf(
              DEPRECATED_OBSOLETE_FORBIDDEN_NOT_FOR_SERVICES_ONE_OF));
    }

    @Test
    public void testScopedObsoleteIsDeprecated() {
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnnotation(Annotation.DEPRECATED));
    }

    @Test
    public void testScopedObsoleteIsForbidden() {
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnnotation(Annotation.FORBIDDEN));
    }

    @Test
    public void testScopedObsoleteIsObsolete() {
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnnotation(Annotation.OBSOLETE));
    }

    @Test
    public void testScopedObsoleteIsNotForServices() {
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnnotation(Annotation.NOT_FOR_SERVICES));
    }

    @Test
    public void testScopedObsoleteIsOneOf() {
        assertFalse(ANNOTATION_SCOPED_OBSOLETE.hasAnnotation(Annotation.ONE_OF));
    }

    @Test
    public void testScopedObsoleteHasAnnotations() {
        assertTrue(ANNOTATION_SCOPED_OBSOLETE.hasAnnotations());
    }

    @Test
    public void testScopedObsoleteGetAnnotationDeclaration() {
        assertEquals(ANNOTATION_FOR_OBSOLETE,
              ANNOTATION_SCOPED_OBSOLETE.getAnnotationDeclaration());
    }
}
