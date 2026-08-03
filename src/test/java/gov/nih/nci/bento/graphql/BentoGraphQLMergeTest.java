package gov.nih.nci.bento.graphql;

import gov.nih.nci.bento.support.GraphQLTestSupport;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Phase 7: unit tests for {@link BentoGraphQL} schema merge helpers.
 */
class BentoGraphQLMergeTest {

    @Test
    void mergeType_combinesFieldDefinitionsFromBothTypes() throws Exception {
        GraphQLObjectType left = (GraphQLObjectType) schemaWithField("leftField").getQueryType();
        GraphQLObjectType right = (GraphQLObjectType) schemaWithField("rightField").getQueryType();

        GraphQLObjectType merged = invokeMergeType(left, right);

        assertNotNull(merged.getFieldDefinition("leftField"));
        assertNotNull(merged.getFieldDefinition("rightField"));
    }

    @Test
    void mergeType_nullLeft_returnsRight() throws Exception {
        GraphQLObjectType right = (GraphQLObjectType) schemaWithField("rightField").getQueryType();

        GraphQLObjectType merged = invokeMergeType(null, right);

        assertEquals(right, merged);
    }

    @Test
    void mergeSchema_combinesQueryFieldsFromBothSchemas() throws Exception {
        GraphQLSchema neo4jSchema = schemaWithField("neo4jField");
        GraphQLSchema esSchema = schemaWithField("esField");

        GraphQLSchema merged = invokeMergeSchema(neo4jSchema, esSchema);

        assertNotNull(merged.getQueryType().getFieldDefinition("neo4jField"));
        assertNotNull(merged.getQueryType().getFieldDefinition("esField"));
    }

    @Test
    void mergeSchema_nullLeft_returnsRightSchema() throws Exception {
        GraphQLSchema esSchema = schemaWithField("esField");

        GraphQLSchema merged = invokeMergeSchema(null, esSchema);

        assertEquals(esSchema, merged);
    }

    @Test
    void mergeSchema_nullRight_returnsLeftSchema() throws Exception {
        GraphQLSchema neo4jSchema = schemaWithField("neo4jField");

        GraphQLSchema merged = invokeMergeSchema(neo4jSchema, null);

        assertEquals(neo4jSchema, merged);
    }

    @Test
    void removeQueryMutationSubscription_removesRootOperationTypes() throws Exception {
        GraphQLSchema schema = schemaWithField("fieldA");
        var allTypes = new java.util.HashMap<String, GraphQLNamedType>(schema.getTypeMap());

        @SuppressWarnings("unchecked")
        java.util.HashMap<String, GraphQLNamedType> filtered =
                (java.util.HashMap<String, GraphQLNamedType>) invokeRemoveQueryMutationSubscription(allTypes, schema);

        assertNull(filtered.get("Query"));
    }

    private static GraphQLSchema schemaWithField(String fieldName) throws Exception {
        return GraphQLTestSupport.schemaWithQueryFields(Map.of(fieldName, (DataFetcher<String>) env -> fieldName));
    }

    private static GraphQLObjectType invokeMergeType(GraphQLObjectType left, GraphQLObjectType right) throws Exception {
        Method method = BentoGraphQL.class.getDeclaredMethod("mergeType", GraphQLObjectType.class, GraphQLObjectType.class);
        method.setAccessible(true);
        return (GraphQLObjectType) method.invoke(allocateInstance(), left, right);
    }

    private static GraphQLSchema invokeMergeSchema(GraphQLSchema left, GraphQLSchema right) throws Exception {
        Method method = BentoGraphQL.class.getDeclaredMethod("mergeSchema", GraphQLSchema.class, GraphQLSchema.class);
        method.setAccessible(true);
        return (GraphQLSchema) method.invoke(allocateInstance(), left, right);
    }

    private static Object invokeRemoveQueryMutationSubscription(
            java.util.HashMap<String, GraphQLNamedType> allTypes, GraphQLSchema schema) throws Exception {
        Method method = BentoGraphQL.class.getDeclaredMethod(
                "removeQueryMutationSubscription", java.util.HashMap.class, GraphQLSchema.class);
        method.setAccessible(true);
        return method.invoke(allocateInstance(), allTypes, schema);
    }

    private static Object allocateInstance() throws Exception {
        Constructor<sun.misc.Unsafe> unsafeConstructor = sun.misc.Unsafe.class.getDeclaredConstructor();
        unsafeConstructor.setAccessible(true);
        sun.misc.Unsafe unsafe = unsafeConstructor.newInstance();
        return unsafe.allocateInstance(BentoGraphQL.class);
    }
}
