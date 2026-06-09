package gov.nih.nci.bento.support;

import graphql.GraphQL;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;

import java.util.Map;

/**
 * Helpers for building minimal GraphQL schemas in controller tests.
 */
public final class GraphQLTestSupport {

    private GraphQLTestSupport() {
    }

    public static GraphQL graphQLWithQueryFields(Map<String, DataFetcher<?>> fetchers) throws Exception {
        GraphQLSchema schema = schemaWithQueryFields(fetchers);
        return GraphQL.newGraphQL(schema).build();
    }

    public static GraphQLSchema schemaWithQueryFields(Map<String, DataFetcher<?>> fetchers) throws Exception {
        StringBuilder queryFields = new StringBuilder("type Query {");
        for (String fieldName : fetchers.keySet()) {
            queryFields.append(' ').append(fieldName).append(": String");
        }
        queryFields.append(" }");

        TypeDefinitionRegistry registry = new SchemaParser().parse(queryFields.toString());
        RuntimeWiring.Builder wiringBuilder = RuntimeWiring.newRuntimeWiring();
        wiringBuilder.type(
                "Query",
                typeWiring -> {
                    fetchers.forEach(typeWiring::dataFetcher);
                    return typeWiring;
                });
        return new SchemaGenerator().makeExecutableSchema(registry, wiringBuilder.build());
    }
}
