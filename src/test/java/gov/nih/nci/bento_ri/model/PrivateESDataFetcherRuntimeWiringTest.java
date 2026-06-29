package gov.nih.nci.bento_ri.model;

import gov.nih.nci.bento_ri.service.InventoryESService;
import graphql.schema.idl.RuntimeWiring;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Phase 4: smoke tests for GraphQL runtime wiring on {@link PrivateESDataFetcher}.
 */
@ExtendWith(MockitoExtension.class)
class PrivateESDataFetcherRuntimeWiringTest {

    @Test
    void buildRuntimeWiring_loadsYamlAndPrivateFetchers() throws Exception {
        InventoryESService inventoryESService = mock(InventoryESService.class);
        PrivateESDataFetcher fetcher = PrivateESDataFetcherTestSupport.newFetcher(inventoryESService);

        RuntimeWiring wiring = fetcher.buildRuntimeWiring();

        assertNotNull(wiring);
    }
}
