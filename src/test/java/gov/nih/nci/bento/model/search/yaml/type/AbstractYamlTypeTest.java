package gov.nih.nci.bento.model.search.yaml.type;

import gov.nih.nci.bento.constants.Const;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 6: unit tests for {@link AbstractYamlType} helpers.
 */
class AbstractYamlTypeTest {

    @Test
    void resolveYamlFileName_privateAccess_usesBaseFileName() throws Exception {
        assertEquals(
                "single_search_es.yml",
                YamlTypeTestSupport.resolveYamlFileName(
                        Const.ES_ACCESS_TYPE.PRIVATE, Const.YAML_QUERY.FILE_NAMES_BENTO.SINGLE));
    }

    @Test
    void resolveYamlFileName_publicAccess_prefixesFileName() throws Exception {
        assertEquals(
                "public_single_search_es.yml",
                YamlTypeTestSupport.resolveYamlFileName(
                        Const.ES_ACCESS_TYPE.PUBLIC, Const.YAML_QUERY.FILE_NAMES_BENTO.SINGLE));
    }

    @Test
    void yamlResourceExists_privateSingleFile_isOnClasspath() throws Exception {
        assertTrue(YamlTypeTestSupport.yamlResourceExists(
                Const.ES_ACCESS_TYPE.PRIVATE, Const.YAML_QUERY.FILE_NAMES_BENTO.SINGLE));
    }

    @Test
    void yamlResourceExists_publicSingleFile_isMissing() throws Exception {
        assertFalse(YamlTypeTestSupport.yamlResourceExists(
                Const.ES_ACCESS_TYPE.PUBLIC, Const.YAML_QUERY.FILE_NAMES_BENTO.SINGLE));
    }
}
