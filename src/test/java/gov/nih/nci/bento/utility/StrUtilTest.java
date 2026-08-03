package gov.nih.nci.bento.utility;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StrUtilTest {

    @Test
    void getBoolText_extractsTrueCaseInsensitive() {
        assertEquals("true", StrUtil.getBoolText("Patient is TRUE positive"));
    }

    @Test
    void getBoolText_extractsFalse() {
        assertEquals("false", StrUtil.getBoolText("result: False"));
    }

    @Test
    void getBoolText_noMatch_returnsEmpty() {
        assertEquals("", StrUtil.getBoolText("unknown"));
    }

    @Test
    void getBoolText_nullInput_returnsEmpty() {
        assertEquals("", StrUtil.getBoolText(null));
    }

    @Test
    void getIntText_extractsFirstInteger() {
        assertEquals("42", StrUtil.getIntText("age 42 years"));
    }

    @Test
    void getIntText_noDigits_returnsEmpty() {
        assertEquals("", StrUtil.getIntText("no numbers here"));
    }
}
