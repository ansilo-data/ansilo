package com.ansilo.connectors.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import org.junit.jupiter.api.Test;

public class LoggedParamTest {
    @Test
    void testNew() throws Exception {
        var param = new LoggedParam(1, "setLong", (Long) 1234L);

        assertEquals(1, param.getIndex());
        assertEquals("setLong", param.getJdbcMethod());
        assertEquals((Long) 1234L, param.getValue());
    }

    @Test
    void testEqual() throws Exception {
        var param1 = new LoggedParam(1, "setLong", (Long) 1234L);
        var param2 = new LoggedParam(1, "setLong", (Long) 1234L);
        var param3 = new LoggedParam(2, "setString", "foo");
        var param4 = new LoggedParam(2, "setString", "bar");

        assertEquals(param1, param2);
        assertNotEquals(param1, param3);
        assertNotEquals(param1, param4);
        assertNotEquals(param3, param4);
    }

    @Test
    void testToString() throws Exception {
        var param1 = new LoggedParam(1, "setInt", (Long) 1234L);
        var param2 = new LoggedParam(1, "setInt", (Long) 1235L);
        var param3 = new LoggedParam(2, "setString", "foo");
        var param4 = new LoggedParam(2, "setString", "bar");

        assertEquals(param1.toString(), "LoggedParam [index=1, method=setInt, value=1234]");
        assertEquals(param2.toString(), "LoggedParam [index=1, method=setInt, value=1235]");
        assertEquals(param3.toString(), "LoggedParam [index=2, method=setString, value=foo]");
        assertEquals(param4.toString(), "LoggedParam [index=2, method=setString, value=bar]");
    }
}
