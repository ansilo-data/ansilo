package com.ansilo.connectors.data;

import static org.mockito.Mockito.mock;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.jupiter.api.BeforeEach;

public abstract class DataTypeTest {
    protected ResultSet resultSet;
    protected ByteBuffer byteBuffer;
    protected PreparedStatement preparedStatement;

    @BeforeEach
    void setUp() {
        this.resultSet = mock(ResultSet.class);
        this.byteBuffer = mock(ByteBuffer.class);
        this.preparedStatement = mock(PreparedStatement.class);
    }
}
