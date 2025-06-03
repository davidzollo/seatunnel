package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestMapperIT extends TestSuiteBase {

    @TestTemplate
    public void testMapperTransform(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult mappers = container.executeJob("/mapper/mapper_sample.conf");
        Assertions.assertEquals(0, mappers.getExitCode());
    }
}
