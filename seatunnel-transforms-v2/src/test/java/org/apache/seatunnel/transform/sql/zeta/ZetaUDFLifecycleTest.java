/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZetaUDFLifecycleTest {
    private static final SeaTunnelRowType INPUT_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name"},
                    new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

    @Test
    public void testContextAwareUdfUsesLifecycleHooks() {
        ContextAwareLifecycleUdf udf = new ContextAwareLifecycleUdf();
        TestingZetaSQLEngine sqlEngine = new TestingZetaSQLEngine(Collections.singletonList(udf));
        sqlEngine.init("test", null, INPUT_ROW_TYPE, "select CTX_INFO(name) as value from test");
        Assertions.assertEquals(0, udf.openCount);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "hello"});
        row.setTableId("db.schema.users");
        row.setRowKind(RowKind.UPDATE_AFTER);

        SeaTunnelRow result = sqlEngine.transformBySQL(row);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, udf.openCount);
        Assertions.assertEquals(1, udf.evaluateWithContextCount);
        Assertions.assertEquals("OPENED|+U|db|schema|users|2|hello", result.getField(0));

        sqlEngine.close();

        Assertions.assertEquals(1, udf.closeCount);
    }

    @Test
    public void testCloseOpenedUdfWhenLaterUdfOpenFails() {
        CloseTrackingUdf firstUdf = new CloseTrackingUdf("FIRST");
        FailingOpenUdf secondUdf = new FailingOpenUdf();
        TestingZetaSQLEngine sqlEngine =
                new TestingZetaSQLEngine(Arrays.asList(firstUdf, secondUdf));

        sqlEngine.init("test", null, INPUT_ROW_TYPE, "select id from test");

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                sqlEngine.transformBySQL(
                                        new SeaTunnelRow(new Object[] {1, "hello"})));

        Assertions.assertTrue(exception.getMessage().contains("Open udf FAIL_OPEN failed"));
        Assertions.assertEquals(1, firstUdf.openCount);
        Assertions.assertEquals(1, firstUdf.closeCount);
        Assertions.assertEquals(1, secondUdf.openCount);
        Assertions.assertEquals(1, secondUdf.closeCount);
    }

    @Test
    public void testContextWithNullTableIdShouldNotFail() {
        ZetaUDFContext context = new ZetaUDFContext();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "hello"});
        row.setTableId(null);

        context.update(row);

        Assertions.assertNull(context.getRawTableId());
        Assertions.assertNull(context.getDatabase());
        Assertions.assertNull(context.getSchema());
        Assertions.assertNull(context.getTable());
    }

    @Test
    public void testLoadUdfFromFallbackClassLoader() throws Exception {
        Path serviceRoot = Files.createTempDirectory("zeta-udf-service");
        Path serviceFile = serviceRoot.resolve("META-INF/services/" + ZetaUDF.class.getName());
        Files.createDirectories(serviceFile.getParent());
        Files.write(
                serviceFile,
                Collections.singletonList(FallbackClassLoaderUdf.class.getName()),
                StandardCharsets.UTF_8);

        try (URLClassLoader fallbackClassLoader =
                new URLClassLoader(
                        new URL[] {serviceRoot.toUri().toURL()},
                        FallbackClassLoaderUdf.class.getClassLoader())) {
            FallbackClassLoaderTestingZetaSQLEngine sqlEngine =
                    new FallbackClassLoaderTestingZetaSQLEngine(
                            Arrays.asList(
                                    Thread.currentThread().getContextClassLoader(),
                                    fallbackClassLoader));

            List<ZetaUDF> loadedUdfs = sqlEngine.loadAvailableUdfs();

            Assertions.assertTrue(
                    loadedUdfs.stream()
                            .map(udf -> udf.getClass().getName())
                            .anyMatch(FallbackClassLoaderUdf.class.getName()::equals));
        } finally {
            deleteRecursively(serviceRoot);
        }
    }

    @Test
    public void testLoadUdfFromExternalJar() throws Exception {
        Path workDir = Files.createTempDirectory("zeta-udf-jar");
        Path sourceRoot = Files.createDirectories(workDir.resolve("src/main/java/com/acme/udf"));
        Path classesRoot = Files.createDirectories(workDir.resolve("classes"));
        Path serviceFile = classesRoot.resolve("META-INF/services/" + ZetaUDF.class.getName());
        Path jarPath = workDir.resolve("custom-zeta-udf.jar");

        try {
            Files.write(
                    sourceRoot.resolve("ExternalJarUdf.java"),
                    Collections.singletonList(
                            String.join(
                                    "\n",
                                    "package com.acme.udf;",
                                    "",
                                    "import org.apache.seatunnel.api.table.type.BasicType;",
                                    "import org.apache.seatunnel.api.table.type.SeaTunnelDataType;",
                                    "import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;",
                                    "",
                                    "import java.util.List;",
                                    "",
                                    "public class ExternalJarUdf implements ZetaUDF {",
                                    "    @Override",
                                    "    public String functionName() {",
                                    "        return \"EXT_JAR\";",
                                    "    }",
                                    "",
                                    "    @Override",
                                    "    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {",
                                    "        return BasicType.STRING_TYPE;",
                                    "    }",
                                    "",
                                    "    @Override",
                                    "    public Object evaluate(List<Object> args) {",
                                    "        return args.get(0);",
                                    "    }",
                                    "}")),
                    StandardCharsets.UTF_8);

            compileJavaSources(sourceRoot, classesRoot);
            Files.createDirectories(serviceFile.getParent());
            Files.write(
                    serviceFile,
                    Collections.singletonList("com.acme.udf.ExternalJarUdf"),
                    StandardCharsets.UTF_8);
            packageJar(classesRoot, jarPath);

            try (URLClassLoader externalClassLoader =
                    new URLClassLoader(
                            new URL[] {jarPath.toUri().toURL()}, ZetaUDF.class.getClassLoader())) {
                FallbackClassLoaderTestingZetaSQLEngine sqlEngine =
                        new FallbackClassLoaderTestingZetaSQLEngine(
                                Collections.singletonList(externalClassLoader));

                List<ZetaUDF> loadedUdfs = sqlEngine.loadAvailableUdfs();

                Assertions.assertTrue(
                        loadedUdfs.stream().map(ZetaUDF::functionName).anyMatch("EXT_JAR"::equals));
            }
        } finally {
            deleteRecursively(workDir);
        }
    }

    private static final class TestingZetaSQLEngine extends ZetaSQLEngine {
        private final List<ZetaUDF> udfList;

        private TestingZetaSQLEngine(List<ZetaUDF> udfList) {
            this.udfList = udfList;
        }

        @Override
        protected List<ZetaUDF> loadUDFs() {
            return udfList;
        }
    }

    private static final class FallbackClassLoaderTestingZetaSQLEngine extends ZetaSQLEngine {
        private final List<ClassLoader> classLoaders;

        private FallbackClassLoaderTestingZetaSQLEngine(List<ClassLoader> classLoaders) {
            this.classLoaders = classLoaders;
        }

        @Override
        protected List<ClassLoader> getUDFClassLoaders() {
            return classLoaders;
        }

        private List<ZetaUDF> loadAvailableUdfs() {
            return loadUDFs();
        }
    }

    private static final class ContextAwareLifecycleUdf implements ZetaUDF {
        private transient String prefix;
        private int openCount;
        private int closeCount;
        private int evaluateWithContextCount;

        @Override
        public String functionName() {
            return "CTX_INFO";
        }

        @Override
        public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
            return BasicType.STRING_TYPE;
        }

        @Override
        public Object evaluate(List<Object> args) {
            throw new AssertionError("evaluate should not be called when context is required");
        }

        @Override
        public boolean requiresContext() {
            return true;
        }

        @Override
        public void open() {
            openCount++;
            prefix = "OPENED";
        }

        @Override
        public Object evaluateWithContext(List<Object> args, ZetaUDFContext context) {
            evaluateWithContextCount++;
            return String.format(
                    "%s|%s|%s|%s|%s|%d|%s",
                    prefix,
                    context.getRowKind().shortString(),
                    context.getDatabase(),
                    context.getSchema(),
                    context.getTable(),
                    context.getAllFields().length,
                    args.get(0));
        }

        @Override
        public void close() {
            closeCount++;
            prefix = null;
        }
    }

    private static final class CloseTrackingUdf implements ZetaUDF {
        private final String functionName;
        private int openCount;
        private int closeCount;

        private CloseTrackingUdf(String functionName) {
            this.functionName = functionName;
        }

        @Override
        public String functionName() {
            return functionName;
        }

        @Override
        public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
            return BasicType.STRING_TYPE;
        }

        @Override
        public Object evaluate(List<Object> args) {
            return null;
        }

        @Override
        public void open() {
            openCount++;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    private static final class FailingOpenUdf implements ZetaUDF {
        private int openCount;
        private int closeCount;

        @Override
        public String functionName() {
            return "FAIL_OPEN";
        }

        @Override
        public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
            return BasicType.STRING_TYPE;
        }

        @Override
        public Object evaluate(List<Object> args) {
            return null;
        }

        @Override
        public void open() {
            openCount++;
            throw new IllegalStateException("open failure");
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    public static final class FallbackClassLoaderUdf implements ZetaUDF {
        @Override
        public String functionName() {
            return "FALLBACK_LOADER";
        }

        @Override
        public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
            return BasicType.STRING_TYPE;
        }

        @Override
        public Object evaluate(List<Object> args) {
            return null;
        }
    }

    private void compileJavaSources(Path sourceRoot, Path classesRoot) throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        Assertions.assertNotNull(compiler);

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8)) {
            List<Path> sourceFiles;
            try (Stream<Path> stream = Files.walk(sourceRoot)) {
                sourceFiles =
                        stream.filter(path -> path.toString().endsWith(".java"))
                                .sorted()
                                .collect(Collectors.toList());
            }

            Iterable<? extends JavaFileObject> compilationUnits =
                    fileManager.getJavaFileObjectsFromFiles(
                            sourceFiles.stream().map(Path::toFile).collect(Collectors.toList()));
            List<String> options =
                    Arrays.asList(
                            "-source",
                            "1.8",
                            "-target",
                            "1.8",
                            "-classpath",
                            buildCompileClassPath(),
                            "-d",
                            classesRoot.toString());
            boolean success =
                    Boolean.TRUE.equals(
                            compiler.getTask(
                                            null,
                                            fileManager,
                                            diagnostics,
                                            options,
                                            null,
                                            compilationUnits)
                                    .call());
            if (!success) {
                throw new IllegalStateException(buildCompilationFailureMessage(diagnostics));
            }
        }
    }

    private String buildCompileClassPath() {
        String javaClassPath = System.getProperty("java.class.path");
        List<String> entries = new ArrayList<>();
        if (javaClassPath != null && !javaClassPath.isEmpty()) {
            entries.addAll(Arrays.asList(javaClassPath.split(File.pathSeparator)));
        }
        return entries.stream()
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.joining(File.pathSeparator));
    }

    private String buildCompilationFailureMessage(DiagnosticCollector<JavaFileObject> diagnostics) {
        StringBuilder builder = new StringBuilder("Compile external UDF jar failed:");
        for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
            builder.append(System.lineSeparator())
                    .append(diagnostic.getKind())
                    .append(" ")
                    .append(
                            diagnostic.getSource() == null
                                    ? "<unknown>"
                                    : diagnostic.getSource().getName())
                    .append(":")
                    .append(diagnostic.getLineNumber())
                    .append(" ")
                    .append(diagnostic.getMessage(Locale.ROOT));
        }
        return builder.toString();
    }

    private void packageJar(Path classesRoot, Path jarPath) throws Exception {
        try (JarOutputStream jarOutputStream =
                new JarOutputStream(Files.newOutputStream(jarPath))) {
            List<Path> files;
            try (Stream<Path> stream = Files.walk(classesRoot)) {
                files = stream.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
            }

            for (Path file : files) {
                JarEntry jarEntry =
                        new JarEntry(
                                classesRoot
                                        .relativize(file)
                                        .toString()
                                        .replace(File.separatorChar, '/'));
                jarOutputStream.putNextEntry(jarEntry);
                Files.copy(file, jarOutputStream);
                jarOutputStream.closeEntry();
            }
        }
    }

    private void deleteRecursively(Path root) throws Exception {
        try (Stream<Path> stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder()).forEach(this::deleteIfExists);
        }
    }

    private void deleteIfExists(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
