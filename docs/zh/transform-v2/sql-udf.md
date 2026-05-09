# SQL UDF

> SQL 转换插件的 UDF 能力

## 描述

通过 UDF SPI 扩展 SQL 转换函数库。

## UDF API

```java
package org.apache.seatunnel.transform.sql.zeta;

public interface ZetaUDF {
    /**
     * Function name
     *
     * @return function name
     */
    String functionName();

    /**
     * The type of function result
     *
     * @param argsType input arguments type
     * @return result type
     */
    SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType);

    /**
     * Evaluate
     *
     * @param args input arguments
     * @return result value
     */
    Object evaluate(List<Object> args);

    /**
     * Whether current udf requires row level context.
     */
    default boolean requiresContext() {
        return false;
    }

    /**
     * Evaluate with row level context.
     */
    default Object evaluateWithContext(List<Object> args, ZetaUDFContext context) {
        return evaluate(args);
    }

    /**
     * Initialize udf resources.
     */
    default void open() throws Exception {}

    /**
     * Release udf resources.
     */
    default void close() {}
}
```

`ZetaUDFContext` 提供了运行时行级上下文信息，包含以下字段：

- `getRawTableId()`
- `getDatabase()`
- `getSchema()`
- `getTable()`
- `getRowKind()`
- `getAllFields()`

说明：

- `database/schema/table` 的解析遵循 `TablePath.of(tableId)` 语义。
- 当 `tableId` 格式不受支持时，访问 `database/schema/table` 会抛出 `IllegalArgumentException`。
- 已有 UDF 保持向后兼容，仍可继续使用 `evaluate(List<Object> args)`。

## UDF 实现示例

在你的 Maven 项目中添加以下依赖并使用 `provided` 作用域：

```xml

<dependencies>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-transforms-v2</artifactId>
        <version>2.3.2</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.seatunnel</groupId>
        <artifactId>seatunnel-api</artifactId>
        <version>2.3.2</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>com.google.auto.service</groupId>
        <artifactId>auto-service</artifactId>
        <version>1.0.1</version>
        <scope>provided</scope>
    </dependency>
</dependencies>

```

新增一个 Java 类并实现 `ZetaUDF`，示例如下：

```java

@AutoService(ZetaUDF.class)
public class ExampleUDF implements ZetaUDF {
    @Override
    public String functionName() {
        return "EXAMPLE";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) return null;
        return "UDF: " + arg;
    }
}
```

打包 UDF 项目后，将 jar 复制到 `${SEATUNNEL_HOME}/lib`。

## 上下文感知与生命周期 UDF 示例

```java
@AutoService(ZetaUDF.class)
public class ContextLifecycleUdf implements ZetaUDF {

    private transient String prefix;

    @Override
    public String functionName() {
        return "CTX_LIFE";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public boolean requiresContext() {
        return true;
    }

    @Override
    public void open() {
        this.prefix = "OPENED";
    }

    @Override
    public Object evaluateWithContext(List<Object> args, ZetaUDFContext context) {
        String arg = args.get(0) == null ? null : String.valueOf(args.get(0));
        if (arg == null) {
            return null;
        }
        return prefix + ":" + context.getRowKind().shortString() + ":" + arg;
    }

    @Override
    public void close() {
        this.prefix = null;
    }
}
```

## 示例

Source 读取到的数据表如下：

| id |   name   | age |
|----|----------|-----|
| 1  | Joy Ding | 20  |
| 2  | May Ding | 21  |
| 3  | Kin Dom  | 24  |
| 4  | Joy Dom  | 22  |

使用 SQL UDF 转换：

```
transform {
  Sql {
    source_table_name = "fake"
    result_table_name = "fake1"
    query = "select id, example(name) as name, age from fake"
  }
}
```

结果表 `fake1` 将变为：

| id |     name      | age |
|----|---------------|-----|
| 1  | UDF: Joy Ding | 20  |
| 2  | UDF: May Ding | 21  |
| 3  | UDF: Kin Dom  | 24  |
| 4  | UDF: Joy Dom  | 22  |

## Changelog

### new version

- Add UDF of SQL Transform Connector
