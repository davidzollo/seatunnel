# Connector 检查命令使用指南

## Command 命令入口

```shell
bin/seatunnel-connector.sh
```

## 选项

```text
Usage: seatunnel-connector.sh [options]
  Options:
    -h, --help         Show the usage message
    -l, --list         List all supported plugins(sources, sinks, transforms) 
                       (default: false)
    -o, --option-rule  Get option rule of the plugin by the plugin 
                       identifier(connector name or transform name)
    -pt, --plugin-type SeaTunnel plugin type, support [source, sink, 
                       transform] 
```

## 示例

```shell
# 列出所有支持的连接器(sources 和 sinks)及 transforms
bin/seatunnel-connector.sh -l
# 列出所有支持的 sinks
bin/seatunnel-connector.sh -l -pt sink
# 根据名称获取连接器(sources 和 sinks) 或 transform 的选项规则
bin/seatunnel-connector.sh -o Paimon
# 获取 Paimon sink 的选项规则
bin/seatunnel-connector.sh -o Paimon -pt sink
```

