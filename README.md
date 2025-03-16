# OCIULDR - Oracle数据导出工具

## 项目简介
基于OCI接口开发的Oracle数据库导出工具，支持：
- 生成SQL*Loader控制文件
- 多种数据格式导出
- 大对象(LOB)字段处理
- 会话级性能参数调整
- 分批次输出数据文件

## 编译要求
- Oracle OCI开发库
- GCC编译器
- Linux/Windows环境

## 使用说明

### 基本语法
```bash
ociuldr user=username/password@tnsname query="SELECT语句" [参数]
```

### 主要参数
| 参数        | 说明                                                                 |
|-------------|--------------------------------------------------------------------|
| file        | 输出文件名（默认：uldrdata.txt）支持%Y等时间格式符                   |
| field       | 字段分隔符（支持0x十六进制格式，如0x7c表示\|）                      |
| record      | 记录分隔符（默认\n）                                               |
| table       | 生成控制文件时的表名                                               |
| mode        | SQL*Loader加载模式（INSERT/APPEND/REPLACE/TRUNCATE）              |
| array       | 批量获取行数（5-2000，默认1000）                                   |
| long        | LONG字段最大长度（100-32767，默认32768）                          |
| buffer      | SQL*Loader缓冲区大小（单位MB，默认16）                            |
| batch       | 文件分割批次（每N个批次生成新文件）                                |
| log         | 日志文件                                                          |

### 示例
```bash
# 基本导出
ociuldr user=scott/tiger@orcl query="SELECT * FROM emp" file=emp.dat

# 使用特殊分隔符
ociuldr user=scott/tiger query="SELECT * FROM dept" field=0x7c record=0x0d0a

# 生成控制文件
ociuldr user=scott/tiger table=EMP mode=replace file=emp%Y%m%d.dat
```

## 功能特性
1. **数据类型支持**：
   - 日期/时间类型（自动格式转换）
   - BLOB/CLOB大对象（自动生成LOB文件）
   - LONG RAW等二进制类型

2. **性能优化**：
   ```bash
   sort=512    # 设置排序区为512MB
   hash=256    # 设置哈希区为256MB
   read=128    # 设置多块读参数
   ```

3. **输出控制**：
   - 支持文件自动分片（batch参数）
   - 头部列名输出（head=ON）
   - 自定义记录格式

## 注意事项
1. 查询语句需用双引号包裹，避免特殊字符解析问题
2. LOB字段会生成额外文件，需保持目录可写
3. 十六进制格式需使用0x前缀（如0x0d0a表示换行）
4. 建议对大表导出时设置合理会话参数提升性能
