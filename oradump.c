#include <oci.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_COLUMN_NAME_LEN 30
#define MAX_ROW_SIZE 4000

typedef struct {
    OCIEnv* env;
    OCIServer* srv;
    OCISvcCtx* svc;
    OCIError* err;
    OCISession* usr;
} OracleConnection;

typedef struct {
    char name[MAX_COLUMN_NAME_LEN];
    int data_type;
    int data_length;
} ColumnMeta;

// 初始化Oracle环境
OracleConnection* init_oracle_env() {
    OracleConnection* conn = malloc(sizeof(OracleConnection));
    OCIEnvCreate(&conn->env, OCI_THREADED, NULL, NULL, NULL, NULL, 0, NULL);
    OCIHandleAlloc(conn->env, (void**)&conn->err, OCI_HTYPE_ERROR, 0, NULL);
    return conn;
}

// 错误处理函数
void check_error(OCIError* err, sword status, const char* message) {
    text errbuf[512];
    sb4 errcode = 0;
    
    switch (status) {
        case OCI_SUCCESS:
            break;
        case OCI_SUCCESS_WITH_INFO:
            printf("警告: OCI_SUCCESS_WITH_INFO\n");
            break;
        case OCI_NEED_DATA:
            printf("错误: OCI_NEED_DATA\n");
            break;
        case OCI_NO_DATA:
            printf("警告: OCI_NO_DATA\n");
            break;
        case OCI_ERROR:
            OCIErrorGet(err, 1, NULL, &errcode, errbuf, sizeof(errbuf), OCI_HTYPE_ERROR);
            printf("OCI错误 %d: %s\n", errcode, errbuf);
            printf("在执行: %s\n", message);
            break;
        case OCI_INVALID_HANDLE:
            printf("错误: OCI_INVALID_HANDLE\n");
            break;
        case OCI_STILL_EXECUTING:
            printf("错误: OCI_STILL_EXECUTING\n");
            break;
        default:
            printf("未知错误: %d\n", status);
            break;
    }
}

// 建立数据库连接
int connect_db(OracleConnection* conn, const char* user, const char* pass, const char* db) {
    sword status;
    
    // 分配服务器句柄
    status = OCIHandleAlloc(conn->env, (void**)&conn->srv, OCI_HTYPE_SERVER, 0, NULL);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "分配服务器句柄");
        return status;
    }
    
    // 连接到服务器
    status = OCIServerAttach(conn->srv, conn->err, (text*)db, strlen(db), OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "连接到服务器");
        return status;
    }
    
    // 分配服务上下文句柄
    status = OCIHandleAlloc(conn->env, (void**)&conn->svc, OCI_HTYPE_SVCCTX, 0, NULL);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "分配服务上下文句柄");
        return status;
    }
    
    // 设置服务器属性
    status = OCIAttrSet(conn->svc, OCI_HTYPE_SVCCTX, conn->srv, 0, OCI_ATTR_SERVER, conn->err);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "设置服务器属性");
        return status;
    }
    
    // 分配会话句柄
    status = OCIHandleAlloc(conn->env, (void**)&conn->usr, OCI_HTYPE_SESSION, 0, NULL);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "分配会话句柄");
        return status;
    }
    
    // 设置用户名
    status = OCIAttrSet(conn->usr, OCI_HTYPE_SESSION, (void*)user, strlen(user), OCI_ATTR_USERNAME, conn->err);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "设置用户名");
        return status;
    }
    
    // 设置密码
    status = OCIAttrSet(conn->usr, OCI_HTYPE_SESSION, (void*)pass, strlen(pass), OCI_ATTR_PASSWORD, conn->err);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "设置密码");
        return status;
    }
    
    // 开始会话
    status = OCISessionBegin(conn->svc, conn->err, conn->usr, OCI_CRED_RDBMS, OCI_DEFAULT);
    if (status != OCI_SUCCESS && status != OCI_SUCCESS_WITH_INFO) {
        check_error(conn->err, status, "开始会话");
        return status;
    }
    
    // 设置会话属性
    status = OCIAttrSet(conn->svc, OCI_HTYPE_SVCCTX, conn->usr, 0, OCI_ATTR_SESSION, conn->err);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "设置会话属性");
        return status;
    }
    
    printf("成功连接到Oracle数据库\n");
    return OCI_SUCCESS;
}

// 获取表列信息
ColumnMeta* get_table_columns(OracleConnection* conn, const char* table_name, int* col_count) {
    OCIStmt* stmt;
    OCIDefine* def1 = NULL;
    OCIDefine* def2 = NULL;
    OCIDefine* def3 = NULL;
    sword status;
    char query[1024];
    
    // 构建查询语句获取列信息
    snprintf(query, sizeof(query), 
             "SELECT column_name, data_type, data_length "
             "FROM user_tab_columns "
             "WHERE table_name = UPPER('%s') "
             "ORDER BY column_id", 
             table_name);
    
    // 分配语句句柄
    status = OCIHandleAlloc(conn->env, (void**)&stmt, OCI_HTYPE_STMT, 0, NULL);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "分配语句句柄");
        return NULL;
    }
    
    // 准备SQL语句
    status = OCIStmtPrepare(stmt, conn->err, (text*)query, strlen(query), OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "准备SQL语句");
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        return NULL;
    }
    
    // 执行查询
    status = OCIStmtExecute(conn->svc, stmt, conn->err, 0, 0, NULL, NULL, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "执行查询");
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        return NULL;
    }
    
    // 获取列数
    ub4 row_count = 0;
    status = OCIAttrGet(stmt, OCI_HTYPE_STMT, &row_count, 0, OCI_ATTR_ROW_COUNT, conn->err);
    if (status != OCI_SUCCESS || row_count == 0) {
        printf("表 %s 不存在或没有列\n", table_name);
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        return NULL;
    }
    
    *col_count = row_count;
    ColumnMeta* columns = (ColumnMeta*)malloc(sizeof(ColumnMeta) * row_count);
    if (!columns) {
        printf("内存分配失败\n");
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        return NULL;
    }
    
    // 定义输出变量
    char col_name[MAX_COLUMN_NAME_LEN];
    char data_type[30];
    int data_length;
    
    status = OCIDefineByPos(stmt, &def1, conn->err, 1, col_name, sizeof(col_name), SQLT_STR, NULL, NULL, NULL, OCI_DEFAULT);
    status = OCIDefineByPos(stmt, &def2, conn->err, 2, data_type, sizeof(data_type), SQLT_STR, NULL, NULL, NULL, OCI_DEFAULT);
    status = OCIDefineByPos(stmt, &def3, conn->err, 3, &data_length, sizeof(data_length), SQLT_INT, NULL, NULL, NULL, OCI_DEFAULT);
    
    // 获取结果
    int i = 0;
    while (OCIStmtFetch2(stmt, conn->err, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT) == OCI_SUCCESS) {
        strncpy(columns[i].name, col_name, MAX_COLUMN_NAME_LEN);
        columns[i].data_length = data_length;
        
        // 根据数据类型设置OCI数据类型
        if (strcmp(data_type, "VARCHAR2") == 0 || strcmp(data_type, "CHAR") == 0) {
            columns[i].data_type = SQLT_CHR;
        } else if (strcmp(data_type, "NUMBER") == 0) {
            columns[i].data_type = SQLT_NUM;
        } else if (strcmp(data_type, "DATE") == 0) {
            columns[i].data_type = SQLT_DAT;
        } else if (strcmp(data_type, "CLOB") == 0) {
            columns[i].data_type = SQLT_CLOB;
        } else if (strcmp(data_type, "BLOB") == 0) {
            columns[i].data_type = SQLT_BLOB;
        } else {
            // 默认作为字符串处理
            columns[i].data_type = SQLT_CHR;
        }
        
        i++;
    }
    
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    return columns;
}

// 生成CREATE TABLE语句
void generate_create_table(OracleConnection* conn, const char* table_name, FILE* output) {
    OCIStmt* stmt;
    OCIDefine* def1 = NULL;
    OCIDefine* def2 = NULL;
    OCIDefine* def3 = NULL;
    OCIDefine* def4 = NULL;
    OCIDefine* def5 = NULL;
    sword status;
    char query[1024];
    
    // 构建查询语句获取列信息
    snprintf(query, sizeof(query), 
             "SELECT column_name, data_type, data_length, data_precision, data_scale, nullable "
             "FROM user_tab_columns "
             "WHERE table_name = UPPER('%s') "
             "ORDER BY column_id", 
             table_name);
    
    // 分配语句句柄
    status = OCIHandleAlloc(conn->env, (void**)&stmt, OCI_HTYPE_STMT, 0, NULL);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "分配语句句柄");
        return;
    }
    
    // 准备SQL语句
    status = OCIStmtPrepare(stmt, conn->err, (text*)query, strlen(query), OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "准备SQL语句");
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        return;
    }
    
    // 执行查询
    status = OCIStmtExecute(conn->svc, stmt, conn->err, 0, 0, NULL, NULL, OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
        check_error(conn->err, status, "执行查询");
        OCIHandleFree(stmt, OCI_HTYPE_STMT);
        return;
    }
    
    // 定义输出变量
    char col_name[MAX_COLUMN_NAME_LEN];
    char data_type[30];
    int data_length;
    int data_precision;
    int data_scale;
    char nullable[2];
    
    status = OCIDefineByPos(stmt, &def1, conn->err, 1, col_name, sizeof(col_name), SQLT_STR, NULL, NULL, NULL, OCI_DEFAULT);
    status = OCIDefineByPos(stmt, &def2, conn->err, 2, data_type, sizeof(data_type), SQLT_STR, NULL, NULL, NULL, OCI_DEFAULT);
    status = OCIDefineByPos(stmt, &def3, conn->err, 3, &data_length, sizeof(data_length), SQLT_INT, NULL, NULL, NULL, OCI_DEFAULT);
    status = OCIDefineByPos(stmt, &def4, conn->err, 4, &data_precision, sizeof(data_precision), SQLT_INT, NULL, NULL, NULL, OCI_DEFAULT);
    status = OCIDefineByPos(stmt, &def5, conn->err, 5, &data_scale, sizeof(data_scale), SQLT_INT, NULL, NULL, NULL, OCI_DEFAULT);
    status = OCIDefineByPos(stmt, &def5, conn->err, 6, nullable, sizeof(nullable), SQLT_STR, NULL, NULL, NULL, OCI_DEFAULT);
    
    // 开始生成CREATE TABLE语句
    fprintf(output, "CREATE TABLE %s (\n", table_name);
    
    int first_column = 1;
    while (OCIStmtFetch2(stmt, conn->err, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT) == OCI_SUCCESS) {
        if (!first_column) {
            fprintf(output, ",\n");
        }
        
        fprintf(output, "  %s ", col_name);
        
        // 根据数据类型生成列定义
        if (strcmp(data_type, "VARCHAR2") == 0 || strcmp(data_type, "CHAR") == 0) {
            fprintf(output, "%s(%d)", data_type, data_length);
        } else if (strcmp(data_type, "NUMBER") == 0) {
            if (data_precision > 0) {
                if (data_scale > 0) {
                    fprintf(output, "%s(%d,%d)", data_type, data_precision, data_scale);
                } else {
                    fprintf(output, "%s(%d)", data_type, data_precision);
                }
            } else {
                fprintf(output, "%s", data_type);
            }
        } else {
            fprintf(output, "%s", data_type);
        }
        
        // 添加NULL/NOT NULL约束
        if (strcmp(nullable, "N") == 0) {
            fprintf(output, " NOT NULL");
        }
        
        first_column = 0;
    }
    
    // 获取主键信息
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    status = OCIHandleAlloc(conn->env, (void**)&stmt, OCI_HTYPE_STMT, 0, NULL);
    
    snprintf(query, sizeof(query), 
             "SELECT cols.column_name "
             "FROM user_constraints cons, user_cons_columns cols "
             "WHERE cons.constraint_name = cols.constraint_name "
             "AND cons.constraint_type = 'P' "
             "AND cons.table_name = UPPER('%s') "
             "ORDER BY cols.position", 
             table_name);
    
    status = OCIStmtPrepare(stmt, conn->err, (text*)query, strlen(query), OCI_NTV_SYNTAX, OCI_DEFAULT);
    status = OCIStmtExecute(conn->svc, stmt, conn->err, 0, 0, NULL, NULL, OCI_DEFAULT);
    
    // 定义输出变量
    status = OCIDefineByPos(stmt, &def1, conn->err, 1, col_name, sizeof(col_name), SQLT_STR, NULL, NULL, NULL, OCI_DEFAULT);
    
    // 收集主键列
    char pk_columns[1024] = "";
    int first_pk = 1;
    
    while (OCIStmtFetch2(stmt, conn->err, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT) == OCI_SUCCESS) {
        if (!first_pk) {
            strcat(pk_columns, ", ");
        }
        strcat(pk_columns, col_name);
        first_pk = 0;
    }
    
    // 添加主键约束
    if (strlen(pk_columns) > 0) {
        fprintf(output, ",\n  PRIMARY KEY (%s)", pk_columns);
    }
    
    fprintf(output, "\n);\n\n");
    
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
}

// 转义单引号函数
void escape_quotes(const char* input, char* output) {
    while(*input) {
        if(*input == '\'') *output++ = '\'';
        *output++ = *input++;
    }
    *output = '\0';
}

// LOB字段转义处理
void escape_lob(const char* data, ub4 length, FILE* output) {
    for(ub4 i = 0; i < length; i++) {
        if(data[i] == '\'') fputc('\'', output);
        fputc(data[i], output);
    }
}

// BLOB转十六进制字符串
void blob_to_hex(const unsigned char* data, ub4 length, FILE* output) {
    fprintf(output, "HEXTORAW('");
    for(ub4 i = 0; i < length; i++) {
        fprintf(output, "%02X", data[i]);
    }
    fprintf(output, "')");
}

void generate_insert_statements(OracleConnection* conn, const char* table_name, FILE* output) {
    ColumnMeta* columns;
    int col_count;
    columns = get_table_columns(conn, table_name, &col_count);
    
    // 构建SELECT语句
    char select_sql[1024];
    snprintf(select_sql, sizeof(select_sql), "SELECT * FROM %s", table_name);
    
    // 执行查询
    OCIStmt* stmt;
    OCIHandleAlloc(conn->env, (void**)&stmt, OCI_HTYPE_STMT, 0, NULL);
    OCIStmtPrepare(stmt, conn->err, (text*)select_sql, strlen(select_sql), OCI_NTV_SYNAPSE, OCI_DEFAULT);
    OCIStmtExecute(conn->svc, stmt, conn->err, 0, 0, NULL, NULL, OCI_DEFAULT);
    
    // 处理结果集
    while (OCIStmtFetch2(stmt, conn->err, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT) == OCI_SUCCESS) {
        // 构建INSERT语句
        fprintf(output, "INSERT INTO %s VALUES (", table_name);
        for (int i = 0; i < col_count; i++) {
            // 根据列类型处理数据格式
            char buffer[MAX_ROW_SIZE];
            sb2 indicator;
            sb4 data_type = columns[i].data_type;
            
            // 定义列缓冲区
            OCIDefine* def;
            OCIDefineByPos(stmt, &def, conn->err, i+1, buffer, 
                         MAX_ROW_SIZE, SQLT_STR, &indicator, 0, 0, OCI_DEFAULT);
            
            // 获取实际数据
            OCIAttrGet(def, OCI_HTYPE_DEFINE, &data_type, 0, 
                      OCI_ATTR_DATA_TYPE, conn->err);
            
            // 检查NULL值
            if (indicator == -1) {
                fprintf(output, "NULL");
            } else {
                // 处理不同数据类型
                switch(data_type) {
                    case SQLT_CHR:  // VARCHAR2, CHAR等
                    case SQLT_AFC:  // CHAR
                        // 转义单引号
                        char* escaped = malloc(strlen(buffer)*2 + 1);
                        escape_quotes(buffer, escaped);
                        fprintf(output, "'%s'", escaped);
                        free(escaped);
                        break;
                        
                    case SQLT_DAT:  // DATE
                        // 转换为标准日期格式
                        OCIDate* oci_date = (OCIDate*)buffer;
                        char date_str[20];
                        OCIDateToText(oci_date, "YYYY-MM-DD HH24:MI:SS", 
                                    strlen("YYYY-MM-DD HH24:MI:SS"), 0, 0, 0, 
                                    &date_str, sizeof(date_str), conn->err);
                        fprintf(output, "TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')", date_str);
                        break;
                        
                    case SQLT_NUM:  // NUMBER
                        // 直接输出数字
                        fprintf(output, "%s", buffer);
                        break;
                        
                    case SQLT_CLOB: // CLOB处理
                        OCILobLocator* clob = (OCILobLocator*)buffer;
                        ub4 clob_length;
                        OCILobGetLength(conn->svc, conn->err, clob, &clob_length);
                        char* clob_data = malloc(clob_length + 1);
                        OCILobRead(conn->svc, conn->err, clob, &clob_length, 1,
                                  clob_data, clob_length, 0, 0, 0, SQLCS_IMPLICIT);
                        fprintf(output, "'");
                        escape_lob(clob_data, clob_length, output);
                        fprintf(output, "'");
                        free(clob_data);
                        break;
                        
                    case SQLT_BLOB: // BLOB处理
                        OCILobLocator* blob = (OCILobLocator*)buffer;
                        ub4 blob_length;
                        OCILobGetLength(conn->svc, conn->err, blob, &blob_length);
                        unsigned char* blob_data = malloc(blob_length);
                        OCILobRead(conn->svc, conn->err, blob, &blob_length, 1,
                                  blob_data, blob_length, 0, 0, 0, SQLCS_IMPLICIT);
                        blob_to_hex(blob_data, blob_length, output);
                        free(blob_data);
                        break;
                        
                    default:
                        fprintf(stderr, "不支持的数据类型: %d\n", data_type);
                        fprintf(output, "NULL");
                }
            }

            if(i < col_count - 1) fprintf(output, ", ");
        }
        fprintf(output, ");\n");
    }
    
    // 清理资源
    OCIHandleFree(stmt, OCI_HTYPE_STMT);
    free(columns);
}

// 释放连接资源
void close_connection(OracleConnection* conn) {
    if (conn) {
        if (conn->svc && conn->err) {
            OCISessionEnd(conn->svc, conn->err, conn->usr, OCI_DEFAULT);
        }
        
        if (conn->srv && conn->err) {
            OCIServerDetach(conn->srv, conn->err, OCI_DEFAULT);
        }
        
        if (conn->usr) OCIHandleFree(conn->usr, OCI_HTYPE_SESSION);
        if (conn->svc) OCIHandleFree(conn->svc, OCI_HTYPE_SVCCTX);
        if (conn->srv) OCIHandleFree(conn->srv, OCI_HTYPE_SERVER);
        if (conn->err) OCIHandleFree(conn->err, OCI_HTYPE_ERROR);
        if (conn->env) OCIHandleFree(conn->env, OCI_HTYPE_ENV);
        
        free(conn);
    }
}

void print_usage() {
    printf("用法: oradump [选项]\n");
    printf("选项:\n");
    printf("  -u, --user <用户名>       Oracle用户名\n");
    printf("  -p, --password <密码>     Oracle密码\n");
    printf("  -d, --database <数据库>   Oracle连接字符串\n");
    printf("  -t, --table <表名>        要导出的表名\n");
    printf("  -o, --output <文件名>     输出文件名 (默认: stdout)\n");
    printf("  -c, --create-table        包含CREATE TABLE语句\n");
    printf("  -h, --help                显示此帮助信息\n");
}

int main(int argc, char** argv) {
    char* username = NULL;
    char* password = NULL;
    char* database = NULL;
    char* table_name = NULL;
    char* output_file = NULL;
    int create_table = 0;
    FILE* output = stdout;
    
    // 解析命令行参数
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-u") == 0 || strcmp(argv[i], "--user") == 0) {
            if (i + 1 < argc) {
                username = argv[++i];
            } else {
                printf("错误: 缺少用户名参数\n");
                print_usage();
                return 1;
            }
        } else if (strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--password") == 0) {
            if (i + 1 < argc) {
                password = argv[++i];
            } else {
                printf("错误: 缺少密码参数\n");
                print_usage();
                return 1;
            }
        } else if (strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--database") == 0) {
            if (i + 1 < argc) {
                database = argv[++i];
            } else {
                printf("错误: 缺少数据库参数\n");
                print_usage();
                return 1;
            }
        } else if (strcmp(argv[i], "-t") == 0 || strcmp(argv[i], "--table") == 0) {
            if (i + 1 < argc) {
                table_name = argv[++i];
            } else {
                printf("错误: 缺少表名参数\n");
                print_usage();
                return 1;
            }
        } else if (strcmp(argv[i], "-o") == 0 || strcmp(argv[i], "--output") == 0) {
            if (i + 1 < argc) {
                output_file = argv[++i];
            } else {
                printf("错误: 缺少输出文件参数\n");
                print_usage();
                return 1;
            }
        } else if (strcmp(argv[i], "-c") == 0 || strcmp(argv[i], "--create-table") == 0) {
            create_table = 1;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage();
            return 0;
        } else {
            printf("错误: 未知选项 %s\n", argv[i]);
            print_usage();
            return 1;
        }
    }
    
    // 检查必要参数
    if (!username || !password || !database || !table_name) {
        printf("错误: 缺少必要参数\n");
        print_usage();
        return 1;
    }
    
    // 打开输出文件
    if (output_file) {
        output = fopen(output_file, "w");
        if (!output) {
            printf("错误: 无法打开输出文件 %s\n", output_file);
            return 1;
        }
    }
    
    // 初始化Oracle环境
    OracleConnection* conn = init_oracle_env();
    if (!conn) {
        printf("错误: 初始化Oracle环境失败\n");
        if (output != stdout) fclose(output);
        return 1;
    }
    
    // 连接到数据库
    if (connect_db(conn, username, password, database) != OCI_SUCCESS) {
        printf("错误: 连接到数据库失败\n");
        if (output != stdout) fclose(output);
        close_connection(conn);
        return 1;
    }
    
    // 输出文件头
    fprintf(output, "-- Oracle数据库导出\n");
    fprintf(output, "-- 表: %s\n", table_name);
    fprintf(output, "-- 导出时间: %s\n\n", __DATE__);
    
    // 生成CREATE TABLE语句
    if (create_table) {
        fprintf(output, "-- 表结构\n");
        generate_create_table(conn, table_name, output);
    }
    
    // 生成INSERT语句
    fprintf(output, "-- 表数据\n");
    fprintf(output, "SET DEFINE OFF;\n\n");
    generate_insert_statements(conn, table_name, output);
    fprintf(output, "\nCOMMIT;\n");
    
    // 清理资源
    close_connection(conn);
    if (output != stdout) fclose(output);
    
    printf("导出完成\n");
    return 0;
}
