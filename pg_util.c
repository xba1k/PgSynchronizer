#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <stdarg.h>
#include <string.h>
#include <arpa/inet.h>

#include "pg_util.h"

PGconn* pg_connect(const char * host, const char *user, const char *pass, const char *dbname, const char *port) {
    
    PGconn *conn = NULL;
    ConnStatusType connStatus;
    
    printf("connecting to %s %s %s %s\n", host, port, user, dbname);
    
    conn = PQsetdbLogin(host, port, NULL, NULL, dbname, user, pass);
            //PQconnectdb("host=localhost port=5432 dbname=newcloudmaster_fresh connect_timeout=10");
    
    if((connStatus = PQstatus(conn)) != CONNECTION_OK) {
        printf("%s\n", PQerrorMessage(conn));
        conn = NULL;
    }
    
    return conn;
    
}

void pg_close(PGconn *conn) {
    
    if(conn) {
    
        PQfinish(conn);
        
    }
}

void pg_begin(PGconn *conn) {
    
    PGresult *res = NULL;
           
    if(conn) {
        
        res = PQexec(conn, "BEGIN");
        
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            
            printf("%s\n", PQerrorMessage(conn));
            PQclear(res);
            
        }
        
    }
    
}

void pg_end(PGconn *conn) {
    
    PGresult *res = NULL;
           
    if(conn) {
        
        res = PQexec(conn, "END");
        
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            
            printf("%s\n", PQerrorMessage(conn));
           
        }
        
        PQclear(res);
        
    }
    
}

void pg_unprepare(PGconn *conn, char *stmt_name) {
    
    PGresult *res = NULL;
    char *query = NULL;
           
    if(conn) {
        
        asprintf(&query, "DEALLOCATE %s", stmt_name);
        
        res = PQexec(conn, query);
        
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            
            printf("%s\n", PQerrorMessage(conn));
           
        }
        
        PQclear(res);
        free(query);
        
    }
    
}

void pg_exec(PGconn *conn, char *query) {
    
    PGresult *res = NULL;
           
    if(conn) {
        
        res = PQexec(conn, query);
        
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            
            printf("%s\n", PQerrorMessage(conn));
           
        }
        
        PQclear(res);
        
    }
    
}

int pg_begin_copy(PGconn *conn, char *tablename, PGconn *src_conn) {
    
    int c;
    char *colnames = NULL;
    char *query_cols = NULL;
    char *tmp = NULL;
    ResultSet *rset = NULL;

    PGresult *res = NULL;
    char *query = NULL;
    int result = 0;
    
    char *header = "PGCOPY\n\377\r\n\0\0\0\0\0\0\0\0\0";
    
    if(conn) {

        asprintf(&query_cols, "SELECT * FROM %s LIMIT 1", tablename);
        rset= pg_query(src_conn, query_cols);

        if(rset) {

            for(c = 0; c < rset->numFields; c++) {
                
                asprintf(&tmp, "%s%s%s", colnames!=NULL ? colnames : "", colnames!=NULL ? "," : "",  rset->colNames[c]);
                free(colnames);
                colnames = tmp;
                tmp = NULL;                
            }            
        }

        
        asprintf(&query, "COPY %s (%s) FROM STDIN WITH BINARY", tablename, colnames);
        free(colnames);

        res = PQexec(conn, query);
        
        if (PQresultStatus(res) != PGRES_COPY_IN) {
            printf("%s\n", PQerrorMessage(conn));
        } else {
            
            if(PQputCopyData(conn, header, 11 + 4 + 4)) {
                result = 1;
            }
        }
        
        PQclear(res);
        free(query);
        
    }
    
    return result;
    
}

int pg_send_copy_data(PGconn *conn, ResultSet *rset) {
    
    int c = 0;
    char *buf = NULL, *p;
    int tupleSize = 0;
    int rowsSent = 0;

    if(conn && rset && rset->data) {
        
        tupleSize = 2 + rset->totalRowLength + rset->numFields * 4;
                
        buf = malloc(tupleSize);
        p = buf;
                
        *((short*)p) = htons((short)rset->numFields);
        p+=2;
                
        for(c = 0; c < rset->numFields; c++) {
                    
            if(!rset->nulls[c]) {
                    
                *((int*)p) = htonl(rset->lengths[c]);
                p+=4;
                memcpy(p, rset->data[c], rset->lengths[c]);
                p+=rset->lengths[c];
                                
            } else {
                        
                *((int*)p) = -1;
                p+=4;
                        
            }
                    
        }
                
        if(PQputCopyData(conn, buf, tupleSize)) {
            rowsSent = 1;
        } 
                
        free(buf);
                
    }
    
    return rowsSent;
    
}

int pg_finish_copy(PGconn *conn) {
    
    PGresult *res = NULL;
    int result = 0;
    int terminator = -1;
    
    if(conn) {
        
//        PQputCopyData(conn, (char*)&terminator, 4);
        
        if(PQputCopyEnd(conn, NULL)) {
            
            res = PQgetResult(conn);
            
            if(PQresultStatus(res) != PGRES_COMMAND_OK) {
                printf("%s\n", PQerrorMessage(conn));
            } else {
                printf("COPY completed with %s rows affected\n", PQcmdTuples(res));
                result = 1;
            }
            
            PQclear(res);
            
            res = PQgetResult(conn);
            
            if(res) {
                
                PQclear(res);
                
            }
            
        }
        
    }
    
    return result;
    
}


ResultSet * pg_query(PGconn *conn, char *query) {
    
    ResultSet *rset = NULL;
    int c;
    
    rset = calloc(1, sizeof(ResultSet));
    
    rset->res = PQexec(conn, query);
    
    if(!rset->res) {
        
        printf("%s\n", PQerrorMessage(conn));
        pg_finish(rset);
        rset = NULL;
        
    } else {
        
        rset->query = query;
        rset->status = PQresultStatus(rset->res);
        
        if(rset->status == PGRES_TUPLES_OK || rset->status == PGRES_COMMAND_OK) {
            
            rset->numFields = PQnfields(rset->res);
            rset->numRows = PQntuples(rset->res);
            
            rset->colNames = calloc(rset->numFields, sizeof(char*));
            
            for(c = 0; c < rset->numFields; c++) {
                
                rset->colNames[c] = PQfname(rset->res, c);
                
            }
            
        } else {
            
            printf("%s\n", PQerrorMessage(conn));
            
        }
        
    }
    
    return rset;
    
}

ResultSet * pg_query_single_mode(PGconn *conn, char *query) {
    
    ResultSet *rset = NULL;
    int c;
    
    rset = calloc(1, sizeof(ResultSet));
    rset->conn = conn;
    
    if(PQsendQuery(conn, query)!=1) {
        
        printf("%s\n", PQerrorMessage(conn));
        pg_finish(rset);
        rset = NULL;
        
    } else if(PQsetSingleRowMode(conn)==1) {
        
        rset->query = query;
        rset->singleRowMode = 1;
        
    } else {

        printf("%s\n", PQerrorMessage(conn));
        pg_finish(rset);
        rset = NULL;

    }
     
    return rset;
    
}

ResultSet * pg_execute_prepared(PGconn *conn, char *stmt_name, char *values[], int lengths[], int formats[], int count) {
    
    ResultSet *rset = NULL;
    int c;
    
    rset = calloc(1, sizeof(ResultSet));
    
    rset->res = PQexecPrepared(conn, stmt_name, count, (const char * const *)values, lengths, formats, 1);
    
    if(!rset->res) {
        
        printf("%s\n", PQerrorMessage(conn));
        pg_finish(rset);
        rset = NULL;
        
    } else {
        
        rset->query = stmt_name;
        rset->status = PQresultStatus(rset->res);
        
        if(rset->status == PGRES_TUPLES_OK || rset->status == PGRES_COMMAND_OK) {
            
            rset->numFields = PQnfields(rset->res);
            
            if(rset->status == PGRES_TUPLES_OK) {
                rset->numRows = PQntuples(rset->res);
            } else {
                rset->numRows = atoi(PQcmdTuples(rset->res));
            }
            
            rset->colNames = calloc(rset->numFields, sizeof(char*));
            rset->formats = calloc(rset->numFields, sizeof(int));
            
            for(c = 0; c < rset->numFields; c++) {
                
                rset->colNames[c] = PQfname(rset->res, c);
                rset->formats[c] = 1;
                
            }            
            
            
        } else {
            
            printf("%s\n", PQerrorMessage(conn));
            
        }
        
    }
    
    return rset;
    
}

char** pg_next(ResultSet *rset) {
    
    int c = 0;
    
    if(rset && rset->singleRowMode) {
        
        if(rset->res)
            PQclear(rset->res);
        
        rset->res = PQgetResult(rset->conn);
        rset->status = PQresultStatus(rset->res);
        
        if(rset->status == PGRES_TUPLES_OK) {
            while((rset->res = PQgetResult(rset->conn))!=NULL);
            return NULL;
        }
        
        if(!rset->colNames && rset->res) {
        
            rset->numFields = PQnfields(rset->res);
            rset->numRows = PQntuples(rset->res);
            
            if(rset->colNames == NULL)
                rset->colNames = calloc(rset->numFields, sizeof(char*));
            
            if(rset->types == NULL)
                rset->types = calloc(rset->numFields, sizeof(Oid));
            
            for(c = 0; c < rset->numFields; c++) {
                rset->colNames[c] = PQfname(rset->res, c);
                rset->types[c] = PQftype(rset->res, c);
            }
        
        }
        
    }    
    
    if(rset && rset->res && ((!rset->singleRowMode && rset->numRows > rset->currentRow) || rset->singleRowMode)) {
        
        if(rset->data == NULL) {
            rset->data = calloc(rset->numFields, sizeof(char*));
        }
        
        if(rset->nulls == NULL) {
            rset->nulls = calloc(rset->numFields, sizeof(char));
        }
        
        if(rset->lengths == NULL) {
            rset->lengths = calloc(rset->numFields, sizeof(int));
        }
        
        rset->totalRowLength = 0;
        
        for(c = 0; c < rset->numFields; c++) {
            
            rset->data[c] = PQgetvalue(rset->res, !rset->singleRowMode ? rset->currentRow : 0, c);
            rset->nulls[c] = PQgetisnull(rset->res, !rset->singleRowMode ? rset->currentRow : 0, c);
            rset->lengths[c] = PQgetlength(rset->res, !rset->singleRowMode ? rset->currentRow : 0, c);
            rset->totalRowLength += rset->lengths[c];
            
            if(rset->nulls[c])
                rset->data[c] = NULL;
            
        }
        
        rset->currentRow++;
        return rset->data;
        
    }
    
    return NULL;
    
}

void pg_reset(ResultSet *rset) {
    
    if(rset) {
        rset->currentRow = 0;
    }
    
}

void pg_finish(ResultSet *rset) {
    
    if(rset) {
        
        PQclear(rset->res);
        
        if(rset->data)
            free(rset->data);
        
        if(rset->nulls)
            free(rset->nulls);
        
        if(rset->lengths)
            free(rset->lengths);
        
        if(rset->query)
            free(rset->query);
        
        if(rset->colNames)
            free(rset->colNames);
        
        if(rset->types)
            free(rset->types);
        
        if(rset->formats)
            free(rset->formats);
        
        free(rset);
        
    }
    
}

void pg_dump_result_set(ResultSet *rset) {

    char **data;    
    int c;
    
    if(rset) {
        
        printf("query: %s\n", rset->query);
        printf("%u cols, %u rows\n", rset->numFields, rset->numRows);
    
        for(c = 0; c < rset->numFields; c++) {
            
            printf("%s ", rset->colNames[c]);
            
        }
        
        printf("\n");
    
        while((data = pg_next(rset))) {
        
            for(c = 0; c < rset->numFields; c++) {
            
                printf("%s ", data[c]);
            
            }
        
            printf("\n");
        
        }
        
    }
    
}
