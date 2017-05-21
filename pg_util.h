#ifndef PG_H
#define PG_H

#ifdef __linux__
#include <postgres/libpq-fe.h>
#elif __APPLE__
#include <libpq-fe.h>
#endif

typedef struct {

    int numFields;
    int numRows;
    int currentRow;

    char *query;
    PGresult *res;
    char **colNames;
    char **data;
    char *nulls;
    int *lengths;
    Oid *types;
    int *formats;
    int totalRowLength;
    
    ExecStatusType status;
    PGconn *conn;
    
    int singleRowMode;

} ResultSet;


PGconn* pg_connect(const char * host, const char *user, const char *pass, const char *dbname, const char *port);
void pg_close(PGconn *conn);
void pg_finish(ResultSet *rset);
ResultSet * pg_query(PGconn *conn, char *query);
ResultSet * pg_query_single_mode(PGconn *conn, char *query);
ResultSet * pg_execute_prepared(PGconn *conn, char *stmt_name, char *values[], int lengths[], int formats[], int count);
char** pg_next(ResultSet *rset);
void pg_begin(PGconn *conn);
void pg_end(PGconn *conn);
void pg_unprepare(PGconn *conn, char *stmt_name);
void pg_exec(PGconn *conn, char *query);
int pg_begin_copy(PGconn *conn, char *tablename);
int pg_send_copy_data(PGconn *conn, ResultSet *rset);
int pg_finish_copy(PGconn *conn);

#endif