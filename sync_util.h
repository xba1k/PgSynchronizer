#ifndef SYNC_H
#define SYNC_H

#include "pg_util.h"

void synchronize_table_csum_with_pk_or_ctid(PGconn *src_conn, PGconn *dst_conn, char *tablename);
void synchronize_table_max(PGconn *src_conn, PGconn *dst_conn, char *tablename);
char *determine_pk(PGconn *conn, char *tablename);
void perform_changes_with_pk(PGconn *src_conn, PGconn *dst_conn, char *src_table, char *dst_table, char *pkcol, list_t *inserts, list_t *updates, list_t *deletes);
void compare_tables_strategy_csum_with_pk(ResultSet *src, ResultSet *dst, list_t *inserts, list_t *updates, list_t *deletes);
void add_pk_to_list(list_t *list, long pk_val);
PGresult *prepare_select_with_pk(PGconn *conn, char *stmt_name, char *tablename, char *pkcol);
PGresult *prepare_delete_with_pk(PGconn *conn, char *stmt_name, char *tablename, char *pkcol);
char *generate_delete_dml(char *tablename, char *pkcol, char *pkval);
PGresult *prepare_insert(PGconn *conn, char *stmt_name, ResultSet *rset, char *tablename);
PGresult *prepare_update_with_pk(PGconn *conn, char *stmt_name, ResultSet *rset, char *tablename);
char *generate_update_dml(ResultSet *rset, char *tablename);
char *generate_insert_dml(ResultSet *rset, char *tablename);
ResultSet *checksum_table_data_with_pk(PGconn *conn, char *tablename, char *pkcol);
void disable_constraints(PGconn *conn, char *tablename);
void enable_constraints(PGconn *conn, char *tablename);

#endif