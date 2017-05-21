#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include "list.h"
#include "pg_util.h"

#include "sync_util.h"

ResultSet *get_pk_max(PGconn *conn, char *tablename, char *pkcol) {

    char *query = NULL;
    
    if(asprintf(&query, "select max(%s) from %s", pkcol, tablename)) {
        return pg_query(conn, query);
    }
    
    return NULL;
 
}

void disable_constraints(PGconn *conn, char *tablename) {

    char *query = NULL;
    
    if(asprintf(&query, "alter table %s disable trigger all", tablename)) {
        pg_exec(conn, query);
    }
    
    free(query);
    
}

void enable_constraints(PGconn *conn, char *tablename) {

    char *query = NULL;
    
    if(asprintf(&query, "alter table %s enable trigger all", tablename)) {
        pg_exec(conn, query);
    }
    
    free(query);
    
}

ResultSet *checksum_table_data_with_pk(PGconn *conn, char *tablename, char *pkcol) {

    char *query = NULL;
    
    if(asprintf(&query, "select %s, md5((%s.*)::text) from %s order by %s", pkcol, tablename, tablename, pkcol)) {
        return pg_query_single_mode(conn, query);
    }
    
    return NULL;
 
}

ResultSet *checksum_table_data_with_ctid(PGconn *conn, char *tablename) {

    char *query = NULL;
    
    if(asprintf(&query, "select md5((%s.*)::text), ctid::text::point from %s order by md5", tablename, tablename)) {
        return pg_query_single_mode(conn, query);
    }
    
    return NULL;
 
}

char *generate_insert_dml(ResultSet *rset, char *tablename) {
    
    char *template = "INSERT INTO %s(%s) VALUES (%s)";
    int c;
    char *colnames = NULL;
    char *colvals = NULL;
    char *tmp = NULL;
    char *result = NULL;
    
    if(rset) {
        
        for(c = 0; c < rset->numFields; c++) {
            
            asprintf(&tmp, "%s%s%s", colnames!=NULL ? colnames : "", colnames!=NULL ? "," : "",  rset->colNames[c]);
            free(colnames);
            colnames = tmp;
            tmp = NULL;
            
            
            if(rset->nulls[c]) {
                asprintf(&tmp, "%s%sNULL", colvals!=NULL ? colvals : "", colvals!=NULL ? "," : "");
            } else {
                asprintf(&tmp, "%s%s'%s'", colvals!=NULL ? colvals : "", colvals!=NULL ? "," : "",  rset->data[c]);
            }
            
            
            free(colvals);
            colvals = tmp;
            tmp = NULL;
            
        }
        
        asprintf(&result, template, tablename, colnames, colvals);
        
        free(colnames);
        free(colvals);
        
    }
    
    return result;
    
}

char *generate_update_dml(ResultSet *rset, char *tablename) {
    
    char *template = "UPDATE %s SET %s WHERE %s = '%s'";
    int c;
    char *setvals = NULL;
    char *tmp = NULL;
    char *result = NULL;
    
    if(rset) {
        
        for(c = 1; c < rset->numFields; c++) {
            
            if(rset->nulls[c]) {
                asprintf(&tmp, "%s%s%s = NULL", setvals!=NULL ? setvals : "", setvals!=NULL ? "," : "",  rset->colNames[c]);
            } else {
                asprintf(&tmp, "%s%s%s = '%s'", setvals!=NULL ? setvals : "", setvals!=NULL ? "," : "",  rset->colNames[c], rset->data[c]);
            }

            free(setvals);
            setvals = tmp;
            tmp = NULL;
            
        }
        
        asprintf(&result, template, tablename, setvals, rset->colNames[0], rset->data[0]);
        free(setvals);
        
    }
    
    return result;
    
}

PGresult *prepare_update_with_pk(PGconn *conn, char *stmt_name, ResultSet *rset, char *tablename) {
    
    int c;
    char *setvals = NULL;
    char *tmp = NULL;
    char *update_query = NULL;
    PGresult *result = NULL;
    
    if(rset) {
        
        for(c = 1; c < rset->numFields; c++) {
            
            asprintf(&tmp, "%s%s%s = $%u", setvals!=NULL ? setvals : "", setvals!=NULL ? "," : "",  rset->colNames[c], c+1);
            free(setvals);
            setvals = tmp;
            tmp = NULL;
            
        }
        
        asprintf(&update_query, "UPDATE %s SET %s WHERE %s = $%u", tablename, setvals, rset->colNames[0], 1);
        free(setvals);
        
    }
    
    result = PQprepare(conn, stmt_name, update_query, 1, NULL);
    
    //printf("%s\n", update_query);
    free(update_query);
    
    return result;
    
}

PGresult *prepare_update_with_ctid(PGconn *conn, char *stmt_name, ResultSet *rset, char *tablename) {
    
    int c;
    char *setvals = NULL;
    char *tmp = NULL;
    char *update_query = NULL;
    PGresult *result = NULL;
    
    if(rset) {
        
        for(c = 1; c < rset->numFields; c++) {
            
            asprintf(&tmp, "%s%s%s = $%u", setvals!=NULL ? setvals : "", setvals!=NULL ? "," : "",  rset->colNames[c], c+1);
            free(setvals);
            setvals = tmp;
            tmp = NULL;
            
        }
        
        asprintf(&update_query, "UPDATE %s SET %s WHERE ctid = $%u", tablename, setvals, 1);
        free(setvals);
        
    }
    
    result = PQprepare(conn, stmt_name, update_query, 1, NULL);
    
    //printf("%s\n", update_query);
    free(update_query);
    
    return result;
    
}

PGresult *prepare_insert(PGconn *conn, char *stmt_name, ResultSet *rset, char *tablename) {
    
    int c;
    char *colnames = NULL;
    char *colvals = NULL;
    char *tmp = NULL;
    char *insert_query = NULL;
    PGresult *result = NULL;
    
    if(rset) {
        
        for(c = 0; c < rset->numFields; c++) {
            
            asprintf(&tmp, "%s%s%s", colnames!=NULL ? colnames : "", colnames!=NULL ? "," : "",  rset->colNames[c]);
            free(colnames);
            colnames = tmp;
            tmp = NULL;
            
            asprintf(&tmp, "%s%s$%u", colvals!=NULL ? colvals : "", colvals!=NULL ? "," : "",  c+1);
            
            free(colvals);
            colvals = tmp;
            tmp = NULL;
            
        }
        
        asprintf(&insert_query, "INSERT INTO %s(%s) VALUES (%s)", tablename, colnames, colvals);
        
        free(colnames);
        free(colvals);
        
    }
    
    result = PQprepare(conn, stmt_name, insert_query, 1, NULL);
    
    //printf("%s\n", insert_query);
    free(insert_query);
    
    return result;
    
}

char *generate_delete_dml(char *tablename, char *pkcol, char *pkval) {
    
    char *template = "DELETE FROM %s WHERE %s = '%s'";
    char *result = NULL;
    
    asprintf(&result, template, tablename, pkcol, pkval);
        
    return result;
    
}

PGresult *prepare_delete_with_pk(PGconn *conn, char *stmt_name, char *tablename, char *pkcol) {

    PGresult *result = NULL;
    char *template = "DELETE FROM %s WHERE %s = $1";
    char *delete_query = NULL;
    
    asprintf(&delete_query, template, tablename, pkcol);
    
    result = PQprepare(conn, stmt_name, delete_query, 1, NULL);
    
    free(delete_query);

    return result;
    
}

PGresult *prepare_delete_with_ctid(PGconn *conn, char *stmt_name, char *tablename) {

    PGresult *result = NULL;
    char *template = "DELETE FROM %s WHERE ctid = $1";
    char *delete_query = NULL;
    
    asprintf(&delete_query, template, tablename);
    
    result = PQprepare(conn, stmt_name, delete_query, 1, NULL);
    
    free(delete_query);

    return result;
    
}

PGresult *prepare_select_with_pk(PGconn *conn, char *stmt_name, char *tablename, char *pkcol) {

    PGresult *result = NULL;
    char *select_query = NULL;
    
    asprintf(&select_query, "SELECT * FROM %s WHERE %s = $1", tablename, pkcol);
    
    result = PQprepare(conn, stmt_name, select_query, 1, NULL);
    
    free(select_query);

    return result;
    
}

PGresult *prepare_select_with_pk_range(PGconn *conn, char *stmt_name, char *tablename, char *pkcol) {

    PGresult *result = NULL;
    char *select_query = NULL;
    
    asprintf(&select_query, "SELECT * FROM %s WHERE %s between $1 and $2", tablename, pkcol);
    
    result = PQprepare(conn, stmt_name, select_query, 1, NULL);
    
    free(select_query);

    return result;
    
}

PGresult *prepare_select_with_pk_max(PGconn *conn, char *stmt_name, char *tablename, char *pkcol) {

    PGresult *result = NULL;
    char *select_query = NULL;
    
    asprintf(&select_query, "SELECT * FROM %s WHERE %s > $1", tablename, pkcol);
    
    result = PQprepare(conn, stmt_name, select_query, 1, NULL);
    
    free(select_query);

    return result;
    
}

PGresult *prepare_delete_with_pk_max(PGconn *conn, char *stmt_name, char *tablename, char *pkcol) {

    PGresult *result = NULL;
    char *delete_query = NULL;
    
    asprintf(&delete_query, "DELETE FROM %s WHERE %s > $1", tablename, pkcol);
    
    result = PQprepare(conn, stmt_name, delete_query, 1, NULL);
    
    free(delete_query);

    return result;
    
}

PGresult *prepare_select_with_ctid(PGconn *conn, char *stmt_name, char *tablename) {

    PGresult *result = NULL;
    char *select_query = NULL;
    
    asprintf(&select_query, "SELECT * FROM %s WHERE ctid = $1", tablename);
    
    result = PQprepare(conn, stmt_name, select_query, 1, NULL);
    
    free(select_query);

    return result;
    
}

void add_pk_to_list(list_t *list, long pk_val) {
    
    if(list) {
        
        list_item_t *item = listitem_new((void*)pk_val);
        list_insert(list, item);

    }    
    
}

void add_ctid_to_list(list_t *list, char *ctid) {
    
    if(list) {
        
        list_item_t *item = listitem_new(strdup(ctid));
        list_insert(list, item);

    }    
    
}

void compare_tables_strategy_csum_with_pk(ResultSet *src, ResultSet *dst, list_t *inserts, list_t *updates, list_t *deletes) {
    
    int state = 0;
    char **src_data, **dst_data;

    do {
        
        switch(state) {
            
            case 0: 
                    src_data = pg_next(src);
                    dst_data = pg_next(dst);
                    break;
            case 1: 
                    src_data = pg_next(src);
                    break;
            case 2: 
                    dst_data = pg_next(dst);
                    break;
            
        }
        
        if(src_data != NULL && dst_data != NULL) {
            
            long src_pk_val = atol(src_data[0]);
            long dst_pk_val = atol(dst_data[0]);
            
            if(src_pk_val == dst_pk_val) {
                
                if(strcmp(src_data[1], dst_data[1]) != 0) {
                    add_pk_to_list(updates, src_pk_val);
                }
                
                state = 0;
                continue;
                
            }
            
            if(src_pk_val < dst_pk_val) {
                add_pk_to_list(inserts, src_pk_val);
                state = 1;
                continue;
            }
            
            if(src_pk_val > dst_pk_val) {
                add_pk_to_list(deletes, dst_pk_val);
                state = 2;
                continue;
            }
            
        } else
        
        if(src_data != NULL) {
            add_pk_to_list(inserts, atol(src_data[0]));
            state = 1;
            continue;
        } else 
            
        if(dst_data != NULL) {
            add_pk_to_list(deletes, atol(dst_data[0]));
            state = 2;
            continue;
        }
        
    } while(src_data != NULL || dst_data != NULL);
    
}

void compare_tables_strategy_csum_with_ctid(ResultSet *src, ResultSet *dst, list_t *inserts, list_t *updates, list_t *deletes) {
    
    int state = 0;
    char **src_data, **dst_data;

    do {
        
        switch(state) {
            
            case 0: 
                    src_data = pg_next(src);
                    dst_data = pg_next(dst);
                    break;
            case 1: 
                    src_data = pg_next(src);
                    break;
            case 2: 
                    dst_data = pg_next(dst);
                    break;
            
        }
        
        if(src_data != NULL && dst_data != NULL) {
            
            int k = strcmp(src_data[0], dst_data[0]);
            
            //printf("%s %s = %i\n", src_data[0], dst_data[0], k);
            
            if(k == 0) { 
                state = 0;
                continue;
            } else
            if(k < 0) {
                add_ctid_to_list(inserts, src_data[1]);
                state = 1;
                continue;
            } else {
                add_ctid_to_list(deletes, dst_data[1]);
                state = 2;
                continue;
            }
            
        } else
        
        if(src_data != NULL) {
            add_ctid_to_list(inserts, src_data[1]);
            state = 1;
            continue;
        } else 
            
        if(dst_data != NULL) {
            add_ctid_to_list(deletes, dst_data[1]);
            state = 2;
            continue;
        }
        
    } while(src_data != NULL || dst_data != NULL);
    
}

void perform_changes_with_pk(PGconn *src_conn, PGconn *dst_conn, char *src_table, char *dst_table, char *pkcol, list_t *inserts, list_t *updates, list_t *deletes) {
    
    list_item_t *item;
    char pkstart[100], pkend[100];
    char *statement;
    ResultSet *select_rset;
    ResultSet *cmd_rset;
    char **data;
    int inserted = 0, updated = 0, deleted = 0;
    
    long pk_range_start = 0, pk_range_end = 0;
    
    char select_rows[200];
    char update_rows[200];
    char delete_rows[200];
    char insert_rows[200];
    
    sprintf(select_rows, "select_rows_%s", src_table);
    sprintf(update_rows, "update_rows_%s", src_table);
    sprintf(delete_rows, "delete_rows_%s", src_table);
    sprintf(insert_rows, "insert_rows_%s", src_table);
    
    PGresult *select_stmt = prepare_select_with_pk_range(src_conn, select_rows, src_table, pkcol);
    PGresult *delete_stmt = NULL;
    PGresult *update_stmt = NULL;
    PGresult *insert_stmt = NULL;
    
    if(select_stmt) {
        
        for(item = list_first(deletes); item != NULL; item = list_next(item)) {
            
            sprintf(pkstart, "%lu", (long)item->data);
            char *values[] = { pkstart };
            
            if(delete_stmt == NULL)
                delete_stmt = prepare_delete_with_pk(dst_conn, delete_rows, dst_table, pkcol);
            
            cmd_rset = pg_execute_prepared(dst_conn, strdup(delete_rows), values, NULL, NULL, 1);
            
            if(cmd_rset) {
             
                deleted += cmd_rset->numRows;
                pg_finish(cmd_rset);
                
            }            
            
        }
        
        if(delete_stmt) {
            PQclear(delete_stmt);
            pg_unprepare(dst_conn, delete_rows);
        }

        for(item = list_first(updates); item != NULL; item = list_next(item)) {
            
            if(pk_range_start == 0)
                pk_range_start = (long)item->data;
            if(pk_range_end == 0)
                pk_range_end = (long)item->data;
            
            if(item->next && (long)item->next->data == pk_range_end + 1) {
                pk_range_end += 1;
                continue;
            }
            
//            if(pk_range_start < pk_range_end)
  //              printf("detected a PK range for UPDATE from %li to %li\n", pk_range_start, pk_range_end);
            
            sprintf(pkstart, "%li", pk_range_start);
            sprintf(pkend, "%li", pk_range_end);
            
            char *values[] = { pkstart, pkend };
            
            select_rset = pg_execute_prepared(src_conn, strdup(select_rows), values, NULL, NULL, 2);
            
            if(select_rset)
            
            while((data = pg_next(select_rset))) {
            
                if(update_stmt == NULL)
                    update_stmt = prepare_update_with_pk(dst_conn, update_rows, select_rset, dst_table);
                
                cmd_rset = pg_execute_prepared(dst_conn, strdup(update_rows), data, select_rset->lengths, select_rset->formats, select_rset->numFields);
            
                if(cmd_rset) {
             
                    updated += cmd_rset->numRows;
                    pg_finish(cmd_rset);
                
                }
                    
            }
                
            pg_finish(select_rset);
                
            pk_range_start = 0;
            pk_range_end = 0;
            
        }
        
            
        if(update_stmt) {
            PQclear(update_stmt);
            pg_unprepare(dst_conn, update_rows);
        }
        
        if(inserts->count > 0) {
            if(!pg_begin_copy(dst_conn, dst_table)) {
                printf("couldn't initiate COPY %s\n", dst_table);
            }
        }
        
        for(item = list_first(inserts); item != NULL; item = list_next(item)) {
            
            if(pk_range_start == 0)
                pk_range_start = (long)item->data;
            if(pk_range_end == 0)
                pk_range_end = (long)item->data;
            
            if(item->next && (long)item->next->data == pk_range_end + 1) {
                pk_range_end += 1;
                continue;
            }
            
//            if(pk_range_start < pk_range_end)
//                printf("detected a PK range for INSERT from %li to %li\n", pk_range_start, pk_range_end);
            
            sprintf(pkstart, "%li", pk_range_start);
            sprintf(pkend, "%li", pk_range_end);
            
            char *values[] = { pkstart, pkend };
            
            select_rset = pg_execute_prepared(src_conn, strdup(select_rows), values, NULL, NULL, 2);
            
            if(select_rset)
            
            while((data = pg_next(select_rset))) {
                inserted += pg_send_copy_data(dst_conn, select_rset);
            } 
            
            pg_finish(select_rset);
            
            pk_range_start = 0;
            pk_range_end = 0;
            
        }
        
        if(inserts->count > 0) {
            if(!pg_finish_copy(dst_conn)) {
                printf("couldn't complete COPY %s\n", dst_table);
                inserted = 0;
            }
        }
        
        if(insert_stmt) {
            PQclear(insert_stmt);
            pg_unprepare(dst_conn, insert_rows);
        }
        
        
        PQclear(select_stmt);
        pg_unprepare(src_conn, select_rows);
        
    }
    
    printf("performed %u inserts, %u updates, %u deletes\n", inserted, updated, deleted);
    
}

void perform_changes_with_max(PGconn *src_conn, PGconn *dst_conn, char *src_table, char *dst_table, char *pkcol, long src_max_val, long dst_max_val) {
    
    list_item_t *item;
    char pkval[100];
    char *statement;
    ResultSet *select_rset;
    ResultSet *cmd_rset;
    char **data;
    int inserted = 0, deleted = 0;
    
    char select_rows[200];
    char insert_rows[200];
    char delete_rows[200];
    
    sprintf(select_rows, "select_rows_%s", src_table);
    sprintf(delete_rows, "delete_rows_%s", src_table);
    
    if(src_max_val > dst_max_val) {
    
        PGresult *select_stmt = prepare_select_with_pk_max(src_conn, select_rows, src_table, pkcol);
        
        if(select_stmt) {
            
            if(!pg_begin_copy(dst_conn, dst_table)) {
                printf("couldn't initiate COPY %s\n", dst_table);
            }

            sprintf(pkval, "%lu", dst_max_val);
            char *values[] = { pkval };
            select_rset = pg_execute_prepared(src_conn, strdup(select_rows), values, NULL, NULL, 1);
        
            while((data = pg_next(select_rset))) {
                inserted += pg_send_copy_data(dst_conn, select_rset);
            } 
        
            if(!pg_finish_copy(dst_conn)) {
                printf("couldn't complete COPY %s\n", dst_table);
                inserted = 0;
            }
            
            pg_finish(select_rset);
            PQclear(select_stmt);
            pg_unprepare(src_conn, select_rows);
        
        }
    
        printf("performed %u inserts\n", inserted);
    
    } else {
        
        PGresult *delete_stmt = prepare_delete_with_pk_max(dst_conn, delete_rows, dst_table, pkcol);
        
        sprintf(pkval, "%lu", src_max_val);
        char *values[] = { pkval };
        cmd_rset = pg_execute_prepared(dst_conn, strdup(delete_rows), values, NULL, NULL, 1);
        
        if(cmd_rset) {
            
            deleted += cmd_rset->numRows;
            pg_finish(cmd_rset);
            
        }
        
        printf("performed %u deletes\n", deleted);
        
        PQclear(delete_stmt);
        
    }
    
}

void perform_changes_with_ctid(PGconn *src_conn, PGconn *dst_conn, char *src_table, char *dst_table, list_t *inserts, list_t *updates, list_t *deletes) {
    
    list_item_t *item;
    char ctid[100];
    char *statement;
    ResultSet *select_rset;
    ResultSet *cmd_rset;
    char **data;
    int inserted = 0, updated = 0, deleted = 0;
    
    char select_rows[200];
    char update_rows[200];
    char delete_rows[200];
    char insert_rows[200];
    
    sprintf(select_rows, "select_rows_%s", src_table);
    sprintf(update_rows, "update_rows_%s", src_table);
    sprintf(delete_rows, "delete_rows_%s", src_table);
    sprintf(insert_rows, "insert_rows_%s", src_table);
    
    PGresult *select_stmt = prepare_select_with_ctid(src_conn, select_rows, src_table);
    PGresult *delete_stmt = NULL;
    PGresult *update_stmt = NULL;
    PGresult *insert_stmt = NULL;
    
    if(select_stmt) {
        
        for(item = list_first(deletes); item != NULL; item = list_next(item)) {
            
            sprintf(ctid, "%s::point::ctid", (char*)item->data);
            char *values[] = { ctid };
            
            if(delete_stmt == NULL)
                delete_stmt = prepare_delete_with_ctid(dst_conn, delete_rows, dst_table);
            
            cmd_rset = pg_execute_prepared(dst_conn, strdup(delete_rows), values, NULL, NULL, 1);
            
            if(cmd_rset) {
             
                deleted += cmd_rset->numRows;
                pg_finish(cmd_rset);
                
            }            
            
        }
        
        if(delete_stmt) {
            PQclear(delete_stmt);
            pg_unprepare(dst_conn, delete_rows);
        }

        for(item = list_first(updates); item != NULL; item = list_next(item)) {
            
            sprintf(ctid, "%s::point::ctid", (char*)item->data);
            char *values[] = { ctid };
            
            select_rset = pg_execute_prepared(src_conn, strdup(select_rows), values, NULL, NULL, 1);
            data = pg_next(select_rset);
            
            if(select_rset && data) {
                
                if(update_stmt == NULL)
                    update_stmt = prepare_update_with_pk(dst_conn, update_rows, select_rset, dst_table);
                
                cmd_rset = pg_execute_prepared(dst_conn, strdup(update_rows), data, select_rset->lengths, select_rset->formats, select_rset->numFields);
            
                if(cmd_rset) {
             
                    updated += cmd_rset->numRows;
                    pg_finish(cmd_rset);
                
                }            
                
                pg_finish(select_rset);
                
            } 
            
        }
        
        if(update_stmt) {
            PQclear(update_stmt);
            pg_unprepare(dst_conn, update_rows);
        }
        
        if(inserts->count > 0) {
            if(!pg_begin_copy(dst_conn, dst_table)) {
                printf("couldn't initiate COPY %s\n", dst_table);
            }
        }

        for(item = list_first(inserts); item != NULL; item = list_next(item)) {
            
            sprintf(ctid, "%s::point::ctid", (char*)item->data);
            char *values[] = { ctid };
            
            select_rset = pg_execute_prepared(src_conn, strdup(select_rows), values, NULL, NULL, 1);
            data = pg_next(select_rset);
            
            if(select_rset && data) {
                inserted += pg_send_copy_data(dst_conn, select_rset);
                pg_finish(select_rset);
            } 
            
        }
        
        if(inserts->count > 0) {
            if(!pg_finish_copy(dst_conn)) {
                printf("couldn't complete COPY %s\n", dst_table);
                inserted = 0;
            }
        }
        
        PQclear(select_stmt);
        pg_unprepare(src_conn, select_rows);
        
    }
    
    printf("performed %u inserts, %u updates, %u deletes\n", inserted, updated, deleted);
    
}

char *determine_pk(PGconn *conn, char *tablename) {
    
    char *query = NULL;
    ResultSet *rset = NULL;
    char *result = NULL;
    asprintf(&query, "select attname from pg_class a join pg_constraint b on a.oid = b.conrelid join pg_attribute c on a.oid = c.attrelid and conkey @> array[c.attnum] where contype = 'p' and relname = '%s'", tablename);
    
    rset = pg_query(conn, query);
    
    if(rset && pg_next(rset)) {
        
        if(rset->numRows > 1) {
            
            printf("table %s appears to have a composite PK\n", tablename);
            
        } else if(rset->numRows == 1) {
            
            printf("found primary key column %s for table %s\n", rset->data[0], tablename);
            result = strdup(rset->data[0]);
            
        } else {
            
            printf("couldn't determine PK for table %s\n", tablename);
            
        }
        
    }
    
    pg_finish(rset);
    
    return result;
    
}

void synchronize_table_csum_with_pk_or_ctid(PGconn *src_conn, PGconn *dst_conn, char *tablename) {
    
    char *pkcol = determine_pk(src_conn, tablename);
    
    list_t *inserts = list_new();
    list_t *deletes = list_new();
    list_t *updates = list_new();
    
    if(pkcol) {
    
        ResultSet *src_checksums = checksum_table_data_with_pk(src_conn, tablename, pkcol);
        ResultSet *dst_checksums = checksum_table_data_with_pk(dst_conn, tablename, pkcol);
        
        if(src_checksums && dst_checksums) {
        
            compare_tables_strategy_csum_with_pk(src_checksums, dst_checksums, inserts, updates, deletes);
        
            printf("analyzed %u source rows, %u destination rows\n", src_checksums->currentRow, dst_checksums->currentRow);
    
            printf("calculated %u inserts, %u updates, %u deletes\n", inserts->count, updates->count, deletes->count);
        
            if(inserts->count > 0 || updates->count > 0 || deletes->count > 0)
                perform_changes_with_pk(src_conn, dst_conn, tablename, tablename, pkcol, inserts, updates, deletes);
        
        } else {
            
            printf("unable to generate checksums\n");
            
        }
    
        pg_finish(src_checksums);
        pg_finish(dst_checksums);
        
        free(pkcol);
    
        list_free(&inserts);
        list_free(&deletes);
        list_free(&updates);
    
    } else {
        
        printf("failed to determine a suitable PK. Resorting to ctid.\n");
        
        ResultSet *src_checksums = checksum_table_data_with_ctid(src_conn, tablename);
        ResultSet *dst_checksums = checksum_table_data_with_ctid(dst_conn, tablename);
        
        if(src_checksums && dst_checksums) {
    
            compare_tables_strategy_csum_with_ctid(src_checksums, dst_checksums, inserts, updates, deletes);
        
            printf("analyzed %u source rows, %u destination rows\n", src_checksums->currentRow, dst_checksums->currentRow);
        
            printf("calculated %u inserts, %u updates, %u deletes\n", inserts->count, updates->count, deletes->count);
        
            if(inserts->count > 0 || updates->count > 0 || deletes->count > 0)
                perform_changes_with_ctid(src_conn, dst_conn, tablename, tablename, inserts, updates, deletes);
            
        } else {
            
            printf("unable to generate checksums\n");
            
        }

        pg_finish(src_checksums);
        pg_finish(dst_checksums);
        
        list_free2(&inserts);
        list_free2(&deletes);
        list_free2(&updates);
        
    }
    
}

void synchronize_table_max(PGconn *src_conn, PGconn *dst_conn, char *tablename) {
    
    char *pkcol = determine_pk(src_conn, tablename);
    
    if(pkcol) {
    
        ResultSet *src_pk_max = get_pk_max(src_conn, tablename, pkcol);
        ResultSet *dst_pk_max = get_pk_max(dst_conn, tablename, pkcol);
        
        long src_pk_max_val = 0;
        long dst_pk_max_val = 0;
        
        if(pg_next(src_pk_max)) {
            
            src_pk_max_val = atol(src_pk_max->data[0]);
            
        }

        if(pg_next(dst_pk_max) && dst_pk_max->data[0]) {
            
            dst_pk_max_val = atol(dst_pk_max->data[0]);
            
        }
        
        if(src_pk_max_val-dst_pk_max_val > 0) {
    
            printf("Source max(%s) = %lu, Destination max(%s) = %lu, up to %lu inserts to be performed\n", pkcol, src_pk_max_val, pkcol, dst_pk_max_val, src_pk_max_val-dst_pk_max_val);
            
        } else if(src_pk_max_val-dst_pk_max_val < 0) {
            
            printf("Source max(%s) = %lu, Destination max(%s) = %lu, up to %lu deletes to be performed\n", pkcol, src_pk_max_val, pkcol, dst_pk_max_val, dst_pk_max_val-src_pk_max_val);
            
        } else {
            
            printf("Source max(%s) = %lu, Destination max(%s) = %lu, nothing to do\n", pkcol, src_pk_max_val, pkcol, dst_pk_max_val);
            
        }
    
        if(src_pk_max_val != dst_pk_max_val) {
            perform_changes_with_max(src_conn, dst_conn, tablename, tablename, pkcol, src_pk_max_val, dst_pk_max_val);
        }
        
        pg_finish(src_pk_max);
        pg_finish(dst_pk_max);
        
        free(pkcol);
    
    
    } else {
        
        printf("failed to determine a suitable PK. Resorting to ctid.\n");
        
    }
    
   
}