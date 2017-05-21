#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include "list.h"
#include "pg_util.h"
#include "sync_util.h"
#include "config_util.h"
//#include <mcheck.h>

int main(int argc, char** argv) {
    
    int i = 0;

    //mtrace();
    
    if(argc != 2 && argc != 3) {
        printf("usage : %s sync.conf\n", argv[0]);
        return 0;
    }
    
    ConfigFile *config = loadConfig(argv[1]);
    
    if(config!=NULL) {
        
        PGconn *src_conn = pg_connect(config->src_host, config->src_user,  config->src_password, config->src_dbname, config->src_port);
        PGconn *dst_conn = pg_connect(config->dst_host, config->dst_user,  config->dst_password, config->dst_dbname, config->dst_port);
        
        if(!src_conn || !dst_conn) {
            
            printf("unable to connect to the server(s)\n");
            exit(1);
            
        }
        
        pg_exec(dst_conn, "set session_replication_role = replica");
        
        do {
            
            if(config->transaction)
                printf("starting a transaction on the source\n");
                pg_begin(src_conn);
        
            for(i = 0; i < config->tableCount; i++) {
            
                printf("\nAttempting to synchronize table %s with strategy %s\n",config->tables[i], SyncStrategies[config->strategies[i]]);
            
//                disable_constraints(dst_conn, config->tables[i]);
            
                switch(config->strategies[i]) {
                        case HASH:synchronize_table_csum_with_pk_or_ctid(src_conn, dst_conn, config->tables[i]); break;
                        case MAX:synchronize_table_max(src_conn, dst_conn, config->tables[i]); break;
                        case HYBRID:synchronize_table_max(src_conn, dst_conn, config->tables[i]);
                                    synchronize_table_csum_with_pk_or_ctid(src_conn, dst_conn, config->tables[i]); 
                            break;
                }
            
//                enable_constraints(dst_conn, config->tables[i]);
                
                if(PQstatus(src_conn) == CONNECTION_BAD || PQstatus(dst_conn) == CONNECTION_BAD) {
                    printf("lost server connection(s), aborting...\n");
                    break;
                }
                                            
            }
        
            if(config->transaction)
                printf("closing transaction on the source\n");
                pg_end(src_conn);
            
            if(config->loopDelay > 0) {
                printf("sleeping %i seconds before next run...\n", i);
                sleep(config->loopDelay);
            }
        
        } while(config->loopDelay >= 0);

        pg_close(src_conn);
        pg_close(dst_conn);
        
    }
    
    freeConfig(config);
    
    return (EXIT_SUCCESS);
    
}

