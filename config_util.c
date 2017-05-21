#include "config_util.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "list.h"

char *trim(char *str) {
  
    char *end;

  while(isspace((unsigned char)*str)) 
      str++;

  if(*str == 0)
    return str;

  end = str + strlen(str) - 1;
  while(end > str && isspace((unsigned char)*end))
      end--;

  *(end+1) = 0;

  return str;
}

ConfigFile *loadConfig(char *path) {
    
    ConfigFile *result = NULL;
    FILE *file = fopen(path, "ro");
    char *buf, *p, *pp, *key, *val;
    int state = 0;
    list_t *table_list;
    
    if(file) {
        
        result = calloc(1, sizeof(ConfigFile));
        buf = malloc(500);
        table_list = list_new();
        
        result->loopDelay = -1;
        
        while(!feof(file) && fgets(buf, 500, file)!=NULL) {
            
            if(buf[0] == '\n')
                continue;
            if(buf[0] == '#')
                continue;
            
            if(strcmp(buf, "[src]\n") == 0) {
                
                state = 1;
                continue;
                
            }
            
            if(strcmp(buf, "[dst]\n") == 0) {
                
                state = 2;
                continue;
                
            }
            
            if(strcmp(buf, "[tables]\n") == 0) {
                
                state = 3;
                continue;
                
            }
            
            if(strcmp(buf, "[global]\n") == 0) {
                
                state = 4;
                continue;
                
            }
            
            pp = buf;
            p = strsep(&pp, "=");
            key = trim(p);
            if(pp)
                val = trim(pp);
            else
                val = NULL;
            
            switch(state) {
                
                case 1:
                        if(strcmp(key, "host") == 0)
                            result->src_host = strdup(val);
                        if(strcmp(key, "port") == 0)
                            result->src_port = strdup(val);
                        if(strcmp(key, "dbname") == 0)
                            result->src_dbname = strdup(val);
                        if(strcmp(key, "user") == 0)
                            result->src_user = strdup(val);
                        if(strcmp(key, "password") == 0)
                            result->src_password = strdup(val);
                                   
                        break;
                case 2: 
                        if(strcmp(key, "host") == 0)
                            result->dst_host = strdup(val);
                        if(strcmp(key, "port") == 0)
                            result->dst_port = strdup(val);
                        if(strcmp(key, "dbname") == 0)
                            result->dst_dbname = strdup(val);
                        if(strcmp(key, "user") == 0)
                            result->dst_user = strdup(val);
                        if(strcmp(key, "password") == 0)
                            result->dst_password = strdup(val);
                    
                        break;
                case 3: 
                    
                { 
                    
                        TableEntry *tableEntry = calloc(1, sizeof(TableEntry));
                        
                        tableEntry->tableName = strdup(key);
                        
                        if(val) {
                            tableEntry->strategy = strdup(val);
                        } else {
                            tableEntry->strategy = strdup("hash");
                        }
                    
                        list_insert(table_list, listitem_new(tableEntry));
                        break;
                        
                }
                
                case 4: 
                        if(strcmp(key, "transaction") == 0)
                            result->transaction = atoi(val);
                        if(strcmp(key, "loop_delay") == 0)
                            result->loopDelay = atoi(val);
                        
                        
                    
                        break;
            }
            
        } 
        
        fclose(file);
        
        list_item_t *item;
        int i = 0;
        
        result->tableCount = table_list->count;
        result->tables = calloc(result->tableCount, sizeof(char*));
        result->strategies = calloc(result->tableCount, sizeof(int));
        
        for(item = list_first(table_list); item != NULL; item = item->next) {
            
            TableEntry *entry = (TableEntry*)item->data;
            
            result->tables[i] = entry->tableName;
            
            if(strcasecmp(entry->strategy, "max")==0) {
                result->strategies[i] = MAX;
            } else if(strcasecmp(entry->strategy, "hybrid") == 0) {
                result->strategies[i] = HYBRID;
            } else {
                result->strategies[i] = HASH;
            }
            
            free(entry->strategy);
            free(entry);
            
            i++;
        }
        
        list_free(&table_list);
        
        
    } else {
        
        perror("fopen");
        
    }
    
    free(buf);
    
    return result;
    
}

void freeConfig(ConfigFile *config) {
    
    int c;
    
    if(config) {
        
        free(config->src_host);
        free(config->src_port);
        free(config->src_user);
        free(config->src_password);
        free(config->src_dbname);
    
        free(config->dst_host);
        free(config->dst_port);
        free(config->dst_user);
        free(config->dst_password);
        free(config->dst_dbname);
    
        for(c = 0; c < config->tableCount; c++) {
            free(config->tables[c]);
        }
        
        free(config->tables);
        free(config->strategies);
        
        free(config);
        
    }
    
}