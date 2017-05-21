#ifndef CONFIG_UTIL_H
#define CONFIG_UTIL_H

typedef struct {
    
    char *src_host;
    char *src_port;
    char *src_user;
    char *src_password;
    char *src_dbname;
    
    char *dst_host;
    char *dst_port;
    char *dst_user;
    char *dst_password;
    char *dst_dbname;
    
    int tableCount;
    char **tables;
    int *strategies;
    
    int transaction;
    int loopDelay;
    
} ConfigFile;

typedef struct {
    
    char *tableName;
    char *strategy;
    
} TableEntry;

enum {
    
    HASH,
    MAX,
    HYBRID
    
} Strategy;

static char *SyncStrategies[] = { "HASH", "MAX", "HYBRID" };

ConfigFile *loadConfig(char *path);
void freeConfig(ConfigFile *config);

#endif 

