#pragma once
#include <pthread.h>
#include "sqlite3.h"

typedef struct unlock {
    int fired;
    pthread_cond_t cond;
    pthread_mutex_t mu;
} unlock;

unlock* unlock_alloc();
void unlock_fire(unlock* un);
void unlock_free(unlock* un);

int _sqlite3_blocking_prepare_v3(sqlite3* db, unlock* un, char const* zSql, int nByte, unsigned int prepFlags, sqlite3_stmt** ppStmt, char const** pzTail);
int _sqlite3_blocking_step(unlock* un, sqlite3_stmt* pStmt);

static inline int _sqlite3_bind_blob(sqlite3_stmt* s, int i, void const* p, int n, int copy) {
    return sqlite3_bind_blob(s, i, p, n, (copy ? SQLITE_TRANSIENT : SQLITE_STATIC));
}

static inline int _sqlite3_bind_text(sqlite3_stmt* s, int i, char const* p, int n, int copy) {
    return sqlite3_bind_text(s, i, p, n, (copy ? SQLITE_TRANSIENT : SQLITE_STATIC));
}

static inline int str_offset(char const* start, char const* p) {
    return (int)(p - start);
}

extern void _sqliteLogFunc(void* pArg, int code, char* msg);
static inline int _sqlite_enable_logging() {
    return sqlite3_config(SQLITE_CONFIG_LOG, _sqliteLogFunc, NULL);
}


extern int go_trace_callback(unsigned,void*,void*,void*);

static inline int registerProfile(sqlite3* db, void* goConn) {
   return sqlite3_trace_v2(db, SQLITE_TRACE_PROFILE, go_trace_callback, goConn);
}
