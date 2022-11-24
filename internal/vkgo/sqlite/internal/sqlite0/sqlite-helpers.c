#include <stdlib.h>
#include "sqlite-helpers.h"

unlock* unlock_alloc() {
    unlock* un = (unlock*)malloc(sizeof(*un));
    pthread_mutex_init(&un->mu, 0);
    pthread_cond_init(&un->cond, 0);
    return un;
}

void unlock_free(unlock* un) {
    pthread_cond_destroy(&un->cond);
    pthread_mutex_destroy(&un->mu);
    free(un);
}

void unlock_fire(unlock* un) {
    pthread_mutex_lock(&un->mu);
    un->fired = 1;
    pthread_cond_signal(&un->cond);
    pthread_mutex_unlock(&un->mu);
}

static void unlock_notify_cb(void** uns, int n) {
    for (int i = 0; i < n; i++) {
        unlock_fire((unlock*)uns[i]);
    }
}

static int wait_for_unlock_notify(sqlite3* db, unlock* un) {
    un->fired = 0;
    int res = sqlite3_unlock_notify(db, unlock_notify_cb, (void*)un);
    if (res == SQLITE_OK) {
        pthread_mutex_lock(&un->mu);
        if (!un->fired) {
            pthread_cond_wait(&un->cond, &un->mu);
        }
        pthread_mutex_unlock(&un->mu);
    }
    return res;
}

int _sqlite3_blocking_prepare_v3(sqlite3* db, unlock* un, char const* zSql, int nByte, unsigned int prepFlags, sqlite3_stmt** ppStmt, char const** pzTail) {
    int rc;
    for (;;) {
        rc = sqlite3_prepare_v3(db, zSql, nByte, prepFlags, ppStmt, pzTail);
        if (rc != SQLITE_LOCKED) {
            break;
        }
        if (sqlite3_extended_errcode(db) != SQLITE_LOCKED_SHAREDCACHE) {
            break;
        }
        rc = wait_for_unlock_notify(db, un);
        if (rc != SQLITE_OK) {
            break;
        }
    }
    return rc;
}

int _sqlite3_blocking_step(unlock* un, sqlite3_stmt* pStmt) {
    int rc;
    for (;;) {
        rc = sqlite3_step(pStmt);
        if (rc != SQLITE_LOCKED) {
            break;
        }
        if (sqlite3_extended_errcode(sqlite3_db_handle(pStmt)) != SQLITE_LOCKED_SHAREDCACHE) {
            break;
        }
        rc = wait_for_unlock_notify(sqlite3_db_handle(pStmt), un);
        if (rc != SQLITE_OK) {
            break;
        }
        sqlite3_reset(pStmt);
    }
    return rc;
}
