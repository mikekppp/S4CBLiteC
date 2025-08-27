//
//  cblutil.h
//  couchtest
//
//  Created by Michael Papp on 8/27/25.
//

#ifndef cblutil_h
#define cblutil_h


#pragma once
#include <stdio.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles — callers don't need Couchbase headers
typedef struct CBLU_Db      CBLU_Db;
typedef struct CBLU_Session CBLU_Session;
typedef struct CBLU_DocW    CBLU_DocW;   // writeable doc
typedef struct CBLU_DocR    CBLU_DocR;   // readable doc

// ---- Database lifecycle ----
bool cblu_open(const char* db_name, const char* dir, CBLU_Db** out_db);  // creates if missing
void cblu_close(CBLU_Db* db);

// ---- Session (optional transaction-like boundary) ----
CBLU_Session* cblu_session_begin(CBLU_Db* db);           // default collection
void          cblu_session_end(CBLU_Session* s);         // flush/cleanup (no txn for simplicity)

// ---- Write document API ----
CBLU_DocW* cblu_docw_begin(CBLU_Session* s, const char* doc_id); // create/overwrite by id
void       cblu_docw_set_i64(CBLU_DocW* d, const char* key, int64_t v);
void       cblu_docw_set_u64(CBLU_DocW* d, const char* key, uint64_t v);
void       cblu_docw_set_f64(CBLU_DocW* d, const char* key, double v);
void       cblu_docw_set_str(CBLU_DocW* d, const char* key, const char* s); // UTF-8
void       cblu_docw_set_f64_array(CBLU_DocW* d, const char* key, const double* a, size_t n);
void       cblu_docw_set_i64_array(CBLU_DocW* d, const char* key, const int64_t* a, size_t n);
bool       cblu_docw_save(CBLU_DocW* d);  // commits into collection
void       cblu_docw_free(CBLU_DocW* d);  // safe if not saved

// ---- Read document API ----
CBLU_DocR* cblu_docr_get(CBLU_Session* s, const char* doc_id); // NULL if missing
bool       cblu_docr_has(CBLU_DocR* d, const char* key);

// returns true if present; outputs into *out (unchanged if missing or wrong type)
bool cblu_docr_get_i64(CBLU_DocR* d, const char* key, int64_t* out);
bool cblu_docr_get_u64(CBLU_DocR* d, const char* key, uint64_t* out);
bool cblu_docr_get_f64(CBLU_DocR* d, const char* key, double* out);
// Copies at most dst_size-1 bytes and NUL-terminates; returns bytes written (excluding NUL) or 0 if missing.
size_t     cblu_docr_get_str(CBLU_DocR* d, const char* key, char* dst, size_t dst_size);
// Returns number of items copied (<= maxn). Missing/non-array → 0.
size_t     cblu_docr_get_f64_array(CBLU_DocR* d, const char* key, double* out, size_t maxn);
size_t     cblu_docr_get_i64_array(CBLU_DocR* d, const char* key, int64_t* out, size_t maxn);
void       cblu_docr_free(CBLU_DocR* d);

#ifdef __cplusplus
}
#endif

#endif /* cblutil_h */
