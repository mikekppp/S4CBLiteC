//
//  cblutil.c
//
//  Utility wrapper over Couchbase Lite C (Fleece) for simple DB/session/doc IO
//

#include "cblutil.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

// If your installation uses framework-style includes, swap these for <cbl/...>
#include "CBLDatabase.h"
#include "CBLCollection.h"
#include "CBLDocument.h"
#include "Fleece.h"
#include "CBLBlob.h"

// --- Fleece portability shims (older/newer headers may use 'Unsigned' vs 'UInt') ---
#if !defined(FLMutableDict_SetUInt) && defined(FLMutableDict_SetUnsigned)
#define FLMutableDict_SetUInt FLMutableDict_SetUnsigned
#endif

#if !defined(FLMutableArray_AppendUInt) && defined(FLMutableArray_AppendUnsigned)
#define FLMutableArray_AppendUInt FLMutableArray_AppendUnsigned
#endif

// --- Small helpers ---
static inline bool fl_is_number(FLValue v) {
	return v && FLValue_GetType(v) == kFLNumber;
}

static inline bool fl_is_string(FLValue v) {
	return v && FLValue_GetType(v) == kFLString;
}

// --- Opaque/inner structs ---
typedef struct {
	CBLDatabase*   db;
	CBLCollection* coll;
} CBLU_Core;

struct CBLU_Db      { CBLU_Core core; };
struct CBLU_Session { CBLU_Core core; bool txn_active; };
struct CBLU_DocW    { const CBLU_Core* core; CBLDocument* doc; FLMutableDict props; };
struct CBLU_DocR    { const CBLU_Core* core; const CBLDocument* doc; FLDict props; };

static inline FLString fl_from_c(const char* s) {
	return (FLString){ .buf = s, .size = s ? strlen(s) : 0 };
}

CBLU_Session* cblu_session_begin_txn(CBLU_Db* db, bool use_txn);

// ---- Database ----
bool cblu_open(const char* db_name, const char* dir, CBLU_Db** out_db) {
	if (!out_db || !db_name) return false;
	*out_db = NULL;

	CBLError err = {0};
	CBLDatabaseConfiguration cfg = {0};
	cfg.directory = fl_from_c(dir);

	CBLDatabase* db = CBLDatabase_Open(fl_from_c(db_name), &cfg, &err);
	if (!db) {
		fprintf(stderr, "CBL open failed: domain=%d code=%d\n", (int)err.domain, (int)err.code);
		return false;
	}

	CBLCollection* coll = CBLDatabase_DefaultCollection(db, &err);
	if (!coll) {
		fprintf(stderr, "CBL default collection failed: domain=%d code=%d\n", (int)err.domain, (int)err.code);
		CBLDatabase_Close(db, NULL);
		CBLDatabase_Release(db);
		return false;
	}

	CBLU_Db* h = (CBLU_Db*)calloc(1, sizeof *h);
	h->core.db   = db;
	h->core.coll = coll;
	*out_db = h;
	return true;
}

void cblu_close(CBLU_Db* db) {
	if (!db) return;
	if (db->core.coll) { CBLCollection_Release(db->core.coll); db->core.coll = NULL; }
	if (db->core.db)   { CBLDatabase_Close(db->core.db, NULL); CBLDatabase_Release(db->core.db); db->core.db = NULL; }
	free(db);
}

// ---- Session ----
CBLU_Session* cblu_session_begin(CBLU_Db* db) {
	return cblu_session_begin_txn(db, false);
}

CBLU_Session* cblu_session_begin_txn(CBLU_Db* db, bool use_txn) {
	if (!db) return NULL;
	CBLU_Session* s = (CBLU_Session*)calloc(1, sizeof *s);
	s->core = db->core;
	s->txn_active = false;
	if (use_txn) {
		CBLError err = {0};
		if (!CBLDatabase_BeginTransaction(s->core.db, &err)) {
			fprintf(stderr, "CBL begin txn failed: domain=%d code=%d\n", (int)err.domain, (int)err.code);
			free(s); return NULL;
		}
		s->txn_active = true;
	}
	return s;
}

void cblu_session_end_txn(CBLU_Session* s, bool commit) {
	if (!s) return;
	if (s->txn_active) {
		CBLError err = {0};
		if (!CBLDatabase_EndTransaction(s->core.db, commit, &err)) {
			fprintf(stderr, "CBL end txn failed: domain=%d code=%d\n", (int)err.domain, (int)err.code);
		}
		s->txn_active = false;
	}
	free(s);
}

void cblu_session_end(CBLU_Session* s) {
	cblu_session_end_txn(s, true);
}

// ---- Write doc ----
CBLU_DocW* cblu_docw_begin(CBLU_Session* s, const char* doc_id) {
	if (!s || !doc_id) return NULL;
	CBLU_DocW* d = (CBLU_DocW*)calloc(1, sizeof *d);
	d->core  = &s->core;
	d->doc   = CBLDocument_CreateWithID(fl_from_c(doc_id));
	d->props = CBLDocument_MutableProperties(d->doc);
	return d;
}

static inline void set_key_i64(FLMutableDict props, const char* k, int64_t v)   { FLMutableDict_SetInt(props,  fl_from_c(k), v); }
static inline void set_key_u64(FLMutableDict props, const char* k, uint64_t v)  { FLMutableDict_SetUInt(props, fl_from_c(k), v); }
static inline void set_key_f64(FLMutableDict props, const char* k, double v)    { FLMutableDict_SetDouble(props, fl_from_c(k), v); }
static inline void set_key_str(FLMutableDict props, const char* k, const char* s){ FLMutableDict_SetString(props, fl_from_c(k), fl_from_c(s ? s : "")); }

void cblu_docw_set_i64 (CBLU_DocW* d, const char* key, int64_t v)   { if (d && key) set_key_i64 (d->props, key, v); }
void cblu_docw_set_u64 (CBLU_DocW* d, const char* key, uint64_t v)  { if (d && key) set_key_u64 (d->props, key, v); }
void cblu_docw_set_f64 (CBLU_DocW* d, const char* key, double v)    { if (d && key) set_key_f64 (d->props, key, v); }
void cblu_docw_set_str (CBLU_DocW* d, const char* key, const char* s){ if (d && key) set_key_str (d->props, key, s); }

void cblu_docw_set_f64_array(CBLU_DocW* d, const char* key, const double* a, size_t n) {
	if (!d || !key) return;
	FLMutableArray arr = FLMutableArray_New();
	for (size_t i=0; i<n; i++) FLMutableArray_AppendDouble(arr, a ? a[i] : 0.0);
	FLMutableDict_SetArray(d->props, fl_from_c(key), arr);
	FLMutableArray_Release(arr);
}

void cblu_docw_set_i64_array(CBLU_DocW* d, const char* key, const int64_t* a, size_t n) {
	if (!d || !key) return;
	FLMutableArray arr = FLMutableArray_New();
	for (size_t i=0; i<n; i++) FLMutableArray_AppendInt(arr, a ? a[i] : 0);
	FLMutableDict_SetArray(d->props, fl_from_c(key), arr);
	FLMutableArray_Release(arr);
}

void cblu_docw_set_bool(CBLU_DocW* d, const char* key, bool v) {
	if (!d || !key) return;
	FLMutableDict_SetBool(d->props, fl_from_c(key), v);
}

bool cblu_docw_set_blob(CBLU_DocW* d, const char* key, const void* data, size_t size, const char* contentType) {
	if (!d || !key || (!data && size>0)) return false;
//	CBLError err = {0};
	FLSlice slice = { .buf = data, .size = size };
	CBLBlob* blob = CBLBlob_CreateWithData(
		fl_from_c(contentType ? contentType : "application/octet-stream"),
		slice);
	if (!blob) return false;
	FLMutableDict_SetBlob(d->props, fl_from_c(key), blob);
	CBLBlob_Release(blob);
	return true;
}

FLMutableDict cblu_docw_begin_dict(CBLU_DocW* d, const char* key) {
	if (!d || !key) return NULL;
	FLMutableDict sub = FLMutableDict_New();
	FLMutableDict_SetDict(d->props, fl_from_c(key), sub);
	return sub;
}

void cblu_docw_dict_set_i64(FLMutableDict dict, const char* key, int64_t v)   { if (dict && key) FLMutableDict_SetInt   (dict, fl_from_c(key), v); }
void cblu_docw_dict_set_u64(FLMutableDict dict, const char* key, uint64_t v)  { if (dict && key) FLMutableDict_SetUInt  (dict, fl_from_c(key), v); }
void cblu_docw_dict_set_f64(FLMutableDict dict, const char* key, double v)    { if (dict && key) FLMutableDict_SetDouble(dict, fl_from_c(key), v); }
void cblu_docw_dict_set_bool(FLMutableDict dict, const char* key, bool v)     { if (dict && key) FLMutableDict_SetBool  (dict, fl_from_c(key), v); }
void cblu_docw_dict_set_str(FLMutableDict dict, const char* key, const char* s){ if (dict && key) FLMutableDict_SetString(dict, fl_from_c(key), fl_from_c(s ? s : "")); }

void cblu_docw_end_dict(CBLU_DocW* d, FLMutableDict dict) {
	(void)d; if (dict) FLMutableDict_Release(dict);
}

FLMutableArray cblu_docw_begin_array(CBLU_DocW* d, const char* key) {
	if (!d || !key) return NULL;
	FLMutableArray arr = FLMutableArray_New();
	FLMutableDict_SetArray(d->props, fl_from_c(key), arr);
	return arr;
}

void cblu_docw_array_append_i64 (FLMutableArray arr, int64_t v)  { if (arr) FLMutableArray_AppendInt    (arr, v); }
void cblu_docw_array_append_u64 (FLMutableArray arr, uint64_t v) { if (arr) FLMutableArray_AppendUInt   (arr, v); }
void cblu_docw_array_append_f64 (FLMutableArray arr, double v)   { if (arr) FLMutableArray_AppendDouble (arr, v); }
void cblu_docw_array_append_bool(FLMutableArray arr, bool v)     { if (arr) FLMutableArray_AppendBool   (arr, v); }
void cblu_docw_array_append_str (FLMutableArray arr, const char* s){ if (arr) FLMutableArray_AppendString(arr, fl_from_c(s ? s : "")); }

void cblu_docw_end_array(CBLU_DocW* d, FLMutableArray arr) {
	(void)d; if (arr) FLMutableArray_Release(arr);
}

bool cblu_docw_save(CBLU_DocW* d) {
	if (!d) return false;
	CBLError err = {0};
	bool ok = CBLCollection_SaveDocument(d->core->coll, d->doc, &err);
	if (!ok) fprintf(stderr, "CBL save failed: domain=%d code=%d\n", (int)err.domain, (int)err.code);
	CBLDocument_Release(d->doc); // doc retained by collection if saved
	d->doc = NULL;
	d->props = NULL;
	free(d);
	return ok;
}

void cblu_docw_free(CBLU_DocW* d) {
	if (!d) return;
	if (d->doc) { CBLDocument_Release(d->doc); d->doc = NULL; }
	d->props = NULL;
	free(d);
}

// ---- Read doc ----
CBLU_DocR* cblu_docr_get(CBLU_Session* s, const char* doc_id) {
	if (!s || !doc_id) return NULL;
	CBLError err = {0};
	const CBLDocument* doc = CBLCollection_GetDocument(s->core.coll, fl_from_c(doc_id), &err);
	if (!doc) return NULL;
	CBLU_DocR* d = (CBLU_DocR*)calloc(1, sizeof *d);
	d->core  = &s->core;
	d->doc   = doc;
	d->props = CBLDocument_Properties(doc);
	return d;
}

bool cblu_docr_has(CBLU_DocR* d, const char* key) {
	if (!d || !key) return false;
	FLValue v = FLDict_Get(d->props, fl_from_c(key));
	return v != NULL;
}

bool cblu_docr_get_i64(CBLU_DocR* d, const char* key, int64_t* out) {
	if (!d || !key || !out) return false;
	FLValue v = FLDict_Get(d->props, fl_from_c(key));
	if (!fl_is_number(v)) return false;
	*out = (int64_t)FLValue_AsInt(v);
	return true;
}

bool cblu_docr_get_u64(CBLU_DocR* d, const char* key, uint64_t* out) {
	if (!d || !key || !out) return false;
	FLValue v = FLDict_Get(d->props, fl_from_c(key));
	if (!fl_is_number(v)) return false;
	*out = (uint64_t)FLValue_AsUnsigned(v); // correct unsigned accessor
	return true;
}

bool cblu_docr_get_f64(CBLU_DocR* d, const char* key, double* out) {
	if (!d || !key || !out) return false;
	FLValue v = FLDict_Get(d->props, fl_from_c(key));
	if (!fl_is_number(v)) return false;
	*out = FLValue_AsDouble(v);
	return true;
}

bool cblu_docr_get_bool(CBLU_DocR* d, const char* key, bool* out) {
	if (!d || !key || !out) return false;
	FLValue v = FLDict_Get(d->props, fl_from_c(key));
	if (!v) return false;
	if (FLValue_GetType(v) == kFLBoolean || FLValue_GetType(v) == kFLNumber) {
		*out = FLValue_AsBool(v);
		return true;
	}
	return false;
}

size_t cblu_docr_get_str(CBLU_DocR* d, const char* key, char* dst, size_t dst_size) {
	if (!d || !key || !dst || dst_size == 0) return 0;
	FLValue v = FLDict_Get(d->props, fl_from_c(key));
	if (!fl_is_string(v)) { dst[0] = 0; return 0; }
	FLString s = FLValue_AsString(v);
	size_t n = (s.buf && s.size < (dst_size - 1)) ? s.size : (dst_size - 1);
	if (s.buf) { memcpy(dst, s.buf, n); dst[n] = 0; } else { dst[0] = 0; }
	return n;
}

size_t cblu_docr_get_f64_array(CBLU_DocR* d, const char* key, double* out, size_t maxn) {
	if (!d || !key || !out || !maxn) return 0;
	FLValue v = FLDict_Get(d->props, fl_from_c(key));
	if (FLValue_GetType(v) != kFLArray) return 0;
	FLArray a = FLValue_AsArray(v);
	size_t n = FLArray_Count(a);
	if (n > maxn) n = maxn;
	for (size_t i = 0; i < n; i++) {
		FLValue item = FLArray_Get(a, (uint32_t)i);
		out[i] = fl_is_number(item) ? FLValue_AsDouble(item) : 0.0;
	}
	return n;
}

size_t cblu_docr_get_i64_array(CBLU_DocR* d, const char* key, int64_t* out, size_t maxn) {
	if (!d || !key || !out || !maxn) return 0;
	FLValue v = FLDict_Get(d->props, fl_from_c(key));
	if (FLValue_GetType(v) != kFLArray) return 0;
	FLArray a = FLValue_AsArray(v);
	size_t n = FLArray_Count(a);
	if (n > maxn) n = maxn;
	for (size_t i = 0; i < n; i++) {
		FLValue item = FLArray_Get(a, (uint32_t)i);
		out[i] = fl_is_number(item) ? (int64_t)FLValue_AsInt(item) : 0;
	}
	return n;
}

size_t cblu_docr_get_blob(CBLU_DocR* d, const char* key, void* dst, size_t dstSize,
						  char* contentTypeDst, size_t ctDstSize) {
	if (!d || !key || !dst || dstSize==0) return 0;
	const CBLBlob* blob = NULL;
	{
		FLValue v = FLDict_Get(d->props, fl_from_c(key));
		if (v) {
		#if defined(FLValue_AsBlob)
			blob = FLValue_AsBlob(v);
		#elif defined(FLValue_GetBlob)
			blob = FLValue_GetBlob(v);
		#else
			blob = NULL;
		#endif
		}
	}
	if (!blob) return 0;

	if (contentTypeDst && ctDstSize>0) {
		FLString ct = CBLBlob_ContentType(blob);
		size_t n = (ct.buf && ct.size < (ctDstSize-1)) ? ct.size : (ctDstSize-1);
		if (ct.buf) { memcpy(contentTypeDst, ct.buf, n); contentTypeDst[n]=0; } else { contentTypeDst[0]=0; }
	}

	CBLError err = {0};
	FLSliceResult sr = CBLBlob_Content(blob, &err);
	if (!sr.buf || sr.size == 0) { FLSliceResult_Release(sr); return 0; }
	size_t nCopy = (sr.size < dstSize) ? sr.size : dstSize;
	memcpy(dst, sr.buf, nCopy);
	FLSliceResult_Release(sr);
	return nCopy;
}

void cblu_docr_free(CBLU_DocR* d) {
	if (!d) return;
	if (d->doc) { CBLDocument_Release(d->doc); d->doc = NULL; }
	d->props = NULL;
	free(d);
}

bool cblu_open_collection(CBLU_Db* base, const char* scopeName, const char* collName, CBLU_Db** out_handle) {
	if (!base || !scopeName || !collName || !out_handle) return false;
	*out_handle = NULL;
	CBLError err = {0};

	// Get scope and collection
	const CBLScope* scope = CBLDatabase_Scope(base->core.db, fl_from_c(scopeName), &err);
	if (!scope) {
		// scope may not exist
		return false;
	}
	CBLCollection* coll = NULL;
#ifdef CBLScope_Collection
	coll = CBLScope_Collection(scope, fl_from_c(collName), &err);
#else
	// Older API exposes collection lookup on the database directly
	coll = CBLDatabase_Collection(base->core.db, fl_from_c(scopeName), fl_from_c(collName), &err);
#endif
	if (!coll) {
		return false;
	}

	// Create a lightweight handle bound to this collection
	CBLU_Db* h = (CBLU_Db*)calloc(1, sizeof *h);
	h->core.db   = base->core.db;   // share DB (owned by base)
	h->core.coll = coll;            // retained by API call
	*out_handle = h;
	return true;
}
