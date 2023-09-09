/*-------------------------------------------------------------------------
 *
 * pg_immutable.c
 *	  immutable table access method code
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2023, Pierre Forstmann
 *
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_immutable_handler);

/* Base structures for scans */
typedef struct ImmuTableScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */

	/* Add more fields here as needed by the AM. */
}			ImmuTableScanDescData;
typedef struct ImmuTableScanDescData *ImmuTableScanDesc;


/* ------------------------------------------------------------------------
 * Slot related callbacks for immutable  AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
immutable_slot_callbacks(Relation relation)
{
	/*
	 * Here you would most likely want to invent your own set of slot
	 * callbacks for your AM.
	 */
	return &TTSOpsMinimalTuple;
}

static TransactionId
immutable_index_delete_tuples(Relation rel,
							  TM_IndexDeleteOp *delstate)
{
	return InvalidTransactionId;
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for immutable AM.
 * ----------------------------------------------------------------------------
 */

static TM_Result
immutable_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					   Snapshot snapshot, Snapshot crosscheck, bool wait,
					   TM_FailureData *tmfd, bool changingPart)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}


static TM_Result
immutable_tuple_update(Relation relation, ItemPointer otid,
					   TupleTableSlot *slot, CommandId cid,
					   Snapshot snapshot, Snapshot crosscheck,
					   bool wait, TM_FailureData *tmfd,
					   LockTupleMode *lockmode,
					   bool *update_indexes)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static TM_Result
immutable_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					 LockWaitPolicy wait_policy, uint8 flags,
					 TM_FailureData *tmfd)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static void
immutable_relation_nontransactional_truncate(Relation rel)
{
	/* nothing to do */
}

static void
immutable_vacuum(Relation onerel, VacuumParams *params,
				 BufferAccessStrategy bstrategy)
{
	/* nothing to do ? */
}

/* ------------------------------------------------------------------------
 * definition of the immutable table access methods:
 * - with default heap access methods
 * - redefined for INSERT/UPDATE/DELETE/TRUNCATE 
 * NB: must use static structure otherwise compiler complains
 * "initializer element is not constant".
 * ------------------------------------------------------------------------
 */

static struct {
	TableAmRoutine immutable_methods;
} immutable_access_method_struct;

Datum
pg_immutable_handler(PG_FUNCTION_ARGS)
{
	TableAmRoutine *local = GetHeapamTableAmRoutine();

	memcpy(&immutable_access_method_struct.immutable_methods, local, sizeof(TableAmRoutine));
	immutable_access_method_struct.immutable_methods.tuple_update = immutable_tuple_update;
	immutable_access_method_struct.immutable_methods.tuple_delete = immutable_tuple_delete;
	immutable_access_method_struct.immutable_methods.relation_nontransactional_truncate = 
		immutable_relation_nontransactional_truncate;


	PG_RETURN_POINTER(&immutable_access_method_struct.immutable_methods);
}
