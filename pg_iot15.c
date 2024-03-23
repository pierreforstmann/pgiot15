/*-------------------------------------------------------------------------
 *
 * pg_iot15.c
 *	  iot15 table access method code
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

/* 
 * From src/backend/access/heap/heapam_handler.c 
 * sed -n '22,46p' /home/pierre/.pgenv/src/postgresql-15.4/src/backend/access/heap/heapam_handler.c
 */
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/syncscan.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "executor/executor.h"
#include "nodes/parsenodes.h"
#include "utils/queryjumble.h"
#include "parser/analyze.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"

extern HeapTuple pgi_heap_getnext(TableScanDesc sscan, ScanDirection direction);

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

PG_FUNCTION_INFO_V1(pg_iot15_handler);

/* Base structures for scans */
typedef struct ImmuTableScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */

	/* Add more fields here as needed by the AM. */
}			ImmuTableScanDescData;
typedef struct ImmuTableScanDescData *ImmuTableScanDesc;


/* ------------------------------------------------------------------------
 * Slot related callbacks for iot15  AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
iot15_slot_callbacks(Relation relation)
{
	/*
	 * Here you would most likely want to invent your own set of slot
	 * callbacks for your AM.
	 */
	return &TTSOpsMinimalTuple;
}

static TransactionId
iot15_index_delete_tuples(Relation rel,
							  TM_IndexDeleteOp *delstate)
{
	return InvalidTransactionId;
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for iot15 AM.
 * ----------------------------------------------------------------------------
 */

static TM_Result
iot15_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
					   Snapshot snapshot, Snapshot crosscheck, bool wait,
					   TM_FailureData *tmfd, bool changingPart)
{
	ereport(ERROR, (errmsg("pg_iot15: iot15_tuple_delete: cannot DELETE from an iot15 table")));
}


static TM_Result
iot15_tuple_update(Relation relation, ItemPointer otid,
					   TupleTableSlot *slot, CommandId cid,
					   Snapshot snapshot, Snapshot crosscheck,
					   bool wait, TM_FailureData *tmfd,
					   LockTupleMode *lockmode,
					   bool *update_indexes)
{
	ereport(ERROR, (errmsg("pg_iot15: iot15_tuple_update: cannot UPDATE an iot15 table")));
}

static TM_Result
iot15_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					 LockWaitPolicy wait_policy, uint8 flags,
					 TM_FailureData *tmfd)
{
	ereport(ERROR, (errmsg("pg_iot15: iot15_tuple_lock: cannot lock an iot15 table row")));
}

static void
iot15_relation_nontransactional_truncate(Relation rel)
{
	/*
	 * only called in some cases by heap_truncate_one_rel.
	 * see ExecuteTruncateGuts in src/backend/commands/tablecmds.c 
	 * Otherwise table is truncated with:
	 * new empty storage file creation for the relation, and assigning it
	 * as the relfilenode value. The old storage file is scheduled for
	 * deletion at commit.
	 */
	ereport(ERROR, (errmsg("pg_iot15: cannot truncate an iot15 table")));
}

static void
iot15_vacuum(Relation onerel, VacuumParams *params,
				 BufferAccessStrategy bstrategy)
{
	ereport(ERROR, (errmsg("pg_iot15: iot15_vacuum: cannot vacuum an iot15 table")));
}

/* -----------------------------------------------------------------------------------------------------
 *
 * include some heapam_handler.c functions to bypass "ERROR: only heap AM is supported" from heap_getnext
 *
 * -----------------------------------------------------------------------------------------------------
 */


/*
 * From src/backend/access/heap/heapam_handler.c version 15.4
 * sed -n '1978,2015p' /home/pierre/.pgenv/src/postgresql-15.4/src/backend/access/heap/heapam_handler.c
 * renamed heap_getnext to pgi_heap_getnext
 */

/*
 * Return the number of blocks that have been read by this scan since
 * starting.  This is meant for progress reporting rather than be fully
 * accurate: in a parallel scan, workers can be concurrently reading blocks
 * further ahead than what we report.
 */
static BlockNumber
heapam_scan_get_blocks_done(HeapScanDesc hscan)
{
        ParallelBlockTableScanDesc bpscan = NULL;
        BlockNumber startblock;
        BlockNumber blocks_done;

        if (hscan->rs_base.rs_parallel != NULL)
        {
                bpscan = (ParallelBlockTableScanDesc) hscan->rs_base.rs_parallel;
                startblock = bpscan->phs_startblock;
        }
        else
                startblock = hscan->rs_startblock;

        /*
         * Might have wrapped around the end of the relation, if startblock was
         * not zero.
         */
        if (hscan->rs_cblock > startblock)
                blocks_done = hscan->rs_cblock - startblock;
        else
        {
                BlockNumber nblocks;

                nblocks = bpscan != NULL ? bpscan->phs_nblocks : hscan->rs_nblocks;
                blocks_done = nblocks - startblock +
                        hscan->rs_cblock;
        }

        return blocks_done;
}


/*
 * From 
 * src/backend/access/heap/heapam_handler.c version 15.4
 * using: 
 * sed -n '1158,1731p' /home/pierre/.pgenv/src/postgresql-15.4/src/backend/access/heap/heapam_handler.c > table_index_build_scan.c
 * function heapam_index_build_range_scan renamed to iot15_index_build_range_scan
 */

static double
iot15_index_build_range_scan(Relation heapRelation,
							  Relation indexRelation,
							  IndexInfo *indexInfo,
							  bool allow_sync,
							  bool anyvisible,
							  bool progress,
							  BlockNumber start_blockno,
							  BlockNumber numblocks,
							  IndexBuildCallback callback,
							  void *callback_state,
							  TableScanDesc scan)
{
	HeapScanDesc hscan;
	bool		is_system_catalog;
	bool		checking_uniqueness;
	HeapTuple	heapTuple;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;
	bool		need_unregister_snapshot = false;
	TransactionId OldestXmin;
	BlockNumber previous_blkno = InvalidBlockNumber;
	BlockNumber root_blkno = InvalidBlockNumber;
	OffsetNumber root_offsets[MaxHeapTuplesPerPage];

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/* Remember if it's a system catalog */
	is_system_catalog = IsSystemRelation(heapRelation);

	/* See whether we're verifying uniqueness/exclusion properties */
	checking_uniqueness = (indexInfo->ii_Unique ||
						   indexInfo->ii_ExclusionOps != NULL);

	/*
	 * "Any visible" mode is not compatible with uniqueness checks; make sure
	 * only one of those is requested.
	 */
	Assert(!(anyvisible && checking_uniqueness));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(heapRelation, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples). In a
	 * concurrent build, or during bootstrap, we take a regular MVCC snapshot
	 * and index whatever's live according to that.
	 */
	OldestXmin = InvalidTransactionId;

	/* okay to ignore lazy VACUUMs here */
	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
		OldestXmin = GetOldestNonRemovableTransactionId(heapRelation);

	if (!scan)
	{
		/*
		 * Serial index build.
		 *
		 * Must begin our own heap scan in this case.  We may also need to
		 * register a snapshot whose lifetime is under our direct control.
		 */
		if (!TransactionIdIsValid(OldestXmin))
		{
			snapshot = RegisterSnapshot(GetTransactionSnapshot());
			need_unregister_snapshot = true;
		}
		else
			snapshot = SnapshotAny;

		scan = table_beginscan_strat(heapRelation,	/* relation */
									 snapshot,	/* snapshot */
									 0, /* number of keys */
									 NULL,	/* scan key */
									 true,	/* buffer access strategy OK */
									 allow_sync);	/* syncscan OK? */
	}
	else
	{
		/*
		 * Parallel index build.
		 *
		 * Parallel case never registers/unregisters own snapshot.  Snapshot
		 * is taken from parallel heap scan, and is SnapshotAny or an MVCC
		 * snapshot, based on same criteria as serial case.
		 */
		Assert(!IsBootstrapProcessingMode());
		Assert(allow_sync);
		snapshot = scan->rs_snapshot;
	}

	hscan = (HeapScanDesc) scan;

	/*
	 * Must have called GetOldestNonRemovableTransactionId() if using
	 * SnapshotAny.  Shouldn't have for an MVCC snapshot. (It's especially
	 * worth checking this for parallel builds, since ambuild routines that
	 * support parallel builds must work these details out for themselves.)
	 */
	Assert(snapshot == SnapshotAny || IsMVCCSnapshot(snapshot));
	Assert(snapshot == SnapshotAny ? TransactionIdIsValid(OldestXmin) :
		   !TransactionIdIsValid(OldestXmin));
	Assert(snapshot == SnapshotAny || !anyvisible);

	/* Publish number of blocks to scan */
	if (progress)
	{
		BlockNumber nblocks;

		if (hscan->rs_base.rs_parallel != NULL)
		{
			ParallelBlockTableScanDesc pbscan;

			pbscan = (ParallelBlockTableScanDesc) hscan->rs_base.rs_parallel;
			nblocks = pbscan->phs_nblocks;
		}
		else
			nblocks = hscan->rs_nblocks;

		pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL,
									 nblocks);
	}

	/* set our scan endpoints */
	if (!allow_sync)
		heap_setscanlimits(scan, start_blockno, numblocks);
	else
	{
		/* syncscan can only be requested on whole relation */
		Assert(start_blockno == 0);
		Assert(numblocks == InvalidBlockNumber);
	}

	reltuples = 0;

	/*
	 * Scan all tuples in the base relation.
	 */
	while ((heapTuple = pgi_heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool		tupleIsAlive;

		CHECK_FOR_INTERRUPTS();

		/* Report scan progress, if asked to. */
		if (progress)
		{
			BlockNumber blocks_done = heapam_scan_get_blocks_done(hscan);

			if (blocks_done != previous_blkno)
			{
				pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
											 blocks_done);
				previous_blkno = blocks_done;
			}
		}

		/*
		 * When dealing with a HOT-chain of updated tuples, we want to index
		 * the values of the live tuple (if any), but index it under the TID
		 * of the chain's root tuple.  This approach is necessary to preserve
		 * the HOT-chain structure in the heap. So we need to be able to find
		 * the root item offset for every tuple that's in a HOT-chain.  When
		 * first reaching a new page of the relation, call
		 * heap_get_root_tuples() to build a map of root item offsets on the
		 * page.
		 *
		 * It might look unsafe to use this information across buffer
		 * lock/unlock.  However, we hold ShareLock on the table so no
		 * ordinary insert/update/delete should occur; and we hold pin on the
		 * buffer continuously while visiting the page, so no pruning
		 * operation can occur either.
		 *
		 * In cases with only ShareUpdateExclusiveLock on the table, it's
		 * possible for some HOT tuples to appear that we didn't know about
		 * when we first read the page.  To handle that case, we re-obtain the
		 * list of root offsets when a HOT tuple points to a root item that we
		 * don't know about.
		 *
		 * Also, although our opinions about tuple liveness could change while
		 * we scan the page (due to concurrent transaction commits/aborts),
		 * the chain root locations won't, so this info doesn't need to be
		 * rebuilt after waiting for another transaction.
		 *
		 * Note the implied assumption that there is no more than one live
		 * tuple per HOT-chain --- else we could create more than one index
		 * entry pointing to the same root tuple.
		 */
		if (hscan->rs_cblock != root_blkno)
		{
			Page		page = BufferGetPage(hscan->rs_cbuf);

			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
			heap_get_root_tuples(page, root_offsets);
			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			root_blkno = hscan->rs_cblock;
		}

		if (snapshot == SnapshotAny)
		{
			/* do our own time qual check */
			bool		indexIt;
			TransactionId xwait;

	recheck:

			/*
			 * We could possibly get away with not locking the buffer here,
			 * since caller should hold ShareLock on the relation, but let's
			 * be conservative about it.  (This remark is still correct even
			 * with HOT-pruning: our pin on the buffer prevents pruning.)
			 */
			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);

			/*
			 * The criteria for counting a tuple as live in this block need to
			 * match what analyze.c's heapam_scan_analyze_next_tuple() does,
			 * otherwise CREATE INDEX and ANALYZE may produce wildly different
			 * reltuples values, e.g. when there are many recently-dead
			 * tuples.
			 */
			switch (HeapTupleSatisfiesVacuum(heapTuple, OldestXmin,
											 hscan->rs_cbuf))
			{
				case HEAPTUPLE_DEAD:
					/* Definitely dead, we can ignore it */
					indexIt = false;
					tupleIsAlive = false;
					break;
				case HEAPTUPLE_LIVE:
					/* Normal case, index and unique-check it */
					indexIt = true;
					tupleIsAlive = true;
					/* Count it as live, too */
					reltuples += 1;
					break;
				case HEAPTUPLE_RECENTLY_DEAD:

					/*
					 * If tuple is recently deleted then we must index it
					 * anyway to preserve MVCC semantics.  (Pre-existing
					 * transactions could try to use the index after we finish
					 * building it, and may need to see such tuples.)
					 *
					 * However, if it was HOT-updated then we must only index
					 * the live tuple at the end of the HOT-chain.  Since this
					 * breaks semantics for pre-existing snapshots, mark the
					 * index as unusable for them.
					 *
					 * We don't count recently-dead tuples in reltuples, even
					 * if we index them; see heapam_scan_analyze_next_tuple().
					 */
					if (HeapTupleIsHotUpdated(heapTuple))
					{
						indexIt = false;
						/* mark the index as unsafe for old snapshots */
						indexInfo->ii_BrokenHotChain = true;
					}
					else
						indexIt = true;
					/* In any case, exclude the tuple from unique-checking */
					tupleIsAlive = false;
					break;
				case HEAPTUPLE_INSERT_IN_PROGRESS:

					/*
					 * In "anyvisible" mode, this tuple is visible and we
					 * don't need any further checks.
					 */
					if (anyvisible)
					{
						indexIt = true;
						tupleIsAlive = true;
						reltuples += 1;
						break;
					}

					/*
					 * Since caller should hold ShareLock or better, normally
					 * the only way to see this is if it was inserted earlier
					 * in our own transaction.  However, it can happen in
					 * system catalogs, since we tend to release write lock
					 * before commit there.  Give a warning if neither case
					 * applies.
					 */
					xwait = HeapTupleHeaderGetXmin(heapTuple->t_data);
					if (!TransactionIdIsCurrentTransactionId(xwait))
					{
						if (!is_system_catalog)
							elog(WARNING, "concurrent insert in progress within table \"%s\"",
								 RelationGetRelationName(heapRelation));

						/*
						 * If we are performing uniqueness checks, indexing
						 * such a tuple could lead to a bogus uniqueness
						 * failure.  In that case we wait for the inserting
						 * transaction to finish and check again.
						 */
						if (checking_uniqueness)
						{
							/*
							 * Must drop the lock on the buffer before we wait
							 */
							LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
							XactLockTableWait(xwait, heapRelation,
											  &heapTuple->t_self,
											  XLTW_InsertIndexUnique);
							CHECK_FOR_INTERRUPTS();
							goto recheck;
						}
					}
					else
					{
						/*
						 * For consistency with
						 * heapam_scan_analyze_next_tuple(), count
						 * HEAPTUPLE_INSERT_IN_PROGRESS tuples as live only
						 * when inserted by our own transaction.
						 */
						reltuples += 1;
					}

					/*
					 * We must index such tuples, since if the index build
					 * commits then they're good.
					 */
					indexIt = true;
					tupleIsAlive = true;
					break;
				case HEAPTUPLE_DELETE_IN_PROGRESS:

					/*
					 * As with INSERT_IN_PROGRESS case, this is unexpected
					 * unless it's our own deletion or a system catalog; but
					 * in anyvisible mode, this tuple is visible.
					 */
					if (anyvisible)
					{
						indexIt = true;
						tupleIsAlive = false;
						reltuples += 1;
						break;
					}

					xwait = HeapTupleHeaderGetUpdateXid(heapTuple->t_data);
					if (!TransactionIdIsCurrentTransactionId(xwait))
					{
						if (!is_system_catalog)
							elog(WARNING, "concurrent delete in progress within table \"%s\"",
								 RelationGetRelationName(heapRelation));

						/*
						 * If we are performing uniqueness checks, assuming
						 * the tuple is dead could lead to missing a
						 * uniqueness violation.  In that case we wait for the
						 * deleting transaction to finish and check again.
						 *
						 * Also, if it's a HOT-updated tuple, we should not
						 * index it but rather the live tuple at the end of
						 * the HOT-chain.  However, the deleting transaction
						 * could abort, possibly leaving this tuple as live
						 * after all, in which case it has to be indexed. The
						 * only way to know what to do is to wait for the
						 * deleting transaction to finish and check again.
						 */
						if (checking_uniqueness ||
							HeapTupleIsHotUpdated(heapTuple))
						{
							/*
							 * Must drop the lock on the buffer before we wait
							 */
							LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
							XactLockTableWait(xwait, heapRelation,
											  &heapTuple->t_self,
											  XLTW_InsertIndexUnique);
							CHECK_FOR_INTERRUPTS();
							goto recheck;
						}

						/*
						 * Otherwise index it but don't check for uniqueness,
						 * the same as a RECENTLY_DEAD tuple.
						 */
						indexIt = true;

						/*
						 * Count HEAPTUPLE_DELETE_IN_PROGRESS tuples as live,
						 * if they were not deleted by the current
						 * transaction.  That's what
						 * heapam_scan_analyze_next_tuple() does, and we want
						 * the behavior to be consistent.
						 */
						reltuples += 1;
					}
					else if (HeapTupleIsHotUpdated(heapTuple))
					{
						/*
						 * It's a HOT-updated tuple deleted by our own xact.
						 * We can assume the deletion will commit (else the
						 * index contents don't matter), so treat the same as
						 * RECENTLY_DEAD HOT-updated tuples.
						 */
						indexIt = false;
						/* mark the index as unsafe for old snapshots */
						indexInfo->ii_BrokenHotChain = true;
					}
					else
					{
						/*
						 * It's a regular tuple deleted by our own xact. Index
						 * it, but don't check for uniqueness nor count in
						 * reltuples, the same as a RECENTLY_DEAD tuple.
						 */
						indexIt = true;
					}
					/* In any case, exclude the tuple from unique-checking */
					tupleIsAlive = false;
					break;
				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					indexIt = tupleIsAlive = false; /* keep compiler quiet */
					break;
			}

			LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			if (!indexIt)
				continue;
		}
		else
		{
			/* heap_getnext did the time qual check */
			tupleIsAlive = true;
			reltuples += 1;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/* Set up for predicate or expression evaluation */
		ExecStoreBufferHeapTuple(heapTuple, slot, hscan->rs_cbuf);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * For the current heap tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/*
		 * You'd think we should go ahead and build the index tuple here, but
		 * some index AMs want to do further processing on the data first.  So
		 * pass the values[] and isnull[] arrays, instead.
		 */

		if (HeapTupleIsHeapOnly(heapTuple))
		{
			/*
			 * For a heap-only tuple, pretend its TID is that of the root. See
			 * src/backend/access/heap/README.HOT for discussion.
			 */
			ItemPointerData tid;
			OffsetNumber offnum;

			offnum = ItemPointerGetOffsetNumber(&heapTuple->t_self);

			/*
			 * If a HOT tuple points to a root that we don't know about,
			 * obtain root items afresh.  If that still fails, report it as
			 * corruption.
			 */
			if (root_offsets[offnum - 1] == InvalidOffsetNumber)
			{
				Page		page = BufferGetPage(hscan->rs_cbuf);

				LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
				heap_get_root_tuples(page, root_offsets);
				LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
			}

			if (!OffsetNumberIsValid(root_offsets[offnum - 1]))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("failed to find parent tuple for heap-only tuple at (%u,%u) in table \"%s\"",
										 ItemPointerGetBlockNumber(&heapTuple->t_self),
										 offnum,
										 RelationGetRelationName(heapRelation))));

			ItemPointerSet(&tid, ItemPointerGetBlockNumber(&heapTuple->t_self),
						   root_offsets[offnum - 1]);

			/* Call the AM's callback routine to process the tuple */
			callback(indexRelation, &tid, values, isnull, tupleIsAlive,
					 callback_state);
		}
		else
		{
			/* Call the AM's callback routine to process the tuple */
			callback(indexRelation, &heapTuple->t_self, values, isnull,
					 tupleIsAlive, callback_state);
		}
	}

	/* Report scan progress one last time. */
	if (progress)
	{
		BlockNumber blks_done;

		if (hscan->rs_base.rs_parallel != NULL)
		{
			ParallelBlockTableScanDesc pbscan;

			pbscan = (ParallelBlockTableScanDesc) hscan->rs_base.rs_parallel;
			blks_done = pbscan->phs_nblocks;
		}
		else
			blks_done = hscan->rs_nblocks;

		pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
									 blks_done);
	}

	table_endscan(scan);

	/* we can now forget our snapshot, if set and registered by us */
	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}

/* ------------------------------------------------------------------------
 * Definition of the iot15 table access methods:
 * - with default heap access methods
 * - redefined for UPDATE/DELETE/TRUNCATE 
 * NB: must use static structure otherwise compiler complains
 * "initializer element is not constant".
 * ------------------------------------------------------------------------
 */

static struct {
	TableAmRoutine iot15_methods;
} iot15_access_method_struct;

Datum
pg_iot15_handler(PG_FUNCTION_ARGS)
{
	TableAmRoutine *local = GetHeapamTableAmRoutine();

	memcpy(&iot15_access_method_struct.iot15_methods, local, sizeof(TableAmRoutine));
	iot15_access_method_struct.iot15_methods.tuple_update = iot15_tuple_update;
	iot15_access_method_struct.iot15_methods.tuple_delete = iot15_tuple_delete;
	iot15_access_method_struct.iot15_methods.relation_vacuum = iot15_vacuum;
	iot15_access_method_struct.iot15_methods.tuple_lock = iot15_tuple_lock;
	iot15_access_method_struct.iot15_methods.relation_nontransactional_truncate = 
		iot15_relation_nontransactional_truncate;
	iot15_access_method_struct.iot15_methods.index_build_range_scan =
	       iot15_index_build_range_scan;


	PG_RETURN_POINTER(&iot15_access_method_struct.iot15_methods);
}

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static void pg_iot15_parse(ParseState *pstate, Query *query, JumbleState *jstate);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* 
	 * Install hook 
	 */
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
 	post_parse_analyze_hook = pg_iot15_parse;
}

/*
 *  Module unload callback
 */
void
_PG_fini(void)
{
	
	/* 
	 * Uninstall hooks 
	 */
	post_parse_analyze_hook = prev_post_parse_analyze_hook;

}

static void pg_iot15_parse(ParseState *pstate, Query *query, JumbleState *jstate)
{
	Node *parsetree = query->utilityStmt;
	LOCKMODE lockmode;
	Oid relid;
	
	StringInfoData buf_select;	
	SPITupleTable *tuptable;
	TupleDesc tupdesc;
	int ret;
	int nr;
	Oid relam;
	bool isnull;
	Oid iot15_am;

	if (query->commandType == CMD_UTILITY && nodeTag(query->utilityStmt) == T_AlterTableStmt)
	{
		elog(LOG, "query=%s", pstate->p_sourcetext);
		/*
		 *  see ProcessUtilitySlow in ./src/backend/tcop/utility.c 
		 */
		lockmode = NoLock;
		relid = AlterTableLookupRelation((AlterTableStmt *)parsetree, lockmode);
		elog(LOG, "pg_iot15_parse: relid=%d", relid);

		/*
		 * check that relation AM is *not* pg_iot15:
		 *
		 * select oid into v_oid from pg_am where amname='pg_iot15'
		 * if v_relam = v_oid : must trigger error "cannot ALTER TABLE for iot15 AM"
		*/	

		/*
		 * to avoid: "ERROR:  cannot execute SQL without an outer snapshot or portal"
		*/
		PushActiveSnapshot(GetTransactionSnapshot());

		SPI_connect();

		/*
		 * get relation access method id.
		 */
		initStringInfo(&buf_select);
		appendStringInfo(&buf_select, 
				     "select relam from pg_class where oid = '%d'", relid);
		ret = SPI_execute(buf_select.data, false, 0);
		if (ret != SPI_OK_SELECT)
			elog(FATAL, "cannot select from pg_class for relid: %d  error code: %d", relid, ret);
		nr = SPI_processed;
		if (nr == 0)
			elog(FATAL, "relid %d not found in pg_class", relid);

		tuptable = SPI_tuptable;
		tupdesc = tuptable->tupdesc;
		relam = DatumGetInt32(SPI_getbinval(tuptable->vals[0], tupdesc, 1, &isnull));	
		elog(LOG, "pg_iot15_parse: relam=%d", relam);

		/*
		 * get iot15 access method id.
		*/
		initStringInfo(&buf_select);
		appendStringInfo(&buf_select, 
				     "select oid from pg_am where amname='pg_iot15'");
		ret = SPI_execute(buf_select.data, false, 0);
		if (ret != SPI_OK_SELECT)
			elog(FATAL, "cannot select from pg_am error code: %d", ret);
		nr = SPI_processed;
		if (nr == 0)
			elog(FATAL, "am 'pg_iot15' not found in pg_am");

		tuptable = SPI_tuptable;
		tupdesc = tuptable->tupdesc;
		iot15_am = DatumGetInt32(SPI_getbinval(tuptable->vals[0], tupdesc, 1, &isnull));	
		elog(LOG, "pg_iot15_parse: iot15_am=%d", iot15_am);

		SPI_finish();

		if (relam == iot15_am)
			elog(ERROR, "Cannot change access method for iot15 am");
	}
}
