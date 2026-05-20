#pragma once

template <bool AllVisible, bool CheckSerializable>
pg_attribute_always_inline static int
PgYaapCollectPageTuples(HeapScanDesc scan,
						 Snapshot snapshot,
						 Page page,
						 Buffer buffer,
						 BlockNumber block,
						 int lines)
{
	Oid			relid = RelationGetRelid(scan->rs_base.rs_rd);
	int			ntup = 0;
	int			nvis = 0;

	Assert(IsMVCCSnapshot(snapshot));

	for (OffsetNumber lineoff = FirstOffsetNumber; lineoff <= lines; lineoff++)
	{
		ItemId		lpp = PageGetItemId(page, lineoff);
		HeapTupleData loctup;
		bool		valid;

		if (unlikely(!ItemIdIsNormal(lpp)))
			continue;

		loctup.t_data = (HeapTupleHeader) PageGetItem(page, lpp);
		loctup.t_len = ItemIdGetLength(lpp);
		loctup.t_tableOid = relid;
		ItemPointerSet(&(loctup.t_self), block, lineoff);

		if constexpr (AllVisible)
			valid = true;
		else
			valid = HeapTupleSatisfiesVisibility(&loctup, snapshot, buffer);

		if constexpr (CheckSerializable)
			HeapCheckForSerializableConflictOut(valid,
												scan->rs_base.rs_rd,
												&loctup,
												buffer,
												snapshot);

		if (valid)
		{
			scan->rs_vistuples[ntup] = lineoff;
			ntup++;
		}
	}

	Assert(ntup <= MaxHeapTuplesPerPage);
	nvis = ntup;

	return nvis;
}
