package index

import "gosearch/index/bkd"

// PointValues provides access to a BKD tree for a numeric point field.
type PointValues interface {
	PointTree() bkd.PointTree
	MinValue() int64
	MaxValue() int64
	Size() int
	DocCount() int
}

// bkdPointValues wraps a BKDReader to implement PointValues.
type bkdPointValues struct {
	reader *bkd.BKDReader
}

func (pv *bkdPointValues) PointTree() bkd.PointTree { return pv.reader.PointTree() }
func (pv *bkdPointValues) MinValue() int64          { return pv.reader.MinValue() }
func (pv *bkdPointValues) MaxValue() int64          { return pv.reader.MaxValue() }
func (pv *bkdPointValues) Size() int                { return pv.reader.NumPoints() }
func (pv *bkdPointValues) DocCount() int            { return pv.reader.DocCount() }
