package geoarrow_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/geoarrow/geoarrow-go"
	"github.com/stretchr/testify/require"
)

func TestE2EWriteArrowToParquetGeometry(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rec := newFiveRowWKBRecord(t, mem, geoarrow.NewWKBType())
	defer rec.Release()

	path := writeRecordToParquetFile(t, mem, rec, "geometry.parquet")
	assertParquetLogicalType(t, path, schema.GeometryLogicalType{})
}

func TestE2EWriteArrowToParquetGeography(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rec := newFiveRowWKBRecord(t, mem, geoarrow.NewWKBType(geoarrow.WKBWithMetadata(geoarrow.Metadata{
		Edges: geoarrow.EdgeSpherical,
	})))
	defer rec.Release()

	path := writeRecordToParquetFile(t, mem, rec, "geography.parquet")
	assertParquetLogicalType(t, path, schema.GeographyLogicalType{
		Algorithm: schema.GeographyEdgeSpherical,
	})
}

func TestParquetLogicalTypeOnlyAppliesToWKB(t *testing.T) {
	_, ok := any(geoarrow.NewWKBType()).(pqarrow.ExtensionCustomParquetType)
	require.True(t, ok)

	_, ok = any(geoarrow.NewPointType()).(pqarrow.ExtensionCustomParquetType)
	require.False(t, ok)

	_, ok = any(geoarrow.NewPolygonType()).(pqarrow.ExtensionCustomParquetType)
	require.False(t, ok)
}

func newFiveRowWKBRecord(t *testing.T, mem memory.Allocator, typ *geoarrow.WKBType) arrow.RecordBatch {
	builder := typ.NewBuilder(mem).(*geoarrow.WKBBuilder)
	defer builder.Release()

	for range 5 {
		builder.Append(geoarrow.WKBBytes(testWKBPoint))
	}

	arr := builder.NewArray()
	defer arr.Release()

	sc := arrow.NewSchema([]arrow.Field{
		{Name: "geometry", Type: typ, Nullable: false},
	}, nil)

	return array.NewRecordBatch(sc, []arrow.Array{arr}, int64(arr.Len()))
}

func writeRecordToParquetFile(t *testing.T, mem memory.Allocator, rec arrow.Record, name string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	out, err := os.Create(path)
	require.NoError(t, err)

	props := parquet.NewWriterProperties(parquet.WithAllocator(mem))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))
	parquetSchema, err := pqarrow.ToParquet(rec.Schema(), props, arrowProps)
	require.NoError(t, err)

	writer, err := file.NewParquetWriterWithError(
		out,
		parquetSchema.Root(),
		file.WithWriterProps(props),
	)
	require.NoError(t, err)

	rowGroupWriter, err := writer.AppendRowGroupChecked()
	require.NoError(t, err)

	columnWriter, err := rowGroupWriter.NextColumn()
	require.NoError(t, err)

	writeContext := pqarrow.NewArrowWriteContext(context.Background(), &arrowProps)
	storage := rec.Column(0).(array.ExtensionArray).Storage()
	require.NoError(t, pqarrow.WriteArrowToColumn(writeContext, columnWriter, storage, nil, nil, false))
	require.NoError(t, columnWriter.Close())
	require.NoError(t, rowGroupWriter.Close())
	require.NoError(t, writer.Close())

	return path
}

func assertParquetLogicalType(t *testing.T, path string, want schema.LogicalType) {
	t.Helper()

	in, err := os.Open(path)
	require.NoError(t, err)
	defer in.Close()

	parquetReader, err := file.NewParquetReader(in)
	require.NoError(t, err)
	defer parquetReader.Close()

	require.EqualValues(t, 5, parquetReader.MetaData().NumRows)
	require.Equal(t, 1, parquetReader.MetaData().Schema.NumColumns())
	require.True(t, want.Equals(parquetReader.MetaData().Schema.Column(0).LogicalType()))
}
