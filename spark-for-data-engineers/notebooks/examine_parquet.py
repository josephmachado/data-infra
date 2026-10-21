import pyarrow.parquet as pq

def print_parquet_metadata(file_path):
    # Open the file
    parquet_file = pq.ParquetFile(file_path)
    
    # Print complete metadata
    print("=" * 80)
    print("METADATA")
    print("=" * 80)
    print(parquet_file.metadata)
    print()
    
    # Schema
    print("=" * 80)
    print("SCHEMA")
    print("=" * 80)
    print(parquet_file.schema)
    print()
    
    # Row group details
    print("=" * 80)
    print("ROW GROUPS")
    print("=" * 80)
    for i in range(parquet_file.num_row_groups):
        rg = parquet_file.metadata.row_group(i)
        print(f"\nRow Group {i}:")
        print(f"  Rows: {rg.num_rows:,}")
        print(f"  Total byte size: {rg.total_byte_size:,}")
        print(f"  Columns: {rg.num_columns}")
        
        # Column details
        for j in range(rg.num_columns):
            col = rg.column(j)
            print(f"\n  Column '{col.path_in_schema}':")
            print(f"    Type: {col.physical_type}")
            print(f"    Encodings: {col.encodings}")
            print(f"    Compression: {col.compression}")
            print(f"    Compressed size: {col.total_compressed_size:,} bytes")
            print(f"    Uncompressed size: {col.total_uncompressed_size:,} bytes")
            print(f"    Compression ratio: {col.total_uncompressed_size / col.total_compressed_size:.2f}x")
            
            # Statistics (if available)
            if col.statistics:
                stats = col.statistics
                print(f"    Statistics:")
                print(f"      Min: {stats.min}")
                print(f"      Max: {stats.max}")
                print(f"      Null count: {stats.null_count}")
                print(f"      Distinct count: {stats.distinct_count if stats.has_distinct_count else 'N/A'}")