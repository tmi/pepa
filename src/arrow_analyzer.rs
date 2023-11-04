/// Module responsible for arrow-based analytics -- does not just skim metadata but actually reads
/// all the file bytes
use serde_json::{Value, Map};
use std::path::Path;
use serde::Serialize;
use std::fs::File;
use parquet::arrow::arrow_reader::*;
use std::collections::HashMap;

#[derive(Serialize, Debug)]
struct ColumnStats {
    nulls: usize,
    non_nulls: usize,
}

/// Inserts into &result a structure for every column, with values:
///  - null values,
///  - non-null values.
///  Implemented via batch reading into arrow and manually processing.
pub fn columnar_stats(file_path: &Path, result: &mut Map<String, Value>) -> () {
    let file = File::open(file_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();

    let mut accumulator: HashMap<String, ColumnStats> = HashMap::new();

    for batch_r in reader.into_iter() {
        let batch = batch_r.unwrap();
        for field in batch.schema().fields() {
            let key = field.name().to_string();
            if !accumulator.contains_key(&key) {
                accumulator.insert(key.clone(), ColumnStats{nulls: 0, non_nulls: 0});
            }
            let current = accumulator.get_mut(&key).unwrap();
            let column = batch.column_by_name(field.name()).unwrap();
            let null_count = column.null_count();
            current.nulls += null_count;
            current.non_nulls += column.len() - null_count;
        }
        // +batch.get_array_memory_size(); // total memory
        // +1 // batch counter
    }
    println!("{:?}", accumulator);
    result.insert("columnar_stats".to_string(), serde_json::to_value(&accumulator).unwrap());
}
