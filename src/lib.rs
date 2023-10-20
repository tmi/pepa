use itertools::Itertools;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::reader::FileReader;
use parquet::file::metadata::ParquetMetaData;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use parquet::basic::Type;
use std::path::Path;

#[derive(Serialize, Debug)]
struct Shape {
    num_rows: i64,
    num_cols_leaf: usize,
}

fn shape(metadata: &ParquetMetaData) -> Shape {
    let schema = metadata.file_metadata().schema_descr();
    Shape {
        num_rows: metadata.file_metadata().num_rows(),
        num_cols_leaf: schema.num_columns(),
    }
}

#[derive(Serialize, Debug)]
struct SchemaFull <'a> {
    columns: HashMap<&'a str, String>,
}

fn schema_full<'a>(metadata: &'a ParquetMetaData) -> SchemaFull {
    let schema = metadata.file_metadata().schema_descr();
    SchemaFull {
        columns: schema.columns().iter().map(|x| (x.name(), x.physical_type().to_string())).collect(),
    }
}

#[derive(Serialize, Debug)]
struct SchemaBrief {
    column_type_counts: HashMap<String, usize>,
}

fn schema_brief(metadata: &ParquetMetaData) -> SchemaBrief {
    let schema = metadata.file_metadata().schema_descr();
    let column_types: Vec<(Type, u8)> = schema.columns().iter().map(|x| (x.physical_type(), 1)).collect();
    let column_type_counts = column_types.into_iter().into_group_map().into_iter().map(|(k, v)| (k.to_string(), v.len())).collect();
    SchemaBrief {
        column_type_counts: column_type_counts
    }
}

pub fn summarize_parquet_metadata(file_path: &Path, level: u8) -> String {
    // TODO error handling
    let reader = SerializedFileReader::try_from(file_path).unwrap();
    let metadata: &ParquetMetaData = reader.metadata();

    // TODO accept only the hashmap and function, derive the key from the function by stripping up
    // to the first _
    macro_rules! insert{ ($a: expr, $b: expr, $c: expr) => { $a.insert($b, serde_json::to_value(&$c).unwrap()) } }
    let mut result: HashMap<&str, Value> = HashMap::new();

    insert!(result, "shape", shape(metadata));
    if level == 0 {
        insert!(result, "schema", schema_brief(metadata));
    } else {
        insert!(result, "schema", schema_full(metadata));
    };

    serde_json::to_string_pretty(&result).unwrap()
}
