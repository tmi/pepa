use itertools::Itertools;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::reader::FileReader;
use parquet::file::metadata::ParquetMetaData;
use serde::Serialize;
use serde_json::{Value, Map};
use std::collections::HashMap;
use parquet::basic::Type;
use std::path::Path;
use std::fs;

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

#[derive(Serialize, Debug)]
struct Pandas {
    version: String,
    library: String,
}


#[derive(Serialize, Debug)]
struct Creator <'a> {
    created_by: &'a str,
    metadata_version: i32,
    pandas: Option<Pandas>,
}

fn creator<'a>(metadata: &'a ParquetMetaData) -> Creator {
    let file_metadata = metadata.file_metadata();
    // TODO improve the None=>None etc
    let pandas_raw: Option<&String> = match file_metadata.key_value_metadata() {
        Some(vec_kv) => {
            let candidate = vec_kv.iter().filter(|x| (x.key == "pandas")).next();
            match candidate {
                Some(kv) => {
                    match &kv.value {
                        Some(x) => Some(x),
                        None => None,
                    }
                },
                None => None,
            }
        },
        None => None,
    };
    // TODO handle fails of json matchings
    let pandas: Option<Pandas> = match pandas_raw {
        Some(s) => {
            let j: Value = serde_json::from_str(s).unwrap();
            Some(Pandas {
                version: j["pandas_version"].as_str().unwrap().to_string(),
                library: j["creator"]["library"].as_str().unwrap().to_string() + "-" + j["creator"]["version"].as_str().unwrap(), 
            })
        },
        None => None,
    };
    Creator {
        created_by: file_metadata.created_by().unwrap_or("unknown"),
        metadata_version: file_metadata.version(),
        pandas: pandas,
    }
}

pub fn summarize_single_parquet(file_path: &Path, level: u8, result: &mut Map<String, Value>) -> () {
    // TODO error handling
    let reader = SerializedFileReader::try_from(file_path).unwrap();
    let metadata: &ParquetMetaData = reader.metadata();

    // TODO accept only the hashmap and function, derive the key from the function by stripping up
    // to the first _
    macro_rules! insert{ ($a: expr, $b: expr, $c: expr) => { $a.insert($b.to_string(), serde_json::to_value(&$c).unwrap()) } }

    insert!(result, "shape", shape(metadata));
    insert!(result, "creator", creator(metadata));
    if level == 0 {
        insert!(result, "schema", schema_brief(metadata));
    } else {
        insert!(result, "schema", schema_full(metadata));
    };
}

pub fn summarize_parquet_metadata(file_path: &Path, level: u8, result: &mut Map<String, Value>) -> () {
    if file_path.is_file() {
        summarize_single_parquet(file_path, level, result);
    } else if file_path.is_dir() {
        let listing = fs::read_dir(file_path).unwrap();
        for entry in listing {
            let entry_u = entry.unwrap();
            let fname = entry_u.file_name().into_string().unwrap();
            // TODO maybe use file_type() ?
            if fname.ends_with(".parquet") || fname.ends_with(".pq") {
                let mut subresult: Map<String, Value> = Map::new();
                summarize_single_parquet(&entry_u.path(), level, &mut subresult);
                let subresult_v: Value = subresult.into();
                result.insert(fname, subresult_v);
            }
            // TODO recursive call here?
        }
    } else {
        eprintln!("neither dir nor file, dear sire! {}", file_path.display());
        panic!("crash"); // TODO change return type to Result, return Error here instead
    }
}
