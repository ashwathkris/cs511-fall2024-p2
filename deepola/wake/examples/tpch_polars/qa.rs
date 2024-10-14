use wake::graph::*;
use wake::polars_operations::*;
use polars::prelude::*;
use std::collections::HashMap;
use crate::utils::{TableInput, build_csv_reader_node};

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Define the part table with its relevant columns
    let table_columns = HashMap::from([(
        "part".into(),
        vec![
            "p_retailprice", 
            "p_size", 
            "p_container", 
            "p_mfgr"
        ],
    )]);

    // CSVReaderNode to read the `part` table
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);

    // WHERE Node - Filter conditions
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // p_retailprice > 1000
            let price_filter = df.column("p_retailprice").unwrap().gt(1000).unwrap();

            let size_filter = df.column("p_size").unwrap()
                .i64() // Change to i64 to match the actual type
                .unwrap()
                .into_iter()
                .map(|opt_val| opt_val.map(|v| [5, 10, 15, 20].contains(&(v as i32)))) // Cast `v` to `i32` for comparison
                .collect::<BooleanChunked>();

            // p_container <> 'JUMBO JAR'
            let container_filter = df.column("p_container").unwrap()
                .utf8()
                .unwrap()
                .into_iter()
                .map(|opt_val| opt_val.map(|v| v != "JUMBO JAR"))
                .collect::<BooleanChunked>();

            // p_mfgr LIKE 'Manufacturer#1%'
            let mfgr_filter = df.column("p_mfgr").unwrap()
                .utf8()
                .unwrap()
                .into_iter()
                .map(|opt_val| opt_val.map(|s| s.starts_with("Manufacturer#1")))
                .collect::<BooleanChunked>();

            // Combine all filters (boolean operations on masks)
            let combined_filter = price_filter
                & size_filter
                & container_filter
                & mfgr_filter;

            // Apply the combined filter
            df.filter(&combined_filter).unwrap()
        })))
        .build();

    // Aggregation Node - Sum of p_retailprice
    let sum_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // let total_price = df.column("p_retailprice").unwrap().sum().unwrap();
            let total_price: f64 = df.column("p_retailprice").unwrap().sum().unwrap();
            DataFrame::new(vec![Series::new("total", &[total_price])]).unwrap()
        })))
        .build();

    // Connect the nodes: part_csvreader_node -> where_node -> sum_node
    where_node.subscribe_to_node(&part_csvreader_node, 0);
    sum_node.subscribe_to_node(&where_node, 0);

    // The output reader will receive the final result from the sum_node
    output_reader.subscribe_to_node(&sum_node, 0);

    // Add all the nodes to the execution service
    let mut service = ExecutionService::<DataFrame>::create();
    service.add(part_csvreader_node);
    service.add(where_node);
    service.add(sum_node);
    
    service
}