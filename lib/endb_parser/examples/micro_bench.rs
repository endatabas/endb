use std::time::Instant;

use chumsky::Parser as ChumskyParser;

// cargo run --release --example micro_bench

fn main() {
    let iterations = 100000;

    let sql = "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b";

    println!("chumsky based parser");
    let now = Instant::now();
    let mut result = endb_parser::SQL_AST_PARSER_NO_ERRORS.with(|parser| parser.parse(sql));
    for _ in 1..(iterations - 1) {
        endb_parser::SQL_AST_PARSER_NO_ERRORS.with(|parser| {
            result = parser.parse(sql);
        });
    }
    println!("Elapsed: {:?}", now.elapsed());
    println!("{:?}", result.into_output());
}
