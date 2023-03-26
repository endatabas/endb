use std::time::Instant;

use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use chumsky::Parser as ChumskyParser;

// cargo run --release --example micro_bench

// (time (let ((acc))
//         (dotimes (n 100000)
//           (setq acc (endb/sql/parser::parse-sql "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b")))
//         acc))

fn main() {
    let iterations = 100000;

    let dialect = SQLiteDialect {};
    let sql = "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b";

    println!("sqlparser crate");
    let now = Instant::now();
    let mut result = Parser::parse_sql(&dialect, sql);
    for _ in 1..(iterations - 1) {
        result = Parser::parse_sql(&dialect, sql);
    }
    println!("Elapsed: {:?}", now.elapsed());
    println!("{:?}", result.unwrap());

    println!("chumsky based parser");
    let now = Instant::now();
    let mut result = endb::SQL_AST_PARSER_NO_ERRORS.with(|parser| parser.parse(sql));
    for _ in 1..(iterations - 1) {
        endb::SQL_AST_PARSER_NO_ERRORS.with(|parser| {
            result = parser.parse(sql);
        });
    }
    println!("Elapsed: {:?}", now.elapsed());
    println!("{:?}", result.into_output());
}
