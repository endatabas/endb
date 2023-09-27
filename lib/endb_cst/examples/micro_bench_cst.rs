use std::time::Instant;

// cargo run --release --example micro_bench_cst

fn main() {
    let iterations = 100000;

    let sql = "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b";

    println!("peg based parser");

    let parser = endb_cst::build_peg_parser(endb_cst::SQL_PEG, "sql_stmt_list", "\\s*").unwrap();
    let now = Instant::now();

    let mut state = endb_cst::ParseState::default();
    assert!(parser(sql, 0, &mut state).is_ok());
    let mut result = endb_cst::events_to_sexp(&sql, &state.events);

    for _ in 1..(iterations - 1) {
        let mut state = endb_cst::ParseState::default();
        assert!(parser(sql, 0, &mut state).is_ok());
        result = endb_cst::events_to_sexp(&sql, &state.events);
    }
    println!("Elapsed: {:?}", now.elapsed());
    println!("{:?}", result);
}
