use std::io::{self, BufRead, Write};

fn main() {
    endb_cst::SQL_CST_PARSER.with(|parser| loop {
        print!(">>> ");
        io::stdout().flush().ok();
        let stdin = io::stdin();
        let s = stdin.lock().lines().next();
        match s {
            Some(Ok(src)) => {
                if src.trim().is_empty() {
                    continue;
                }
                let mut state = endb_cst::ParseState::default();
                match parser(&src, 0, &mut state) {
                    Ok(_) => {
                        println!("{}", endb_cst::events_to_sexp(&src, &state.events).unwrap());
                    }
                    Err(_) => {
                        eprintln!(
                            "{}",
                            endb_cst::parse_errors_to_string(
                                "/sql",
                                &src,
                                &endb_cst::events_to_errors(&state.errors)
                            )
                            .unwrap()
                        );
                    }
                }
            }
            _ => break,
        }
    });
}
