use std::io::{self, BufRead, Write};

fn main() {
    loop {
        print!(">>> ");
        io::stdout().flush().ok();
        let stdin = io::stdin();
        let s = stdin.lock().lines().next();
        match s {
            Some(Ok(src)) => {
                if src.trim().is_empty() {
                    continue;
                }
                let mut state = endb_cst::ParseState {
                    track_errors: true,
                    ..endb_cst::ParseState::default()
                };
                match endb_cst::sql::sql_stmt_list(&src, 0, &mut state) {
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
    }
}
