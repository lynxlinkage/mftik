//! Increment apply and a top-of-book walk.
//!
//! E02-D004: report the numbers. No comparison with the Go bench and no pass
//! threshold.

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use mftik_core::book::{DEFAULT_N, Level, OrderBook};

fn seed_prices(n: usize) -> Vec<Level> {
    (0..n)
        .map(|i| Level {
            price: (10_000 - i) as f64,
            size: 1.0,
        })
        .collect()
}

fn increment(c: &mut Criterion) {
    let seed = seed_prices(1000);
    let mut inc = [
        Level {
            price: 10_000.0,
            size: 2.0,
        },
        Level {
            price: 9_999.0,
            size: 3.0,
        },
        Level {
            price: 9_990.0,
            size: 4.0,
        },
        Level {
            price: 9_600.0,
            size: 5.0,
        },
    ];
    let mut book = OrderBook::new(DEFAULT_N, 1.0).expect("book");
    book.apply(&seed, &[]);
    let mut i = 0_u64;
    c.bench_function("increment", |b| {
        b.iter(|| {
            inc[0].size = (i & 1) as f64 + 1.0;
            i = i.wrapping_add(1);
            book.apply(black_box(&inc), &[]);
            black_box(book.best_bid_tick());
        });
    });
}

fn top_n(c: &mut Criterion) {
    let seed = seed_prices(1000);
    let book = {
        let mut book = OrderBook::new(DEFAULT_N, 1.0).expect("book");
        book.apply(&seed, &[]);
        book
    };
    c.bench_function("top_n", |b| {
        b.iter(|| {
            let mut sink = 0_i64;
            for (tick, size) in black_box(&book).bids(20) {
                sink = sink.wrapping_add(tick).wrapping_add(size as i64);
            }
            black_box(sink);
        });
    });
}

fn config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50)
}

criterion_group! {
    name = book;
    config = config();
    targets = increment, top_n
}
criterion_main!(book);
