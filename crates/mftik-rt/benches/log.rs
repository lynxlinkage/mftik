//! One JSON log line.
//!
//! E06-D010: report the numbers. No comparison and no pass threshold.

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use mftik_rt::log::{COMPONENT, Field, Level, Logger, SDK_VERSION, Style, Value};

fn json_line(c: &mut Criterion) {
    let logger = Logger {
        min: Level::Info as i8,
        style: Style::Json,
        no_color: true,
        component: COMPONENT.to_owned(),
        version: String::new(),
        commit: String::new(),
        sdk_version: SDK_VERSION.to_owned(),
        session_id: Some("s1".to_owned()),
    };
    let fields = [
        Field {
            name: "k".to_owned(),
            value: Value::Int(1),
        },
        Field {
            name: "name".to_owned(),
            value: Value::Str("abc".to_owned()),
        },
    ];
    let mut sink = Vec::with_capacity(512);
    c.bench_function("json_line", |b| {
        b.iter(|| {
            sink.clear();
            logger.write(
                Level::Info,
                "hello",
                black_box(&fields),
                black_box(1_700_000_000_123_456_789),
                &mut sink,
            );
            black_box(sink.len());
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
    name = log;
    config = config();
    targets = json_line
}
criterion_main!(log);
