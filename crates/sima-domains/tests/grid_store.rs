//! A grid round-trips through a real content-addressed store as an opaque
//! snapshot object: the address the store returns equals the grid's content
//! id, and the bytes read back decode to a byte-identical grid.

use sima_domains::substrates::cellular::Grid;
use sima_store::Store;

#[test]
fn grid_round_trips_through_the_store() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let store = Store::open(dir.path()).expect("open store");

    // A 3x2 grid of 4 channels: distinct, non-trivial float values.
    let data: Vec<f32> = (0..3 * 2 * 4).map(|i| i as f32 * 0.5 - 3.0).collect();
    let grid = Grid::new(3, 2, 4, data).expect("build grid");

    // The store addresses the grid by exactly its content id.
    let hash = store.put(&grid.to_bytes()).expect("put grid");
    assert_eq!(hash, grid.content_id());

    // The bytes read back decode to a byte-identical grid.
    let bytes = store.get(&hash).expect("get grid");
    let restored = Grid::from_bytes(&bytes).expect("decode grid");
    assert_eq!(restored.to_bytes(), grid.to_bytes());
}
