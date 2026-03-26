use automerge::{
    transaction::{CommitOptions, Transactable},
    ActorId, AutoCommit, Automerge, ObjType, ReadDoc, ScalarValue, ROOT,
};
use std::fs;
use std::path::Path;

fn main() {
    let out_dir = std::env::args()
        .nth(1)
        .expect("usage: fixture-gen <output-dir>");
    let out = Path::new(&out_dir);
    fs::create_dir_all(out).unwrap();

    gen_scalars(out);
    gen_nested_objects(out);
    gen_list_operations(out);
    gen_text(out);
    gen_counter(out);
    gen_concurrent_edits(out);
    gen_delete_operations(out);
    gen_multiple_changes(out);
    gen_empty_doc(out);
    gen_large_text(out);

    eprintln!("Generated all fixtures in {}", out.display());
}

fn commit(doc: &mut AutoCommit, msg: &str, time: i64) {
    doc.commit_with(CommitOptions::default().with_message(msg).with_time(time));
}

/// All scalar types in a root map.
fn gen_scalars(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);

    doc.put(ROOT, "str", "hello world").unwrap();
    doc.put(ROOT, "int", ScalarValue::Int(-42)).unwrap();
    doc.put(ROOT, "uint", ScalarValue::Uint(u64::MAX)).unwrap();
    doc.put(ROOT, "float", ScalarValue::F64(3.141592653589793))
        .unwrap();
    doc.put(ROOT, "true", ScalarValue::Boolean(true)).unwrap();
    doc.put(ROOT, "false", ScalarValue::Boolean(false)).unwrap();
    doc.put(ROOT, "null", ScalarValue::Null).unwrap();
    doc.put(
        ROOT,
        "bytes",
        ScalarValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    )
    .unwrap();
    doc.put(ROOT, "timestamp", ScalarValue::Timestamp(-1000000))
        .unwrap();
    doc.put(ROOT, "counter", ScalarValue::counter(100)).unwrap();
    doc.put(ROOT, "empty_str", "").unwrap();
    doc.put(ROOT, "zero_int", ScalarValue::Int(0)).unwrap();
    doc.put(ROOT, "zero_uint", ScalarValue::Uint(0)).unwrap();
    doc.put(ROOT, "zero_float", ScalarValue::F64(0.0)).unwrap();
    doc.put(ROOT, "unicode", "Hello 🌍 café résumé").unwrap();

    commit(&mut doc, "scalars", 1000);
    write_fixture(out, "scalars.automerge", &mut doc);
}

/// Nested maps and lists.
fn gen_nested_objects(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);

    let config = doc.put_object(ROOT, "config", ObjType::Map).unwrap();
    doc.put(&config, "debug", true).unwrap();
    doc.put(&config, "port", ScalarValue::Int(8080)).unwrap();

    let inner = doc.put_object(&config, "nested", ObjType::Map).unwrap();
    doc.put(&inner, "deep", ScalarValue::Int(42)).unwrap();

    let items = doc.put_object(ROOT, "items", ObjType::List).unwrap();
    doc.insert(&items, 0, "first").unwrap();
    doc.insert(&items, 1, ScalarValue::Int(2)).unwrap();
    doc.insert(&items, 2, true).unwrap();

    let nested_in_list = doc.insert_object(&items, 3, ObjType::Map).unwrap();
    doc.put(&nested_in_list, "key", "value").unwrap();

    let nested_list = doc.insert_object(&items, 4, ObjType::List).unwrap();
    doc.insert(&nested_list, 0, "sub-item").unwrap();

    commit(&mut doc, "nested", 2000);
    write_fixture(out, "nested_objects.automerge", &mut doc);
}

/// List insert, delete, splice operations.
fn gen_list_operations(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);

    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "a").unwrap();
    doc.insert(&list, 1, "b").unwrap();
    doc.insert(&list, 2, "c").unwrap();
    doc.insert(&list, 3, "d").unwrap();
    doc.insert(&list, 4, "e").unwrap();
    commit(&mut doc, "initial list", 1000);

    doc.delete(&list, 2).unwrap();
    commit(&mut doc, "delete c", 2000);

    doc.insert(&list, 1, "X").unwrap();
    commit(&mut doc, "insert X", 3000);

    doc.splice(&list, 2, 2, ["Y", "Z"].into_iter().map(|s| automerge::hydrate::Value::from(s)))
        .unwrap();
    commit(&mut doc, "splice", 4000);

    write_fixture(out, "list_operations.automerge", &mut doc);
}

/// Text object with splice operations.
fn gen_text(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);

    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello World").unwrap();
    commit(&mut doc, "initial text", 1000);

    doc.splice_text(&text, 5, 6, " Go").unwrap();
    commit(&mut doc, "edit text", 2000);

    write_fixture(out, "text.automerge", &mut doc);
}

/// Counter increment operations.
fn gen_counter(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);

    doc.put(ROOT, "count", ScalarValue::counter(0)).unwrap();
    commit(&mut doc, "init counter", 1000);

    doc.increment(ROOT, "count", 5).unwrap();
    commit(&mut doc, "inc 5", 2000);

    doc.increment(ROOT, "count", -2).unwrap();
    commit(&mut doc, "dec 2", 3000);

    doc.increment(ROOT, "count", 10).unwrap();
    commit(&mut doc, "inc 10", 4000);

    // Final value should be 0 + 5 - 2 + 10 = 13
    write_fixture(out, "counter.automerge", &mut doc);
}

/// Two actors making concurrent edits — produces conflicts.
fn gen_concurrent_edits(out: &Path) {
    let actor1 = ActorId::from(hex_to_bytes("1111111111111111aaaaaaaaaaaaaaaa"));
    let actor2 = ActorId::from(hex_to_bytes("2222222222222222bbbbbbbbbbbbbbbb"));

    let mut doc1 = AutoCommit::new().with_actor(actor1);
    doc1.put(ROOT, "x", ScalarValue::Int(1)).unwrap();
    doc1.put(ROOT, "shared", "initial").unwrap();
    commit(&mut doc1, "init", 1000);

    let mut doc2 = doc1.fork().with_actor(actor2);

    doc1.put(ROOT, "shared", "from-actor1").unwrap();
    doc1.put(ROOT, "only1", "hello").unwrap();
    commit(&mut doc1, "actor1 edit", 2000);

    doc2.put(ROOT, "shared", "from-actor2").unwrap();
    doc2.put(ROOT, "only2", "world").unwrap();
    commit(&mut doc2, "actor2 edit", 2000);

    doc1.merge(&mut doc2).unwrap();

    // Print the winner for reference
    let (val, _) = doc1.get(ROOT, "shared").unwrap().unwrap();
    eprintln!("  concurrent_edits: shared winner = {:?}", val);

    write_fixture(out, "concurrent_edits.automerge", &mut doc1);
}

/// Delete operations on maps and lists.
fn gen_delete_operations(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);

    doc.put(ROOT, "keep", "stays").unwrap();
    doc.put(ROOT, "remove", "goes away").unwrap();
    doc.put(ROOT, "also_remove", ScalarValue::Int(99)).unwrap();
    commit(&mut doc, "initial", 1000);

    doc.delete(ROOT, "remove").unwrap();
    doc.delete(ROOT, "also_remove").unwrap();
    commit(&mut doc, "delete keys", 2000);

    write_fixture(out, "delete_operations.automerge", &mut doc);
}

/// Multiple changes from the same actor.
fn gen_multiple_changes(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);

    for i in 0..10 {
        doc.put(ROOT, "step", ScalarValue::Int(i as i64)).unwrap();
        doc.put(ROOT, &format!("key_{}", i), ScalarValue::Int(i as i64))
            .unwrap();
        commit(&mut doc, &format!("step {}", i), 1000 + i * 100);
    }

    write_fixture(out, "multiple_changes.automerge", &mut doc);
}

/// Empty document.
fn gen_empty_doc(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);
    write_fixture(out, "empty.automerge", &mut doc);
}

/// Large text to stress test.
fn gen_large_text(out: &Path) {
    let actor = ActorId::from(hex_to_bytes("aabbccdd00112233aabbccdd00112233"));
    let mut doc = AutoCommit::new().with_actor(actor);

    let text = doc.put_object(ROOT, "content", ObjType::Text).unwrap();
    let msg = "The quick brown fox jumps over the lazy dog. ";
    for _ in 0..10 {
        let len = doc.length(&text);
        doc.splice_text(&text, len, 0, msg).unwrap();
    }
    commit(&mut doc, "large text", 1000);

    write_fixture(out, "large_text.automerge", &mut doc);
}

fn write_fixture(dir: &Path, name: &str, doc: &mut AutoCommit) {
    let bytes = doc.save();
    let path = dir.join(name);
    fs::write(&path, &bytes).unwrap();
    eprintln!("  {} ({} bytes)", name, bytes.len());

    // Verify round-trip
    let loaded = Automerge::load(&bytes).unwrap();
    let resaved = loaded.save();
    let reloaded = Automerge::load(&resaved).unwrap();
    assert_eq!(
        loaded.get_heads(),
        reloaded.get_heads(),
        "heads mismatch after round-trip for {}",
        name
    );
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}
