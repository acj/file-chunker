# file-chunker

This crate provides the `FileChunker` type, which is useful for efficiently reading a file
in (approximately) equally-sized parts.

The original use case was to process a log file in chunks, one thread per chunk, and to
guarantee that each chunk ended with a full line of text.

## Example

```rust,no_run
use file_chunker::FileChunker;
let file = std::fs::File::open("/path/to/file").unwrap();
let chunker = FileChunker::new(&file).unwrap();
chunker.chunks(1024, Some('\n'))
    .unwrap()
    .iter()
    .for_each(|chunk| {
        println!("{:?}", chunk);
    });
```
