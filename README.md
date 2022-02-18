# file-chunker

[<img alt="crates.io" src="https://img.shields.io/crates/v/file-chunker.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/file-chunker)
[<img alt="build status" src="https://img.shields.io/github/workflow/status/acj/file-chunker/CI/main?style=for-the-badge" height="20">](https://github.com/acj/file-chunker/actions?query=branch%3Amain)

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
