//! This crate provides the `FileChunker` type, which is useful for efficiently reading a file
//! in (approximately) equally-sized parts.
//!
//! The original use case was to process a log file in chunks, one thread per chunk, and to
//! guarantee that each chunk ended with a full line of text.
//!
//! ## Example
//!
//! ```rust,no_run
//! use file_chunker::FileChunker;
//!
//! let file = std::fs::File::open("/path/to/file").unwrap();
//! let chunker = FileChunker::new(&file).unwrap();
//! chunker.chunks(1024, Some('\n'))
//!     .unwrap()
//!     .iter()
//!     .for_each(|chunk| {
//!         println!("{:?}", chunk);
//!     });
//! ```
//!

use anyhow::Result;
use memmap2::Mmap;
use std::fs::File;

pub struct FileChunker {
    mmap: Mmap,
}

impl FileChunker {
    /// Create a new FileChunker
    pub fn new(file: &File) -> Result<Self> {
        let mmap = unsafe { Mmap::map(file)? };
        Ok(Self { mmap })
    }

    /// Divide the file into chunks approximately equal size. Returns a vector of memory-mapped
    /// slices that each correspond to a chunk.
    ///
    /// If a delimeter is provided, then each chunk will end with an instance of the delimeter,
    /// assuming the delimiter exists in the file. This is useful when working with text files
    /// that have newline characters, for example. If no delimeter is provided, then each chunk
    /// will be the same size, except for the last chunk which may be smaller.
    ///
    /// It is assumed that the underlying `File` will not change while this function is running.
    pub fn chunks(&self, count: usize, delimiter: Option<char>) -> Result<Vec<&[u8]>> {
        let chunk_size = chunk_size(self.mmap.len(), count);
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < self.mmap.len() {
            let mut chunk_end = offset + chunk_size;
            if chunk_end > self.mmap.len() {
                chunks.push(&self.mmap[offset..]);
                break;
            }
            if let Some(delimiter) = delimiter {
                while (chunk_end < self.mmap.len() - 1) && (self.mmap[chunk_end] != delimiter as u8)
                {
                    chunk_end += 1;
                }
                chunk_end += 1;
            }
            chunks.push(&self.mmap[offset..chunk_end]);
            offset = chunk_end;
        }

        Ok(chunks)
    }
}

fn chunk_size(file_size: usize, count: usize) -> usize {
    f64::ceil(file_size as f64 / count as f64) as usize
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Write;

    #[test]
    fn chunks_with_delimiter() {
        let log = "01\n23\n45\n67\n89";

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(2, Some('\n')).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
        assert_eq!(String::from_utf8_lossy(chunks[0]), "01\n23\n45\n");
        assert_eq!(String::from_utf8_lossy(chunks[1]), "67\n89");
    }

    #[test]
    fn chunks_without_delimiter() {
        let log = "0123456789";

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(10, None).unwrap();
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
        assert_eq!(chunks.len(), 10);
        (0..9).into_iter().for_each(|i| {
            assert_eq!(String::from_utf8_lossy(chunks[i]), format!("{}", i));
        });
    }

    #[test]
    fn chunks_with_delimiter_empty() {
        let log = "";

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(2, Some('\n')).unwrap();
        assert_eq!(chunks.len(), 0);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
    }

    #[test]
    fn chunks_without_delimiter_empty() {
        let log = "";

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(2, None).unwrap();
        assert_eq!(chunks.len(), 0);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
    }

    #[test]
    fn chunks_with_delimiter_start_and_end() {
        let log = "\n01\n23\n45\n67\n89\n";

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(2, Some('\n')).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
        assert_eq!(String::from_utf8_lossy(chunks[0]), "\n01\n23\n45\n");
        assert_eq!(String::from_utf8_lossy(chunks[1]), "67\n89\n");
    }

    #[test]
    fn chunks_with_delimiter_only() {
        let log = "\n\n\n\n\n\n\n\n\n\n";

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(2, Some('\n')).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
        assert_eq!(String::from_utf8_lossy(chunks[0]), "\n\n\n\n\n\n");
        assert_eq!(String::from_utf8_lossy(chunks[1]), "\n\n\n\n");
    }

    #[test]
    fn chunks_with_nonexistent_delimiter() {
        let log = "0123456789";

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(10, Some('\n')).unwrap();
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], b"0123456789");
    }

    #[test]
    fn chunks_with_delimiter_long_text() {
        let log = "Nov 23 06:26:40 ip-10-1-1-1 haproxy[20128]: 10.1.1.10:57305 [23/Nov/2019:06:26:40.781] public myapp/i-05fa49c0e7db8c328 0/0/0/78/78 206 913/458 - - ---- 9/9/6/0/0 0/0 {bytes=0-0} {||1|bytes 0-0/499704} \"GET /2518cb13a48bdf53b2f936f44e7042a3cc7baa06 HTTP/1.1\"
Nov 23 06:26:41 ip-10-1-1-1 haproxy[20128]: 10.1.1.11:51819 [23/Nov/2019:06:27:41.780] public myapp/i-059c225b48702964a 0/0/0/80/80 200 802/142190 - - ---- 8/8/5/0/0 0/0 {} {||141752|} \"GET /2043f2eb9e2691edcc0c8084d1ffce8bd70bc6e7 HTTP/1.1\"
Nov 23 06:26:42 ip-10-1-1-1 haproxy[20128]: 10.1.1.12:38870 [23/Nov/2019:06:28:42.773] public myapp/i-048088fd46abe7ed0 0/0/0/77/100 200 823/512174 - - ---- 8/8/5/0/0 0/0 {} {||511736|} \"GET /eb59c0b5dad36f080f3d261c6257ce0e21ef1a01 HTTP/1.1\"
Nov 23 06:26:43 ip-10-1-1-1 haproxy[20128]: 10.1.1.13:35528 [23/Nov/2019:06:29:43.775] public myapp/i-05e9315b035d50f62 0/0/0/103/105 200 869/431481 - - ---- 8/8/1/0/0 0/0 {} {|||} \"GET /164672c9d75c76a8fa237c24f9cbfd2222554f6d HTTP/1.1\"
Nov 23 06:26:44 ip-10-1-1-1 haproxy[20128]: 10.1.1.14:48553 [23/Nov/2019:06:30:44.808] public myapp/i-0008bfe6b1c98e964 0/0/0/72/73 200 840/265518 - - ---- 7/7/5/0/0 0/0 {} {||265080|} \"GET /e3b526928196d19ab3419d433f3de0ceb71e62b5 HTTP/1.1\"
Nov 23 06:26:45 ip-10-1-1-1 haproxy[20128]: 10.1.1.15:60969 [23/Nov/2019:06:31:45.727] public myapp/i-005a2bfdba4c405a8 0/0/0/146/167 200 852/304622 - - ---- 7/7/5/0/0 0/0 {} {||304184|} \"GET /52f5edb4a46276defe54ead2fae3a19fb8cafdb6 HTTP/1.1\"
Nov 23 06:26:46 ip-10-1-1-1 haproxy[20128]: 10.1.1.14:48539 [23/Nov/2019:06:32:46.730] public myapp/i-03b180605be4fa176 0/0/0/171/171 200 889/124142 - - ---- 6/6/4/0/0 0/0 {} {||123704|} \"GET /ef9e0c85cc1c76d7dc777f5b19d7cb85478496e4 HTTP/1.1\"
Nov 23 06:26:47 ip-10-1-1-1 haproxy[20128]: 10.1.1.11:51847 [23/Nov/2019:06:33:47.886] public myapp/i-0aa566420409956d6 0/0/0/28/28 206 867/458 - - ---- 6/6/4/0/0 0/0 {bytes=0-0} {} \"GET /3c7ace8c683adcad375a4d14995734ac0db08bb3 HTTP/1.1\"
Nov 23 06:26:48 ip-10-1-1-1 haproxy[20128]: 10.1.1.13:35554 [23/Nov/2019:06:34:48.866] public myapp/i-07f4205f35b4774b6 0/0/0/23/49 200 816/319662 - - ---- 5/5/3/0/0 0/0 {} {||319224|} \"GET /b95db0578977cd32658fa28b386c0db67ab23ee7 HTTP/1.1\"
Nov 23 06:26:49 ip-10-1-1-1 haproxy[20128]: 10.1.1.12:38899 [23/Nov/2019:06:35:49.879] public myapp/i-08cb5309afd22e8c0 0/0/0/59/59 200 1000/112110 - - ---- 5/5/3/0/0 0/0 {} {||111672|} \"GET /5314ca870ed0f5e48a71adca185e4ff7f1d9d80f HTTP/1.1\"
";
        let log_lines: Vec<_> = log.lines().collect();

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(5, Some('\n')).unwrap();
        assert_eq!(chunks.len(), 4); // N.B.: This is smaller than requested because we're chunking based on the delimeter
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
        assert_eq!(
            String::from_utf8_lossy(chunks[0]),
            format!("{}\n{}\n", log_lines[0], &log_lines[1])
        );
        assert_eq!(
            String::from_utf8_lossy(chunks[1]),
            format!("{}\n{}\n{}\n", log_lines[2], &log_lines[3], &log_lines[4])
        );
        assert_eq!(
            String::from_utf8_lossy(chunks[2]),
            format!("{}\n{}\n{}\n", log_lines[5], &log_lines[6], &log_lines[7])
        );
        assert_eq!(
            String::from_utf8_lossy(chunks[3]),
            format!("{}\n{}\n", log_lines[8], &log_lines[9])
        );
    }

    #[test]
    fn chunks_without_delimiter_long_text() {
        let log = "Nov 23 06:26:40 ip-10-1-1-1 haproxy[20128]: 10.1.1.10:57305 [23/Nov/2019:06:26:40.781] public myapp/i-05fa49c0e7db8c328 0/0/0/78/78 206 913/458 - - ---- 9/9/6/0/0 0/0 {bytes=0-0} {||1|bytes 0-0/499704} \"GET /2518cb13a48bdf53b2f936f44e7042a3cc7baa06 HTTP/1.1\"
Nov 23 06:26:41 ip-10-1-1-1 haproxy[20128]: 10.1.1.11:51819 [23/Nov/2019:06:27:41.780] public myapp/i-059c225b48702964a 0/0/0/80/80 200 802/142190 - - ---- 8/8/5/0/0 0/0 {} {||141752|} \"GET /2043f2eb9e2691edcc0c8084d1ffce8bd70bc6e7 HTTP/1.1\"
Nov 23 06:26:42 ip-10-1-1-1 haproxy[20128]: 10.1.1.12:38870 [23/Nov/2019:06:28:42.773] public myapp/i-048088fd46abe7ed0 0/0/0/77/100 200 823/512174 - - ---- 8/8/5/0/0 0/0 {} {||511736|} \"GET /eb59c0b5dad36f080f3d261c6257ce0e21ef1a01 HTTP/1.1\"
Nov 23 06:26:43 ip-10-1-1-1 haproxy[20128]: 10.1.1.13:35528 [23/Nov/2019:06:29:43.775] public myapp/i-05e9315b035d50f62 0/0/0/103/105 200 869/431481 - - ---- 8/8/1/0/0 0/0 {} {|||} \"GET /164672c9d75c76a8fa237c24f9cbfd2222554f6d HTTP/1.1\"
Nov 23 06:26:44 ip-10-1-1-1 haproxy[20128]: 10.1.1.14:48553 [23/Nov/2019:06:30:44.808] public myapp/i-0008bfe6b1c98e964 0/0/0/72/73 200 840/265518 - - ---- 7/7/5/0/0 0/0 {} {||265080|} \"GET /e3b526928196d19ab3419d433f3de0ceb71e62b5 HTTP/1.1\"
Nov 23 06:26:45 ip-10-1-1-1 haproxy[20128]: 10.1.1.15:60969 [23/Nov/2019:06:31:45.727] public myapp/i-005a2bfdba4c405a8 0/0/0/146/167 200 852/304622 - - ---- 7/7/5/0/0 0/0 {} {||304184|} \"GET /52f5edb4a46276defe54ead2fae3a19fb8cafdb6 HTTP/1.1\"
Nov 23 06:26:46 ip-10-1-1-1 haproxy[20128]: 10.1.1.14:48539 [23/Nov/2019:06:32:46.730] public myapp/i-03b180605be4fa176 0/0/0/171/171 200 889/124142 - - ---- 6/6/4/0/0 0/0 {} {||123704|} \"GET /ef9e0c85cc1c76d7dc777f5b19d7cb85478496e4 HTTP/1.1\"
Nov 23 06:26:47 ip-10-1-1-1 haproxy[20128]: 10.1.1.11:51847 [23/Nov/2019:06:33:47.886] public myapp/i-0aa566420409956d6 0/0/0/28/28 206 867/458 - - ---- 6/6/4/0/0 0/0 {bytes=0-0} {} \"GET /3c7ace8c683adcad375a4d14995734ac0db08bb3 HTTP/1.1\"
Nov 23 06:26:48 ip-10-1-1-1 haproxy[20128]: 10.1.1.13:35554 [23/Nov/2019:06:34:48.866] public myapp/i-07f4205f35b4774b6 0/0/0/23/49 200 816/319662 - - ---- 5/5/3/0/0 0/0 {} {||319224|} \"GET /b95db0578977cd32658fa28b386c0db67ab23ee7 HTTP/1.1\"
Nov 23 06:26:49 ip-10-1-1-1 haproxy[20128]: 10.1.1.12:38899 [23/Nov/2019:06:35:49.879] public myapp/i-08cb5309afd22e8c0 0/0/0/59/59 200 1000/112110 - - ---- 5/5/3/0/0 0/0 {} {||111672|} \"GET /5314ca870ed0f5e48a71adca185e4ff7f1d9d80f HTTP/1.1\"
";

        let mut file: File = tempfile::tempfile().unwrap();
        file.write_all(log.as_bytes()).unwrap();
        file.flush().unwrap();

        let chunks = 5;
        let chunk_size = log.len() / chunks;
        let chunker = FileChunker::new(&file).unwrap();
        let chunks = chunker.chunks(chunks, None).unwrap();

        assert_eq!(chunks.len(), 5);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), log.len());
        assert_eq!(
            String::from_utf8_lossy(chunks[0]),
            log[(0 * chunk_size)..=(1 * chunk_size)].to_string()
        );
        assert_eq!(
            String::from_utf8_lossy(chunks[1]),
            log[(1 * (chunk_size + 1))..=(2 * chunk_size + 1)].to_string()
        );
        assert_eq!(
            String::from_utf8_lossy(chunks[2]),
            log[(2 * (chunk_size + 1))..=(3 * chunk_size + 2)].to_string()
        );
        assert_eq!(
            String::from_utf8_lossy(chunks[3]),
            log[(3 * (chunk_size + 1))..=(4 * chunk_size + 3)].to_string()
        );
        assert_eq!(
            String::from_utf8_lossy(chunks[4]),
            log[(4 * (chunk_size + 1))..].to_string()
        );
    }
}
