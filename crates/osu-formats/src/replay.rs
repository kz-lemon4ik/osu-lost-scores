use std::io::{self, Write};

use lzma_rs::decompress::{Options, UnpackedSize};
use lzma_rs::lzma_decompress_with_options;
use thiserror::Error;

use crate::{Mods, utf8_ignoring_invalid};

const TICKS_AT_UNIX_EPOCH: i64 = 621_355_968_000_000_000;
const TICKS_PER_SECOND: i64 = 10_000_000;
const LONG_ID_VERSION: i32 = 20_140_721;
const INT_ID_VERSION: i32 = 20_121_008;
const TARGET_PRACTICE: u32 = 1 << 23;
const OSU_MODE: u8 = 0;
const MAX_FRAMES_TEXT: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayHeader {
    pub mode: u8,
    pub version: i32,
    pub beatmap_md5: String,
    pub player_name: String,
    pub replay_md5: String,
    pub count_300: u16,
    pub count_100: u16,
    pub count_50: u16,
    pub count_geki: u16,
    pub count_katu: u16,
    pub count_miss: u16,
    pub score: u32,
    pub max_combo: u16,
    pub perfect: bool,
    pub mods: Mods,
    pub timestamp_ticks: i64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReplayError {
    #[error("replay ends too early")]
    Truncated,
    #[error("unexpected string marker {marker:#04x} at byte {offset}")]
    InvalidString { marker: u8, offset: usize },
    #[error("text at byte {offset} is not UTF-8")]
    InvalidText { offset: usize },
    #[error("replay is for mode {0}, not osu!standard")]
    OtherMode(u8),
    #[error("replay frames claim {0} bytes that the file does not hold")]
    FramesLength(i32),
    #[error("replay has {0} bytes after its end")]
    TrailingBytes(usize),
    #[error("replay hashes are not 32 hex digits")]
    Hash,
    #[error("replay life bar is not pairs of time and life")]
    LifeBar,
    #[error("replay frames are not a whole LZMA stream of cursor frames")]
    Frames,
}

impl ReplayHeader {
    pub fn parse(data: &[u8]) -> Result<Self, ReplayError> {
        Self::read(&mut Reader::new(data, false))
    }

    pub fn check(data: &[u8]) -> Result<Self, ReplayError> {
        let mut r = Reader::new(data, true);
        let (header, life) = Self::read_with_life(&mut r)?;
        if header.mode != OSU_MODE {
            return Err(ReplayError::OtherMode(header.mode));
        }
        let frames = r.i32()?;
        let len = usize::try_from(frames).map_err(|_| ReplayError::FramesLength(frames))?;
        let stream = r.take(len).map_err(|_| ReplayError::FramesLength(frames))?;
        if header.version >= LONG_ID_VERSION {
            r.i64()?;
        } else if header.version >= INT_ID_VERSION {
            r.i32()?;
        }
        if header.mods.bits() & TARGET_PRACTICE != 0 {
            r.take(8)?;
        }
        match r.left() {
            0 => {}
            left => return Err(ReplayError::TrailingBytes(left)),
        }
        if !hex_hash(&header.beatmap_md5) || !hex_hash(&header.replay_md5) {
            return Err(ReplayError::Hash);
        }
        if !life_pairs(life.as_bytes()) {
            return Err(ReplayError::LifeBar);
        }
        if !frames_text(stream).is_some_and(|text| cursor_frames(&text)) {
            return Err(ReplayError::Frames);
        }
        Ok(header)
    }

    fn read(r: &mut Reader) -> Result<Self, ReplayError> {
        Self::read_with_life(r).map(|(header, _)| header)
    }

    fn read_with_life(r: &mut Reader) -> Result<(Self, String), ReplayError> {
        let mode = r.u8()?;
        let version = r.i32()?;
        let beatmap_md5 = r.string()?;
        let player_name = r.string()?;
        let replay_md5 = r.string()?;
        let [
            count_300,
            count_100,
            count_50,
            count_geki,
            count_katu,
            count_miss,
        ] = [r.u16()?, r.u16()?, r.u16()?, r.u16()?, r.u16()?, r.u16()?];
        let score = r.u32()?;
        let max_combo = r.u16()?;
        let perfect = r.u8()? == 1;
        let mods = Mods::from_bits(r.u32()?);
        let life = r.string()?;
        let timestamp_ticks = r.i64()?;
        Ok((
            Self {
                mode,
                version,
                beatmap_md5,
                player_name,
                replay_md5,
                count_300,
                count_100,
                count_50,
                count_geki,
                count_katu,
                count_miss,
                score,
                max_combo,
                perfect,
                mods,
                timestamp_ticks,
            },
            life,
        ))
    }

    pub fn unix_time(&self) -> i64 {
        self.timestamp_ticks
            .saturating_sub(TICKS_AT_UNIX_EPOCH)
            .div_euclid(TICKS_PER_SECOND)
    }
}

fn hex_hash(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

struct Scan<'a> {
    text: &'a [u8],
    at: usize,
}

impl Scan<'_> {
    fn eat(&mut self, byte: u8) -> bool {
        let hit = self.text.get(self.at) == Some(&byte);
        if hit {
            self.at += 1;
        }
        hit
    }

    fn digits(&mut self) -> bool {
        let start = self.at;
        while self.text.get(self.at).is_some_and(u8::is_ascii_digit) {
            self.at += 1;
        }
        self.at > start
    }

    fn whole(&mut self) -> bool {
        self.eat(b'-');
        self.digits()
    }

    fn number(&mut self) -> bool {
        if !self.whole() || (self.eat(b'.') && !self.digits()) {
            return false;
        }
        if self.eat(b'E') || self.eat(b'e') {
            if !self.eat(b'+') {
                self.eat(b'-');
            }
            return self.digits();
        }
        true
    }

    fn done(&self) -> bool {
        self.at >= self.text.len()
    }
}

struct Capped(Vec<u8>);

impl Write for Capped {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.0.len() + data.len() > MAX_FRAMES_TEXT {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        self.0.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn frames_text(stream: &[u8]) -> Option<Vec<u8>> {
    let declared = u64::from_le_bytes(stream.get(5..13)?.try_into().ok()?);
    whole_stream(stream, UnpackedSize::ReadFromHeader).or_else(|| {
        if declared == u64::MAX {
            return None;
        }
        whole_stream(stream, UnpackedSize::ReadHeaderButUseProvided(None))
            .filter(|text| u64::try_from(text.len()).is_ok_and(|length| length == declared))
    })
}

fn whole_stream(stream: &[u8], unpacked_size: UnpackedSize) -> Option<Vec<u8>> {
    // lzma-rs skips the first byte of the range coder; a whole LZMA stream has
    // a zero there, as the decoder this replaced demanded.
    if stream.get(13).copied() != Some(0) {
        return None;
    }
    let mut input = io::Cursor::new(stream);
    let mut output = Capped(Vec::new());
    let options = Options {
        unpacked_size,
        memlimit: Some(MAX_FRAMES_TEXT),
        allow_incomplete: false,
    };
    lzma_decompress_with_options(&mut input, &mut output, &options).ok()?;
    (usize::try_from(input.position()).ok()? == stream.len()).then_some(output.0)
}

fn cursor_frames(text: &[u8]) -> bool {
    let mut scan = Scan { text, at: 0 };
    let mut frames = 0;
    while !scan.done() {
        let frame = scan.whole()
            && scan.eat(b'|')
            && scan.number()
            && scan.eat(b'|')
            && scan.number()
            && scan.eat(b'|')
            && scan.whole();
        if !frame || (!scan.done() && !scan.eat(b',')) {
            return false;
        }
        frames += 1;
    }
    frames > 0
}

fn life_pairs(text: &[u8]) -> bool {
    let mut scan = Scan { text, at: 0 };
    while !scan.done() {
        if !(scan.whole() && scan.eat(b'|') && scan.number() && scan.eat(b',')) {
            return false;
        }
    }
    true
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    strict: bool,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8], strict: bool) -> Self {
        Self {
            data,
            pos: 0,
            strict,
        }
    }

    fn left(&self) -> usize {
        self.data.len() - self.pos
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ReplayError> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|&end| end <= self.data.len())
            .ok_or(ReplayError::Truncated)?;
        let bytes = &self.data[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ReplayError> {
        let mut out = [0; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, ReplayError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ReplayError> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Result<u32, ReplayError> {
        self.array().map(u32::from_le_bytes)
    }

    fn i32(&mut self) -> Result<i32, ReplayError> {
        self.array().map(i32::from_le_bytes)
    }

    fn i64(&mut self) -> Result<i64, ReplayError> {
        self.array().map(i64::from_le_bytes)
    }

    fn uleb128(&mut self) -> Result<u64, ReplayError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(ReplayError::Truncated)
    }

    fn string(&mut self) -> Result<String, ReplayError> {
        let offset = self.pos;
        match self.u8()? {
            0x00 => Ok(String::new()),
            0x0b => {
                let len = usize::try_from(self.uleb128()?).map_err(|_| ReplayError::Truncated)?;
                let at = self.pos;
                let bytes = self.take(len)?;
                if self.strict {
                    std::str::from_utf8(bytes)
                        .map(str::to_owned)
                        .map_err(|_| ReplayError::InvalidText { offset: at })
                } else {
                    Ok(utf8_ignoring_invalid(bytes))
                }
            }
            marker => Err(ReplayError::InvalidString { marker, offset }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ReplayError, ReplayHeader};
    use crate::Mods;

    const TICKS_2024_10_28_11_17_00: i64 = 638_657_110_200_000_000;
    const HEADER_LEN: usize = 112;

    struct Sample {
        beatmap_md5: Vec<u8>,
        player: Vec<u8>,
        replay_md5: Vec<u8>,
        ticks: i64,
    }

    impl Default for Sample {
        fn default() -> Self {
            Self {
                beatmap_md5: short_string("0123456789abcdef0123456789abcdef"),
                player: short_string("Lemon"),
                replay_md5: short_string("fedcba9876543210fedcba9876543210"),
                ticks: TICKS_2024_10_28_11_17_00,
            }
        }
    }

    impl Sample {
        fn bytes(&self) -> Vec<u8> {
            let mut out = vec![2];
            out.extend(20190906i32.to_le_bytes());
            out.extend(&self.beatmap_md5);
            out.extend(&self.player);
            out.extend(&self.replay_md5);
            for count in [557u16, 160, 64, 12, 34, 125] {
                out.extend(count.to_le_bytes());
            }
            out.extend(447406u32.to_le_bytes());
            out.extend(106u16.to_le_bytes());
            out.push(1);
            out.extend(10u32.to_le_bytes());
            out.push(0x00);
            out.extend(self.ticks.to_le_bytes());
            out.extend(5i32.to_le_bytes());
            out.extend([1, 2, 3, 4, 5]);
            out
        }
    }

    fn text(value: &str) -> Vec<u8> {
        if value.is_empty() {
            return vec![0x00];
        }
        let mut out = vec![0x0b];
        let mut len = value.len();
        loop {
            let byte = u8::try_from(len & 0x7f).unwrap();
            len >>= 7;
            out.push(if len == 0 { byte } else { byte | 0x80 });
            if len == 0 {
                break;
            }
        }
        out.extend(value.as_bytes());
        out
    }

    fn with_frames(frames: &[u8]) -> Vec<u8> {
        Whole {
            frames_len: i32::try_from(frames.len()).unwrap(),
            frames: frames.to_vec(),
            ..Whole::default()
        }
        .bytes()
    }

    #[test]
    fn the_frames_are_a_whole_lzma_stream_of_cursor_frames() {
        let mut known = FRAMES.to_vec();
        known[5..13].copy_from_slice(&72u64.to_le_bytes());
        for frames in [FRAMES, LAST_WITHOUT_COMMA, &known] {
            assert!(ReplayHeader::check(&with_frames(frames)).is_ok());
        }
    }

    #[test]
    fn frames_that_are_not_lzma_or_not_cursor_frames_are_refused() {
        let mut zip = vec![0x50, 0x4b, 0x03, 0x04];
        zip.extend([0; 40]);
        for frames in [&zip[..], HTML, BAD_NUMBER, EMPTY] {
            assert_eq!(
                ReplayHeader::check(&with_frames(frames)),
                Err(ReplayError::Frames)
            );
        }
    }

    #[test]
    fn a_stream_that_would_unpack_past_the_limit_is_refused() {
        let mut huge = FRAMES.to_vec();
        huge[5..13].copy_from_slice(&(1u64 << 32).to_le_bytes());
        for frames in [BOMB, &huge] {
            assert_eq!(
                ReplayHeader::check(&with_frames(frames)),
                Err(ReplayError::Frames)
            );
        }
    }

    #[test]
    fn anything_after_the_stream_a_cut_stream_or_bad_properties_are_refused() {
        let mut after = FRAMES.to_vec();
        after.extend([0x50, 0x4b, 0x05, 0x06]);
        let cut = &FRAMES[..FRAMES.len() - 3];
        let mut properties = FRAMES.to_vec();
        properties[0] = 225;
        for frames in [&after[..], cut, &properties] {
            assert_eq!(
                ReplayHeader::check(&with_frames(frames)),
                Err(ReplayError::Frames)
            );
        }
    }

    #[test]
    fn a_nonzero_range_coder_lead_byte_is_refused() {
        for bit in 0..8 {
            let mut frames = FRAMES.to_vec();
            frames[13] ^= 1 << bit;
            assert_eq!(
                ReplayHeader::check(&with_frames(&frames)),
                Err(ReplayError::Frames),
                "{bit}"
            );
        }
    }

    #[test]
    fn broken_streams_are_refused_without_panicking() {
        for at in 0..FRAMES.len() {
            for bit in 0..8 {
                let mut frames = FRAMES.to_vec();
                frames[at] ^= 1 << bit;
                let _ = ReplayHeader::check(&with_frames(&frames));
            }
            assert!(ReplayHeader::check(&with_frames(&FRAMES[..at])).is_err());
        }
    }

    #[test]
    fn the_life_bar_holds_only_time_and_life_pairs() {
        for life in ["-120|1,0|0.98,4000|1E-05,", ""] {
            let whole = Whole {
                life: life.into(),
                ..Whole::default()
            };
            assert!(ReplayHeader::check(&whole.bytes()).is_ok(), "{life}");
        }
        for life in ["%PDF-1.4 %\u{e2}\u{e3} 1 0 obj", "0|1,abc|0.5,", "0|1"] {
            let whole = Whole {
                life: life.into(),
                ..Whole::default()
            };
            assert_eq!(
                ReplayHeader::check(&whole.bytes()),
                Err(ReplayError::LifeBar),
                "{life}"
            );
        }
    }

    #[test]
    fn both_hashes_are_32_lowercase_hex_digits() {
        for (beatmap_md5, replay_md5) in [
            (
                "0123456789abcdef0123456789abcdef",
                format!("{:<32}", "%PDF-1.4"),
            ),
            ("0123456789abcdef0123456789abcdef", "F".repeat(32)),
            (
                "0123456789abcdef0123456789abcde",
                "fedcba9876543210fedcba9876543210".into(),
            ),
        ] {
            let whole = Whole {
                beatmap_md5: beatmap_md5.into(),
                replay_md5,
                ..Whole::default()
            };
            assert_eq!(ReplayHeader::check(&whole.bytes()), Err(ReplayError::Hash));
        }
    }

    fn short_string(text: &str) -> Vec<u8> {
        let mut out = vec![0x0b, u8::try_from(text.len()).unwrap()];
        out.extend(text.as_bytes());
        out
    }

    fn parse(sample: Sample) -> ReplayHeader {
        ReplayHeader::parse(&sample.bytes()).unwrap()
    }

    const FRAMES: &[u8] = include_bytes!("../testdata/frames.lzma");
    const LAST_WITHOUT_COMMA: &[u8] = include_bytes!("../testdata/last-without-comma.lzma");
    const HTML: &[u8] = include_bytes!("../testdata/html.lzma");
    const BAD_NUMBER: &[u8] = include_bytes!("../testdata/bad-number.lzma");
    const EMPTY: &[u8] = include_bytes!("../testdata/empty.lzma");
    const BOMB: &[u8] = include_bytes!("../testdata/bomb.lzma");

    struct Whole {
        mode: u8,
        version: i32,
        beatmap_md5: String,
        replay_md5: String,
        life: String,
        player: Vec<u8>,
        mods: u32,
        frames_len: i32,
        frames: Vec<u8>,
        online: Vec<u8>,
        accuracy: Option<f64>,
        tail: Vec<u8>,
    }

    impl Default for Whole {
        fn default() -> Self {
            Self {
                mode: 0,
                version: 20210520,
                beatmap_md5: "0123456789abcdef0123456789abcdef".into(),
                replay_md5: "fedcba9876543210fedcba9876543210".into(),
                life: "0|1,".into(),
                player: short_string("Lemon"),
                mods: 72,
                frames_len: i32::try_from(FRAMES.len()).unwrap(),
                frames: FRAMES.to_vec(),
                online: 3_456_789_012i64.to_le_bytes().to_vec(),
                accuracy: None,
                tail: Vec::new(),
            }
        }
    }

    impl Whole {
        fn bytes(&self) -> Vec<u8> {
            let mut out = vec![self.mode];
            out.extend(self.version.to_le_bytes());
            out.extend(text(&self.beatmap_md5));
            out.extend(&self.player);
            out.extend(text(&self.replay_md5));
            for count in [557u16, 160, 64, 12, 34, 125] {
                out.extend(count.to_le_bytes());
            }
            out.extend(447406u32.to_le_bytes());
            out.extend(106u16.to_le_bytes());
            out.push(1);
            out.extend(self.mods.to_le_bytes());
            out.extend(text(&self.life));
            out.extend(TICKS_2024_10_28_11_17_00.to_le_bytes());
            out.extend(self.frames_len.to_le_bytes());
            out.extend(&self.frames);
            out.extend(&self.online);
            if let Some(accuracy) = self.accuracy {
                out.extend(accuracy.to_le_bytes());
            }
            out.extend(&self.tail);
            out
        }
    }

    #[test]
    fn a_whole_replay_is_checked_field_by_field() {
        let header = ReplayHeader::check(&Whole::default().bytes()).unwrap();
        assert_eq!(
            (
                header.mode,
                header.score,
                header.mods,
                header.player_name.as_str()
            ),
            (0, 447406, Mods::from_bits(72), "Lemon")
        );
    }

    #[test]
    fn anything_after_the_online_id_is_refused() {
        for tail in [vec![0], vec![1; 8]] {
            let len = tail.len();
            let whole = Whole {
                tail,
                ..Whole::default()
            };
            assert_eq!(
                ReplayHeader::check(&whole.bytes()),
                Err(ReplayError::TrailingBytes(len))
            );
        }
    }

    #[test]
    fn a_truncated_file_or_a_frames_length_past_the_end_is_refused() {
        let whole = Whole::default().bytes();
        for cut in [0, 1, 10, 40, whole.len() - 9, whole.len() - 1] {
            assert!(ReplayHeader::check(&whole[..cut]).is_err(), "{cut}");
        }
        for frames_len in [6, 100_000, -1] {
            let whole = Whole {
                frames_len,
                ..Whole::default()
            };
            assert!(ReplayHeader::check(&whole.bytes()).is_err(), "{frames_len}");
        }
        let backwards = Whole {
            frames_len: -8,
            frames: Vec::new(),
            online: Vec::new(),
            ..Whole::default()
        };
        assert_eq!(
            ReplayHeader::check(&backwards.bytes()),
            Err(ReplayError::FramesLength(-8))
        );
    }

    #[test]
    fn a_string_without_its_marker_or_with_broken_text_is_refused() {
        let mut no_marker = Whole::default().bytes();
        no_marker[5] = 0x0c;
        assert!(matches!(
            ReplayHeader::check(&no_marker),
            Err(ReplayError::InvalidString { marker: 0x0c, .. })
        ));
        let broken = Whole {
            player: vec![0x0b, 2, 0xff, 0x61],
            ..Whole::default()
        }
        .bytes();
        assert!(matches!(
            ReplayHeader::check(&broken),
            Err(ReplayError::InvalidText { .. })
        ));
        assert!(ReplayHeader::parse(&broken).is_ok());
    }

    #[test]
    fn a_replay_of_another_mode_is_refused() {
        for mode in [1, 2, 3] {
            let whole = Whole {
                mode,
                ..Whole::default()
            };
            assert_eq!(
                ReplayHeader::check(&whole.bytes()),
                Err(ReplayError::OtherMode(mode))
            );
        }
    }

    #[test]
    fn older_and_target_practice_replays_keep_their_own_endings() {
        let older = Whole {
            version: 20130101,
            online: 123_456i32.to_le_bytes().to_vec(),
            ..Whole::default()
        };
        assert!(ReplayHeader::check(&older.bytes()).is_ok());
        let oldest = Whole {
            version: 20110101,
            online: Vec::new(),
            ..Whole::default()
        };
        assert!(ReplayHeader::check(&oldest.bytes()).is_ok());
        let target = 1 << 23;
        let practice = Whole {
            mods: target,
            accuracy: Some(0.98),
            ..Whole::default()
        };
        assert!(ReplayHeader::check(&practice.bytes()).is_ok());
        let no_accuracy = Whole {
            mods: target,
            ..Whole::default()
        };
        assert!(ReplayHeader::check(&no_accuracy.bytes()).is_err());
    }

    #[test]
    fn reads_every_header_field() {
        assert_eq!(
            parse(Sample::default()),
            ReplayHeader {
                mode: 2,
                version: 20190906,
                beatmap_md5: "0123456789abcdef0123456789abcdef".into(),
                player_name: "Lemon".into(),
                replay_md5: "fedcba9876543210fedcba9876543210".into(),
                count_300: 557,
                count_100: 160,
                count_50: 64,
                count_geki: 12,
                count_katu: 34,
                count_miss: 125,
                score: 447406,
                max_combo: 106,
                perfect: true,
                mods: Mods::from_bits(10),
                timestamp_ticks: TICKS_2024_10_28_11_17_00,
            }
        );
    }

    #[test]
    fn empty_string_marker_reads_as_empty_string() {
        let header = parse(Sample {
            replay_md5: vec![0x00],
            ..Sample::default()
        });
        assert_eq!((header.replay_md5.as_str(), header.score), ("", 447406));
    }

    #[test]
    fn string_length_is_uleb128() {
        let mut player = vec![0x0b, 0xc8, 0x01];
        player.extend([b'a'; 200]);
        let header = parse(Sample {
            player,
            ..Sample::default()
        });
        assert_eq!((header.player_name.len(), header.score), (200, 447406));
    }

    #[test]
    fn invalid_utf8_in_a_string_is_dropped() {
        let header = parse(Sample {
            player: vec![0x0b, 0x06, b'L', b'e', 0xff, b'm', b'o', b'n'],
            ..Sample::default()
        });
        assert_eq!(header.player_name, "Lemon");
    }

    #[test]
    fn unknown_string_marker_is_an_error() {
        let sample = Sample {
            beatmap_md5: vec![0x05],
            ..Sample::default()
        };
        assert_eq!(
            ReplayHeader::parse(&sample.bytes()),
            Err(ReplayError::InvalidString {
                marker: 0x05,
                offset: 5
            })
        );
    }

    #[test]
    fn data_ending_inside_the_header_is_truncated() {
        let bytes = Sample::default().bytes();
        for len in 0..HEADER_LEN {
            assert_eq!(
                ReplayHeader::parse(&bytes[..len]),
                Err(ReplayError::Truncated),
                "{len}"
            );
        }
    }

    #[test]
    fn replay_frames_are_not_needed() {
        let bytes = Sample::default().bytes();
        assert!(ReplayHeader::parse(&bytes[..HEADER_LEN]).is_ok());
    }

    #[test]
    fn string_longer_than_the_data_is_truncated() {
        for player in [
            vec![0x0b, 0x7f, b'a'],
            vec![
                0x0b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
            ],
        ] {
            let sample = Sample {
                player,
                ..Sample::default()
            };
            assert_eq!(
                ReplayHeader::parse(&sample.bytes()),
                Err(ReplayError::Truncated)
            );
        }
    }

    #[test]
    fn unix_time_floors_ticks_to_whole_seconds() {
        for (ticks, expected) in [
            (TICKS_2024_10_28_11_17_00, 1_730_114_220),
            (TICKS_2024_10_28_11_17_00 + 9_999_999, 1_730_114_220),
            (TICKS_2024_10_28_11_17_00 - 1, 1_730_114_219),
        ] {
            let header = parse(Sample {
                ticks,
                ..Sample::default()
            });
            assert_eq!(header.unix_time(), expected, "{ticks}");
        }
    }
}
