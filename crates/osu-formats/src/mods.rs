use rosu_mods::GameModsLegacy;

const RANKED_IN_STANDARD: GameModsLegacy = GameModsLegacy::NoFail
    .union(GameModsLegacy::Easy)
    .union(GameModsLegacy::TouchDevice)
    .union(GameModsLegacy::Hidden)
    .union(GameModsLegacy::HardRock)
    .union(GameModsLegacy::SuddenDeath)
    .union(GameModsLegacy::DoubleTime)
    .union(GameModsLegacy::HalfTime)
    .union(GameModsLegacy::Nightcore)
    .union(GameModsLegacy::Flashlight)
    .union(GameModsLegacy::SpunOut)
    .union(GameModsLegacy::Perfect);

const NIGHTCORE_ONLY: u32 = GameModsLegacy::Nightcore
    .difference(GameModsLegacy::DoubleTime)
    .bits();
const PERFECT_ONLY: u32 = GameModsLegacy::Perfect
    .difference(GameModsLegacy::SuddenDeath)
    .bits();

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Mods(u32);

impl Mods {
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn is_ranked_in_standard(self) -> bool {
        GameModsLegacy::from_bits_retain(self.0)
            .difference(RANKED_IN_STANDARD)
            .is_empty()
    }

    pub fn acronyms(self) -> String {
        // Only the ranked mods belong in the saved string, and rosu-mods names
        // Nightcore and Perfect only when their implied bits are present.
        let mut bits = self.0 & RANKED_IN_STANDARD.bits();
        if bits & NIGHTCORE_ONLY != 0 {
            bits |= GameModsLegacy::DoubleTime.bits();
        }
        if bits & PERFECT_ONLY != 0 {
            bits |= GameModsLegacy::SuddenDeath.bits();
        }
        let mods = GameModsLegacy::from_bits_retain(bits);
        if mods.is_empty() {
            String::new()
        } else {
            mods.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Mods;

    #[test]
    fn acronyms_follow_bit_order_and_hide_implied_mods() {
        for (bits, expected) in [
            (0, ""),
            (HD | DT, "HDDT"),
            (HD | DT | NC, "HDNC"),
            (SD | PF, "PF"),
            (NC, "NC"),
            (PF, "PF"),
            (RX, ""),
            (TARGET, ""),
            (EZ | HD | HT | FL, "EZHDHTFL"),
            (NF | TD | SO, "NFTDSO"),
        ] {
            assert_eq!(Mods::from_bits(bits).acronyms(), expected, "{bits}");
        }
    }

    const NF: u32 = 1;
    const EZ: u32 = 2;
    const TD: u32 = 4;
    const HD: u32 = 8;
    const HR: u32 = 16;
    const SD: u32 = 32;
    const DT: u32 = 64;
    const RX: u32 = 128;
    const HT: u32 = 256;
    const NC: u32 = 512;
    const FL: u32 = 1024;
    const AT: u32 = 2048;
    const SO: u32 = 4096;
    const AP: u32 = 8192;
    const PF: u32 = 16384;
    const KEY4: u32 = 32768;
    const CINEMA: u32 = 4194304;
    const TARGET: u32 = 8388608;
    const SCORE_V2: u32 = 536870912;

    #[test]
    fn ranked_combinations_are_accepted() {
        for bits in [0, NF | HD | DT | NC, HR | FL, SD | PF, EZ | HT, TD, SO] {
            assert!(Mods::from_bits(bits).is_ranked_in_standard(), "{bits}");
        }
    }

    #[test]
    fn any_unranked_mod_rejects_the_play() {
        for bits in [
            RX,
            AT,
            AP,
            CINEMA,
            TARGET,
            SCORE_V2,
            KEY4,
            HD | RX,
            DT | SCORE_V2,
        ] {
            assert!(!Mods::from_bits(bits).is_ranked_in_standard(), "{bits}");
        }
    }
}
