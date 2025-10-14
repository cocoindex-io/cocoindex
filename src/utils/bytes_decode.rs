use encoding_rs::Encoding;

pub fn bytes_to_string(bytes: &[u8]) -> (String, bool) {
    // 1) BOM sniff first (definitive for UTF-8/16; UTF-32 is not supported here).
    if let Some((enc, bom_len)) = Encoding::for_bom(bytes) {
        let (cow, had_errors) = enc.decode_without_bom_handling(&bytes[bom_len..]);
        return (cow.into_owned(), had_errors);
    }
    // 2) Otherwise, try UTF-8 (accepts input with or without a UTF-8 BOM).
    let (cow, had_errors) = encoding_rs::UTF_8.decode_with_bom_removal(bytes);
    (cow.into_owned(), had_errors)
}
