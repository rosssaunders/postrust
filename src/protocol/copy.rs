#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    pub format: CopyFormat,
    pub delimiter: char,
    pub null_marker: String,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            format: CopyFormat::Text,
            delimiter: '\t',
            null_marker: "\\N".to_string(),
        }
    }
}

pub fn encode_text_row(values: &[Option<String>], options: &CopyOptions) -> String {
    values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map(|raw| raw.replace(options.delimiter, "\\t"))
                .unwrap_or_else(|| options.null_marker.clone())
        })
        .collect::<Vec<_>>()
        .join(&options.delimiter.to_string())
}

pub fn decode_text_row(line: &str, options: &CopyOptions) -> Vec<Option<String>> {
    line.split(options.delimiter)
        .map(|value| {
            if value == options.null_marker {
                None
            } else {
                Some(value.replace("\\t", "\t"))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_copy_row_roundtrip() {
        let options = CopyOptions::default();
        let row = vec![Some("alpha".to_string()), None, Some("x\ty".to_string())];
        let encoded = encode_text_row(&row, &options);
        assert_eq!(decode_text_row(&encoded, &options), row);
    }
}
