use serde::Deserialize;

///
/// Configuration for the fumarole service
/// 
#[derive(Debug, Clone, Deserialize)]
pub struct FumaroleConfig {
    /// Endpoint to connect to the fumarole service
    pub endpoint: String,

    /// Optional token to use for authentication
    pub x_token: Option<String>,

    /// Maximum size of a message that can be decoded
    #[serde(default = "FumaroleConfig::default_max_decoding_message_size_bytes")]
    pub max_decoding_message_size_bytes: usize,
}

impl FumaroleConfig {
    ///
    /// Returns the default maximum size of a message that can be decoded
    ///  
    const fn default_max_decoding_message_size_bytes() -> usize {
        512_000_000
    }
}
