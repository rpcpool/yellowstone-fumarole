use {serde::Deserialize, std::collections::BTreeMap};

///
/// Configuration for the fumarole service
///
#[derive(Debug, Clone, Deserialize)]
pub struct FumaroleConfig {
    /// Endpoint to connect to the fumarole service
    pub endpoint: String,

    /// Optional token to use for authentication
    #[serde(default, alias = "x-token")]
    pub x_token: Option<String>,

    /// Maximum size of a message that can be decoded
    #[serde(default = "FumaroleConfig::default_max_decoding_message_size_bytes")]
    pub max_decoding_message_size_bytes: usize,

    /// Additional metadata to include in the request
    /// Must be a mapping of string to string
    #[serde(default, rename = "x-metadata")]
    pub x_metadata: BTreeMap<String, String>,
}

impl FumaroleConfig {
    ///
    /// Returns the default maximum size of a message that can be decoded
    ///  
    const fn default_max_decoding_message_size_bytes() -> usize {
        512_000_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_yaml_deser() {
        let yaml = r#"
        endpoint: http://127.0.0.1:9090
        x-token: some-token
        x-metadata: 
            x-foo: bar
            x-baz: jazz 
        "#;

        let config: FumaroleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.max_decoding_message_size_bytes, 512_000_000);
        assert_eq!(config.x_token, Some("some-token".to_string()));
        assert_eq!(config.x_metadata.len(), 2);
        assert_eq!(config.x_metadata.get("x-foo"), Some(&"bar".to_string()));
        assert_eq!(config.x_metadata.get("x-baz"), Some(&"jazz".to_string()));
    }

    #[test]
    fn it_should_support_x_token_alias() {
        let yaml1 = r#"
        endpoint: http://127.0.0.1:9090
        x_token: some-token
        "#;
        let yaml2 = r#"
        endpoint: http://127.0.0.1:9090
        x-token: some-token
        "#;
        let config1: FumaroleConfig = serde_yaml::from_str(yaml1).unwrap();
        let config2: FumaroleConfig = serde_yaml::from_str(yaml2).unwrap();
        assert_eq!(config1.x_token, config2.x_token);
    }
}
