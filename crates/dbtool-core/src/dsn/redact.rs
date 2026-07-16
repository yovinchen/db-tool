/// Replace credentials and secret query parameters of a DSN with `***` for
/// safe logging. The fallback also masks common assignment forms so malformed
/// configured DSNs are never echoed verbatim in `conn list` or errors.
pub fn redact_dsn(raw: &str) -> String {
    if let Ok(mut url) = url::Url::parse(raw) {
        if !url.username().is_empty() || url.password().is_some() {
            let _ = url.set_username("***");
            let _ = url.set_password(None);
        }
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(key, value)| {
                let value = if is_secret_key(&key) {
                    "***".to_owned()
                } else {
                    value.into_owned()
                };
                (key.into_owned(), value)
            })
            .collect();
        if url.query().is_some() {
            url.query_pairs_mut().clear().extend_pairs(pairs);
        }
        url.to_string()
    } else {
        redact_assignments(&redact_userinfo(raw))
    }
}

fn is_secret_key(key: &str) -> bool {
    let normalized: String = key
        .chars()
        .filter(|character| !matches!(character, '-' | '_' | '.' | ' '))
        .flat_map(char::to_lowercase)
        .collect();
    matches!(
        normalized.as_str(),
        "password"
            | "passwd"
            | "pwd"
            | "token"
            | "authtoken"
            | "accesstoken"
            | "refreshtoken"
            | "apikey"
            | "secret"
            | "clientsecret"
            | "saslpwd"
            | "saslpassword"
            | "credential"
            | "credentials"
    ) || normalized.ends_with("password")
        || normalized.ends_with("token")
        || normalized.ends_with("secret")
}

fn redact_userinfo(raw: &str) -> String {
    let Some(scheme_end) = raw.find("://") else {
        return raw.to_owned();
    };
    let authority_start = scheme_end + 3;
    let authority_end = raw[authority_start..]
        .find(['/', '?', '#'])
        .map_or(raw.len(), |offset| authority_start + offset);
    let authority = &raw[authority_start..authority_end];
    let Some(at) = authority.rfind('@') else {
        return raw.to_owned();
    };
    let userinfo_end = authority_start + at;
    format!("{}***{}", &raw[..authority_start], &raw[userinfo_end..])
}

fn redact_assignments(raw: &str) -> String {
    let mut output = String::with_capacity(raw.len());
    let mut cursor = 0;

    while cursor < raw.len() {
        let remaining = &raw[cursor..];
        let Some(equal_offset) = remaining.find('=') else {
            output.push_str(remaining);
            break;
        };
        let equal = cursor + equal_offset;
        let key_start = raw[..equal]
            .rfind(['?', '&', ';', ',', ' ', '\t'])
            .map_or(0, |index| index + 1);
        let key = &raw[key_start..equal];

        output.push_str(&raw[cursor..=equal]);
        let value_start = equal + 1;
        let value_end = raw[value_start..]
            .find(['&', ';', ',', ' ', '\t', '#'])
            .map_or(raw.len(), |offset| value_start + offset);
        if is_secret_key(key) {
            output.push_str("***");
        } else {
            output.push_str(&raw[value_start..value_end]);
        }
        cursor = value_end;
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_password() {
        let raw = "mysql://user:s3cr3t@host:3306/db";
        let redacted = redact_dsn(raw);
        assert!(redacted.contains("***"));
        assert!(!redacted.contains("user"));
        assert!(!redacted.contains("s3cr3t"));
    }

    #[test]
    fn masks_username_only_credentials() {
        let raw = "nats://NATS_USERNAME_TOKEN_MARKER@localhost:4222";
        let redacted = redact_dsn(raw);

        assert_eq!(redacted, "nats://***@localhost:4222");
        assert!(!redacted.contains("NATS_USERNAME_TOKEN_MARKER"));
    }

    #[test]
    fn no_password_unchanged() {
        let raw = "redis://host:6379/0";
        assert_eq!(redact_dsn(raw), raw);
    }

    #[test]
    fn masks_secret_query_parameters_without_hiding_normal_options() {
        let raw = "postgres://user:credential-one@host/db?sslmode=require&token=credential-two&access_token=credential-three&sasl-password=credential-four";
        let redacted = redact_dsn(raw);

        for secret in [
            "credential-one",
            "credential-two",
            "credential-three",
            "credential-four",
        ] {
            assert!(!redacted.contains(secret), "leaked {secret}: {redacted}");
        }
        assert!(redacted.contains("sslmode=require"));
    }

    #[test]
    fn malformed_dsn_uses_fail_safe_best_effort_redaction() {
        let raw = "not a url://user:plain-secret@bad host/path?token=query-secret&mode=safe";
        let redacted = redact_dsn(raw);

        assert!(!redacted.contains("plain-secret"));
        assert!(!redacted.contains("query-secret"));
        assert!(redacted.contains("mode=safe"));
    }

    #[test]
    fn malformed_username_only_credentials_are_redacted() {
        let raw = "not a url://NATS_USERNAME_TOKEN_MARKER@bad host/path";
        let redacted = redact_dsn(raw);

        assert_eq!(redacted, "not a url://***@bad host/path");
        assert!(!redacted.contains("NATS_USERNAME_TOKEN_MARKER"));
    }

    #[test]
    fn assignment_only_secret_is_redacted() {
        assert_eq!(
            redact_dsn("password=secret mode=readonly"),
            "password=*** mode=readonly"
        );
    }
}
