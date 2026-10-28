use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, Result, anyhow, bail};
use base64::Engine as _;
use collections::HashMap;
use context_server::{
    client::{Client, ContextServerId},
    oauth::{self, AuthServerMetadata, OAuthTokens, WwwAuthenticate},
    protocol::ModelContextProtocol,
    transport::HttpTransport,
    types::{CallToolParams, Implementation, ToolResponseContent, requests::CallTool},
};
use gpui::AsyncApp;
use http_client::HttpClient;
use rand::Rng as _;
use url::Url;

use crate::callback::{CallbackListener, CallbackOutcome};

/// Meegle sites Vilo trusts as identity issuers. Discovery responses are only
/// accepted when every endpoint stays on the chosen host.
pub const MEEGLE_HOSTS: &[&str] = &["project.larksuite.com", "project.feishu.cn", "meegle.com"];

const RESOURCE_PATH: &str = "mcp_server/v1";
const IDENTITY_TOOL: &str = "search_user_info";
const SIGN_IN_DEADLINE: Duration = Duration::from_secs(5 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct VerifiedIdentity {
    pub issuer: Url,
    pub resource: Url,
    pub subject: String,
    pub display_name: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub client_id: String,
    pub token_endpoint: Url,
    pub tokens: OAuthTokens,
}

pub(crate) async fn sign_in(host: &str, cx: &mut AsyncApp) -> Result<VerifiedIdentity> {
    if !MEEGLE_HOSTS.contains(&host) {
        bail!("{host} is not a trusted Meegle site");
    }
    let issuer = Url::parse(&format!("https://{host}"))?;
    let resource = issuer.join(RESOURCE_PATH)?;
    let http = cx.update(|cx| cx.http_client());

    let metadata = discover(&http, &issuer, &resource).await?;
    let registration_endpoint = metadata
        .registration_endpoint
        .clone()
        .context("Meegle does not offer client registration")?;

    let state = random_token();
    let listener = CallbackListener::bind(state.clone(), SIGN_IN_DEADLINE)?;
    let registration = oauth::perform_dcr(
        &http,
        &registration_endpoint,
        "Vilo",
        &listener.redirect_uri,
        metadata.grant_types_supported.as_deref(),
        &[],
    )
    .await?;
    let pkce = oauth::generate_pkce_challenge();
    let authorization_url = oauth::build_authorization_url(
        &metadata,
        &registration.client_id,
        &listener.redirect_uri,
        &[],
        resource.as_str(),
        &pkce,
        &state,
    );
    let redirect_uri = listener.redirect_uri.clone();
    cx.update(|cx| cx.open_url(authorization_url.as_str()));

    let code = match listener.outcome().await? {
        CallbackOutcome::Code(code) => code,
        CallbackOutcome::Denied(error) => bail!("sign-in was declined in the browser ({error})"),
    };
    let tokens = oauth::exchange_code(
        &http,
        &metadata,
        &code,
        &registration.client_id,
        &redirect_uri,
        &pkce.verifier,
        resource.as_str(),
        registration.client_secret.as_deref(),
    )
    .await?;

    let identity = fetch_identity(&resource, &tokens.access_token, cx).await?;
    Ok(VerifiedIdentity {
        issuer,
        resource,
        subject: identity.subject,
        display_name: identity.display_name,
        email: identity.email,
        avatar_url: identity.avatar_url,
        client_id: registration.client_id,
        token_endpoint: metadata.token_endpoint,
        tokens,
    })
}

/// Fetches and pins the OAuth metadata for one trusted host. Each request is
/// checked before it is made, so a hostile response cannot steer later requests.
async fn discover(
    http: &Arc<dyn HttpClient>,
    issuer: &Url,
    resource: &Url,
) -> Result<AuthServerMetadata> {
    let no_challenge = WwwAuthenticate {
        resource_metadata: None,
        scope: None,
        error: None,
        error_description: None,
    };
    let resource_metadata =
        oauth::fetch_protected_resource_metadata(http, resource, &no_challenge).await?;
    if resource_metadata.resource != *resource
        || resource_metadata.authorization_servers != [issuer.clone()]
    {
        bail!("Meegle resource metadata does not match {issuer}");
    }

    let metadata = oauth::fetch_auth_server_metadata(http, issuer).await?;
    let endpoints = [
        Some(&metadata.authorization_endpoint),
        Some(&metadata.token_endpoint),
        metadata.registration_endpoint.as_ref(),
    ];
    for endpoint in endpoints.into_iter().flatten() {
        if endpoint.scheme() != "https" || endpoint.host_str() != issuer.host_str() {
            bail!("Meegle endpoint {endpoint} is outside {issuer}");
        }
    }
    let supports_s256 = metadata
        .code_challenge_methods_supported
        .as_ref()
        .is_some_and(|methods| methods.iter().any(|method| method == "S256"));
    if !supports_s256 {
        bail!("Meegle does not support PKCE S256");
    }
    Ok(metadata)
}

struct Identity {
    subject: String,
    display_name: String,
    email: Option<String>,
    avatar_url: Option<String>,
}

/// Asks the Meegle MCP server who the token belongs to. The account is only
/// accepted when the answer carries a `user_key`; names and email are display only.
async fn fetch_identity(resource: &Url, access_token: &str, cx: &mut AsyncApp) -> Result<Identity> {
    let response = call_tool(
        resource,
        access_token,
        IDENTITY_TOOL,
        serde_json::json!({ "user_keys": ["current_login_user()"] }),
        cx,
    )
    .await?;
    parse_identity(&response)
        .ok_or_else(|| anyhow!("Meegle did not return a user_key for the signed-in user"))
}

/// Read-only Meegle MCP tools Vilo may call with the account's token. The token
/// itself can do more, so this list is the boundary (VILO_IDENTITY_LOGIN_PLAN §4.1).
const ALLOWED_TOOLS: &[&str] = &[IDENTITY_TOOL, "list_todo", "search_project_info"];

/// Calls one Meegle MCP tool and returns the JSON document in its text output.
pub(crate) async fn call_tool(
    resource: &Url,
    access_token: &str,
    name: &str,
    arguments: serde_json::Value,
    cx: &mut AsyncApp,
) -> Result<serde_json::Value> {
    if !ALLOWED_TOOLS.contains(&name) {
        bail!("Vilo does not call the Meegle tool {name}");
    }
    let http = cx.update(|cx| cx.http_client());
    let headers = HashMap::from_iter([(
        "Authorization".to_string(),
        format!("Bearer {access_token}"),
    )]);
    let transport = Arc::new(HttpTransport::new(
        http,
        resource.to_string(),
        headers,
        cx.background_executor().clone(),
    ));
    let client = Client::new(
        ContextServerId("vilo-account".into()),
        "Vilo account".into(),
        transport,
        Some(REQUEST_TIMEOUT),
        cx.clone(),
    )?;
    let protocol = ModelContextProtocol::new(client)
        .initialize(Implementation {
            name: "Vilo".to_string(),
            title: None,
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: None,
        })
        .await?;
    let response = protocol
        .request::<CallTool>(CallToolParams {
            name: name.to_string(),
            arguments: Some(arguments),
            meta: None,
        })
        .await?;
    if response.is_error == Some(true) {
        bail!("Meegle reported an error for {name}");
    }
    response
        .content
        .iter()
        .find_map(|content| match content {
            ToolResponseContent::Text { text } => serde_json::from_str(text).ok(),
            _ => None,
        })
        .ok_or_else(|| anyhow!("Meegle returned no data for {name}"))
}

/// Exchanges the stored refresh token for a fresh access token.
pub(crate) async fn refresh(
    token_endpoint: &Url,
    refresh_token: &str,
    client_id: &str,
    resource: &Url,
    cx: &mut AsyncApp,
) -> Result<OAuthTokens> {
    if token_endpoint.scheme() != "https" || token_endpoint.host_str() != resource.host_str() {
        bail!("stored Meegle token endpoint does not match the account");
    }
    let http = cx.update(|cx| cx.http_client());
    oauth::refresh_tokens(
        &http,
        token_endpoint,
        refresh_token,
        client_id,
        resource.as_str(),
        None,
    )
    .await
}

fn parse_identity(value: &serde_json::Value) -> Option<Identity> {
    match value {
        serde_json::Value::Object(object) => {
            let subject = match object.get("user_key") {
                Some(serde_json::Value::String(key)) if !key.is_empty() => Some(key.clone()),
                Some(serde_json::Value::Number(key)) => Some(key.to_string()),
                _ => None,
            };
            if let Some(subject) = subject {
                let text = |key: &str| {
                    object
                        .get(key)
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                };
                let display_name = text("name_en")
                    .or_else(|| text("name_cn"))
                    .or_else(|| text("name"))
                    .unwrap_or_else(|| subject.clone());
                return Some(Identity {
                    subject,
                    display_name,
                    email: text("email"),
                    avatar_url: text("avatar_url"),
                });
            }
            object.values().find_map(parse_identity)
        }
        serde_json::Value::Array(items) => items.iter().find_map(parse_identity),
        _ => None,
    }
}

fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_identity_requires_user_key() {
        let nested = serde_json::json!({
            "data": [{ "user_key": "7574", "name_en": "Shi Eng", "email": "a@b.c" }]
        });
        let identity = parse_identity(&nested).unwrap();
        assert_eq!(identity.subject, "7574");
        assert_eq!(identity.display_name, "Shi Eng");
        assert_eq!(identity.email.as_deref(), Some("a@b.c"));

        let numeric = serde_json::json!({ "user_key": 42 });
        assert_eq!(parse_identity(&numeric).unwrap().display_name, "42");

        for missing in [
            serde_json::json!({ "name_en": "No Key", "email": "a@b.c" }),
            serde_json::json!({ "user_key": "" }),
            serde_json::json!([]),
        ] {
            assert!(parse_identity(&missing).is_none(), "{missing}");
        }
    }
}
