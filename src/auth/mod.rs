use anyhow::Result;
use async_trait::async_trait;
use reqwest::{Client, Url};
use serde_derive::Deserialize;

use crate::SalesforceError;

#[cfg(test)]
mod test;

#[async_trait]
pub trait Authentication: Send + Sync {
    async fn refresh_access_token(&mut self) -> Result<()>;
    async fn get_instance_url(&self) -> Result<&Url>;
    fn get_access_token(&self) -> Option<&String>;
}

#[derive(Debug, Clone)]
pub struct ConnectedApp {
    consumer_key: String,
    client_secret: String,
    redirect_url: Option<Url>,
}

impl ConnectedApp {
    pub fn new(
        consumer_key: String,
        client_secret: String,
        redirect_url: Option<Url>,
    ) -> ConnectedApp {
        ConnectedApp {
            consumer_key,
            client_secret,
            redirect_url,
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    id: String,
    issued_at: String,
    instance_url: String,
    signature: String,
    access_token: String,
    token_type: String,
    scope: Option<String>,
}

#[derive(Clone)]
pub struct RefreshTokenAuth {
    refresh_token: String,
    instance_url: Url,
    access_token: Option<String>,
    app: ConnectedApp,
}

#[async_trait]
impl Authentication for RefreshTokenAuth {
    async fn refresh_access_token(&mut self) -> Result<()> {
        self.access_token = None;

        let url = format!("{}/services/oauth2/token", self.instance_url);

        let result: TokenResponse = Client::builder()
            .build()?
            .post(url)
            .form(&[
                ("client_id", &self.app.consumer_key),
                ("client_secret", &self.app.client_secret),
                ("grant_type", &"refresh_token".to_string()),
            ])
            .send()
            .await?
            .error_for_status()? // TODO: handle differently, parse error body
            .json()
            .await?;

        self.access_token = Some(result.access_token);
        self.instance_url = Url::parse(&result.instance_url)?;

        Ok(())
    }

    async fn get_instance_url(&self) -> Result<&Url> {
        Ok(&self.instance_url)
    }

    fn get_access_token(&self) -> Option<&String> {
        self.access_token.as_ref()
    }
}

#[derive(Clone)]
pub struct JwtAuth {
    access_token: Option<String>,
    instance_url: Url,
    app: ConnectedApp,
    cert: String,
}

#[async_trait]
impl Authentication for JwtAuth {
    async fn refresh_access_token(&mut self) -> Result<()> {
        todo!();
    }

    async fn get_instance_url(&self) -> Result<&Url> {
        Ok(&self.instance_url)
    }

    fn get_access_token(&self) -> Option<&String> {
        self.access_token.as_ref()
    }
}

#[derive(Clone)]
pub struct UsernamePasswordAuth {
    username: String,
    password: String,
    security_token: Option<String>,
    app: ConnectedApp,
    access_token: Option<String>,
    instance_url: Url,
}

impl UsernamePasswordAuth {
    pub fn new(
        username: String,
        password: String,
        security_token: Option<String>,
        app: ConnectedApp,
        instance_url: Url,
    ) -> UsernamePasswordAuth {
        UsernamePasswordAuth {
            username,
            password,
            security_token,
            app,
            instance_url,
            access_token: None,
        }
    }
}

#[async_trait]
impl Authentication for UsernamePasswordAuth {
    async fn refresh_access_token(&mut self) -> Result<()> {
        self.access_token = None;

        let url = self.instance_url.join("services/oauth2/token")?;
        let empty = "".to_string();
        let security_token = if let Some(security_token) = &self.security_token {
            security_token
        } else {
            &empty
        };

        let result: TokenResponse = Client::builder()
            .build()?
            .post(url)
            .form(&[
                // TODO: make this into a struct
                ("client_id", &self.app.consumer_key),
                ("client_secret", &self.app.client_secret),
                ("grant_type", &"password".to_string()),
                ("username", &self.username),
                ("password", &format!("{}{}", self.password, security_token)),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?; // TODO: is there a 200-with-error-body case?

        self.access_token = Some(result.access_token);
        self.instance_url = Url::parse(&result.instance_url)?;

        Ok(())
    }

    async fn get_instance_url(&self) -> Result<&Url> {
        // We may not yet be authenticated.
        if self.access_token.is_none() {
            return Err(SalesforceError::NotAuthenticated.into());
        }

        Ok(&self.instance_url)
    }

    fn get_access_token(&self) -> Option<&String> {
        self.access_token.as_ref()
    }
}

#[derive(Clone)]
pub struct AccessTokenAuth {
    access_token: String,
    instance_url: Url,
}

impl AccessTokenAuth {
    pub fn new(access_token: String, instance_url: Url) -> AccessTokenAuth {
        AccessTokenAuth {
            access_token,
            instance_url,
        }
    }
}

#[async_trait]
impl Authentication for AccessTokenAuth {
    async fn refresh_access_token(&mut self) -> Result<()> {
        Err(SalesforceError::CannotRefresh.into())
    }
    async fn get_instance_url(&self) -> Result<&Url> {
        Ok(&self.instance_url)
    }

    fn get_access_token(&self) -> Option<&String> {
        Some(&self.access_token)
    }
}
