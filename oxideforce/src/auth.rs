use anyhow::Result;
use reqwest::{Client, Url};
use serde_derive::Deserialize;

use crate::SalesforceError;

#[derive(Clone)]
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

#[derive(Clone)]
pub struct RefreshTokenAuth {
    refresh_token: String,
    instance_url: String,
    access_token: Option<String>,
    app: ConnectedApp,
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

impl RefreshTokenAuth {
    pub async fn refresh_access_token(&mut self) -> Result<()> {
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
        self.instance_url = result.instance_url;

        Ok(())
    }
}

#[derive(Clone)]
pub struct JwtAuth {
    access_token: Option<String>,
    instance_url: String,
    app: ConnectedApp,
    cert: String,
}

impl JwtAuth {
    pub async fn refresh_access_token(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct UsernamePasswordAuth {
    username: String,
    password: String,
    security_token: Option<String>,
    app: ConnectedApp,
    access_token: Option<String>,
    instance_url: String,
}

impl UsernamePasswordAuth {
    pub fn new(
        username: String,
        password: String,
        security_token: Option<String>,
        app: ConnectedApp,
        instance_url: String,
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
    pub async fn refresh_access_token(&mut self) -> Result<()> {
        self.access_token = None;

        let url = format!("{}/services/oauth2/token", self.instance_url);
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
            .await?;

        self.access_token = Some(result.access_token);
        self.instance_url = result.instance_url;

        Ok(())
    }
}

#[derive(Clone)]
pub struct AccessTokenAuth {
    access_token: String,
    instance_url: String,
}

#[derive(Clone)]
pub enum AuthDetails {
    AccessToken(AccessTokenAuth),
    RefreshToken(RefreshTokenAuth),
    Jwt(JwtAuth),
    UsernamePassword(UsernamePasswordAuth),
}

impl AuthDetails {
    pub async fn refresh_access_token(&mut self) -> Result<()> {
        match self {
            AuthDetails::RefreshToken(tok) => Ok(tok.refresh_access_token().await?),
            AuthDetails::Jwt(tok) => Ok(tok.refresh_access_token().await?),
            AuthDetails::UsernamePassword(tok) => Ok(tok.refresh_access_token().await?),
            AuthDetails::AccessToken(_) => Err(SalesforceError::CannotRefresh.into()),
        }
    }

    pub fn get_instance_url(&self) -> &str {
        match self {
            AuthDetails::RefreshToken(tok) => &tok.instance_url,
            AuthDetails::Jwt(tok) => &tok.instance_url,
            AuthDetails::UsernamePassword(tok) => &tok.instance_url,
            AuthDetails::AccessToken(tok) => &tok.instance_url,
        }
    }

    pub fn get_access_token(&self) -> Option<&String> {
        match self {
            AuthDetails::RefreshToken(tok) => tok.access_token.as_ref(),
            AuthDetails::Jwt(tok) => tok.access_token.as_ref(),
            AuthDetails::UsernamePassword(tok) => tok.access_token.as_ref(),
            AuthDetails::AccessToken(tok) => Some(&tok.access_token),
        }
    }
}
