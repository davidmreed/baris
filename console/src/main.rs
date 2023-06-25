use baris::prelude::*;
use iced::widget::{button, column, row, Button, Column, Rule, Space, Text};
use iced::{Alignment, Element, Sandbox, Settings};
use std::collections::HashMap;

struct OrganizationSobject {
    instance_name: String,
    is_sandbox: bool,
    name: String,
    namespace_prefix: String,
    organization_type: String,
    id: SalesforceId,
    trial_expiration_date: DateTime,
}

#[derive(Debug, Clone)]
enum Tab {
    Orgs,
    Jobs,
    SchemaExplorer,
    AnonymousApex,
    QueryData,
    DeleteData,
}

#[derive(Debug, Clone)]
enum Message {
    TabSelected(Tab),
    SelectOrg(String),
    AuthorizeOrg,
    RunAnonymousApex,
}

struct AppState {
    tab: Tab,
    orgs: Vec<String>,
    selected_org: Option<String>,
}

impl Sandbox for AppState {
    type Message = Message;

    fn new() -> Self {
        Self {
            tab: Tab::Orgs,
            orgs: vec!["Org".to_string()],
            selected_org: None,
        }
    }

    fn title(&self) -> String {
        String::from("Salesforce CIC")
    }

    fn update(&mut self, message: Message) {
        match message {
            Message::TabSelected(tab) => {
                self.tab = tab;
            }
            Message::SelectOrg(tab) => {
                self.selected_org = Some(tab);
            }
            Message::AuthorizeOrg => {}
            Message::RunAnonymousApex => {}
        }
    }

    fn view(&self) -> Element<Message> {
        let buttons = row![
            button("My Orgs").on_press(Message::TabSelected(Tab::Orgs)),
            button("Jobs").on_press(Message::TabSelected(Tab::Jobs)),
            Rule::vertical(2),
            button("Anonymous Apex").on_press(Message::TabSelected(Tab::AnonymousApex)),
            button("Schema Explorer").on_press(Message::TabSelected(Tab::SchemaExplorer)),
            button("Query Data").on_press(Message::TabSelected(Tab::QueryData)),
            button("Delete Data").on_press(Message::TabSelected(Tab::DeleteData))
        ]
        .spacing(4)
        .height(32);

        row![
            Space::with_width(8),
            column![
                Space::with_height(8),
                buttons,
                Space::with_height(4),
                Rule::horizontal(1),
                Space::with_height(4),
                self.get_panel_content()
            ],
            Space::with_width(8)
        ]
        .into()
    }
}

impl AppState {
    fn get_panel_content(&self) -> Element<Message> {
        match &self.tab {
            Tab::Orgs => {
                let mut org_buttons = column![
                    row![
                        Text::new("You have authorized orgs"),
                        Button::new("Authorize a new org").on_press(Message::AuthorizeOrg)
                    ],
                    Space::with_height(4)
                ];
                for org in &self.orgs {
                    let org_button =
                        Button::new(org.as_str()).on_press(Message::SelectOrg(org.to_string()));
                    org_buttons = org_buttons.push(org_button);
                }
                org_buttons.into()
            }
            Tab::Jobs => row![].into(),
            Tab::AnonymousApex => row![
                Text::new("").height(480),
                button("Execute").on_press(Message::RunAnonymousApex),
            ]
            .into(),
            _ => row![].into(),
        }
    }
}
pub fn main() -> iced::Result {
    AppState::run(Settings::default())
}
