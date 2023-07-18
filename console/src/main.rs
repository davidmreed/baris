use baris::prelude::*;
use iced::alignment::Horizontal;
use iced::widget::{
    button, column, container, row, scrollable, text, Button, Component, Rule, Space, Text,
};
use iced::{executor, Alignment, Application, Command, Element, Length, Renderer, Settings, Theme};
use panes::anon_apex::AnonymousApex;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;

use anyhow::Result;
use baris::{api::Connection, auth::AccessTokenAuth};
use reqwest::Url;
use std::env;

mod panes;

use panes::schema_explorer::SchemaExplorer;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum JobType {
    AsynchronousApex(String),
    QueryData(String, String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AsynchronousJobRequest {
    description: String,
    job: JobType,
}

impl AsynchronousJobRequest {
    pub async fn execute(&self, org: &Connection) {
        match self.job {
            JobType::AsynchronousApex(anon_apex) => org.execute_anonymous(anon_apex).await,
            JobType::QueryData(_, _) => {}
        }
    }
}

pub fn get_test_connection() -> Result<Connection> {
    let access_token = env::var("SESSION_ID")?;
    let instance_url = env::var("INSTANCE_URL")?;

    Connection::new(
        Box::new(AccessTokenAuth::new(
            access_token,
            Url::parse(&instance_url)?,
        )),
        "v58.0",
    )
}

struct OrganizationSobject {
    instance_name: String,
    is_sandbox: bool,
    name: String,
    namespace_prefix: String,
    organization_type: String,
    id: SalesforceId,
    trial_expiration_date: DateTime,
}

#[derive(Serialize, Deserialize)]
struct IdOnlySObject {
    id: SalesforceId,
}

impl SObjectWithId for IdOnlySObject {
    fn get_id(&self) -> FieldValue {
        FieldValue::Id(self.id.clone())
    }

    fn set_id(&mut self, id: FieldValue) -> Result<()> {
        panic!("Not supported")
    }
}

impl TypedSObject for IdOnlySObject {
    fn get_api_name(&self) -> &str {
        "Foo"
    }
}

impl SObjectBase for IdOnlySObject {}

impl SObjectDeserialization for IdOnlySObject {
    fn from_value(
        value: &serde_json::value::Value,
        _: &SObjectType,
    ) -> Result<Self, anyhow::Error> {
        Ok(serde_json::from_value::<IdOnlySObject>(value.clone())?)
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
enum Tab {
    Orgs,
    Jobs,
    SchemaExplorer,
    AnonymousApex,
    QueryData,
    DeleteData,
}

#[derive(Debug, Clone)]
enum JobStatus {
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
enum Message {
    TabSelected(Tab),
    SelectOrg(String),
    AuthorizeOrg,
    StartJob(AsynchronousJobRequest),
    JobCompleted(String, JobStatus),
    ComponentCommand(Tab, Command<T>), // ??? How do I type this T?)
                                       // can I walk the retained element tree?
}

struct AppState {
    tab: Tab,
    orgs: HashMap<String, Connection>,
    jobs: HashMap<String, (String, JobStatus)>,
    job_count: u16,
    selected_org: Option<String>,
    // Query Data state
    soql_query: String,
    query_object: Option<String>,
    // Delete Data state
    delete_soql_query: String,
    delete_object: Option<String>,
    anon_apex: Option<AnonymousApex>,
    schema_explorer: Option<SchemaExplorer<Message>>,
    state: HashMap<Tab, serde_json::Value>,
}

impl Application for AppState {
    type Message = Message;
    type Executor = executor::Default;
    type Theme = Theme;
    type Flags = ();

    fn new(_flags: ()) -> (Self, Command<Message>) {
        let mut orgs = HashMap::new();
        orgs.insert(
            "David's Scratch Org".to_owned(),
            get_test_connection().unwrap(),
        );

        (
            Self {
                tab: Tab::Orgs,
                orgs: orgs,
                jobs: HashMap::new(),
                job_count: 0,
                selected_org: Some("David's Scratch Org".to_string()),
                soql_query: "".to_string(),
                query_object: None,
                delete_soql_query: "".to_string(),
                delete_object: None,
                state: HashMap::new(),
            },
            Command::none(),
        )
    }

    fn title(&self) -> String {
        String::from("Salesforce Data Console")
    }

    fn update(&mut self, message: Message) -> Command<Message> {
        match message {
            Message::TabSelected(tab) => {
                self.tab = tab;
                Command::none()
            }
            Message::StartJob(job) => {
                let job_id = self.get_next_job_id();
                self.jobs.insert(
                    job_id.clone(),
                    (job.description.clone(), JobStatus::InProgress),
                );

                Command::perform(job.execute(), |result: Result<(), anyhow::Error>| {
                    Message::JobCompleted(
                        job_id,
                        if result.is_ok() {
                            JobStatus::Completed
                        } else {
                            JobStatus::Failed
                        },
                    )
                })
            }
            Message::JobCompleted(job_id, job_status) => {
                let (job_name, _) = self.jobs.get(&job_id).unwrap();
                self.jobs.insert(job_id, (job_name.clone(), job_status));
                Command::none()
            }
            Message::AuthorizeOrg => Command::none(),
            // Message::RunQueryData => {
            //     let soql_query = self.soql_query.clone();
            //     let query_object = self.query_object.clone().unwrap();
            //     let org: Connection = self.get_selected_org();
            //     let future = async move {
            //         let query_job: BulkQueryJob =
            //             BulkQueryJob::create(&org, &soql_query, true).await?;
            //         let query_job = query_job.complete(&org).await?;

            //         let mut writer = Writer::from_path("Query.csv")?;
            //         let mut headers: Option<Vec<String>> = None;

            //         query_job
            //             .get_results_stream::<SObject>(&org, &org.get_type(&query_object).await?)
            //             .await
            //             .map(move |rec| {
            //                 let rec = rec.unwrap();

            //                 if headers.is_none() {
            //                     let mut keys = Vec::new();
            //                     for f in rec.fields.keys() {
            //                         keys.push(f.clone());
            //                         writer.write_field(f).unwrap();
            //                     }
            //                     headers = Some(keys);
            //                     writer.write_record(None::<&[u8]>).unwrap();
            //                 }

            //                 if let Some(headers) = &headers {
            //                     for f in headers {
            //                         writer
            //                             .write_field(rec.get(&f).unwrap().as_string())
            //                             .unwrap();
            //                     }
            //                     writer.write_record(None::<&[u8]>).unwrap();
            //                 }
            //             })
            //             .collect::<Vec<()>>()
            //             .await;

            //         Ok(())
            //     };
            //     let job_id = self.get_next_job_id();
            //     self.jobs
            //         .insert(job_id.clone(), ("Query Data".into(), JobStatus::InProgress));

            //     Command::perform(future, |result: Result<(), anyhow::Error>| {
            //         Message::JobCompleted(
            //             job_id,
            //             if result.is_ok() {
            //                 JobStatus::Completed
            //             } else {
            //                 JobStatus::Failed
            //             },
            //         )
            //     })
            // }
            // Message::RunDeleteData => {
            //     let org: Connection = self.get_selected_org();
            //     let delete_soql_query = self.delete_soql_query.clone();
            //     let delete_soql_object = self.delete_object.clone().unwrap();
            //     let future = async move {
            //         let query_job = BulkQueryJob::create(
            //             &org,
            //             &format!(
            //                 "SELECT Id FROM {} {}",
            //                 delete_soql_object,
            //                 if delete_soql_query.len() > 0 {
            //                     format!("WHERE {}", delete_soql_query)
            //                 } else {
            //                     "".to_string()
            //                 }
            //             ),
            //             true,
            //         )
            //         .await?;

            //         let query_job = query_job.complete(&org).await?;

            //         let delete_job: BulkDmlJob = BulkDmlJob::create(
            //             &org,
            //             BulkApiDmlOperation::Delete,
            //             delete_soql_object.clone(),
            //         )
            //         .await?;
            //         let result_stream = query_job
            //             .get_results_stream::<SObject>(
            //                 &org,
            //                 &org.get_type(&delete_soql_object).await?,
            //             )
            //             .await
            //             .map(Result::unwrap)
            //             .map(|s| IdOnlySObject {
            //                 id: match s.get("Id").unwrap() {
            //                     FieldValue::Id(id) => id.clone(),
            //                     _ => panic!("Not an id"),
            //                 },
            //             });

            //         delete_job.ingest(&org, result_stream).await?;
            //         delete_job.close(&org).await?;
            //         delete_job.complete(&org).await
            //     };
            //     let job_id = self.get_next_job_id();
            //     self.jobs.insert(
            //         job_id.clone(),
            //         ("Delete Data".into(), JobStatus::InProgress),
            //     );

            //     Command::perform(future, |result| {
            //         Message::JobCompleted(
            //             job_id,
            //             if result.is_ok() {
            //                 JobStatus::Completed
            //             } else {
            //                 JobStatus::Failed
            //             },
            //         )
            //     })
            // }
        }
    }

    fn view(&self) -> Element<Message> {
        let buttons = row![
            button("My Orgs").on_press(Message::TabSelected(Tab::Orgs)),
            button("Jobs").on_press(Message::TabSelected(Tab::Jobs)),
            Rule::vertical(2),
            container(
                row![
                    button("Anonymous Apex").on_press(Message::TabSelected(Tab::AnonymousApex)),
                    button("Schema Explorer").on_press(Message::TabSelected(Tab::SchemaExplorer)),
                    button("Query Data").on_press(Message::TabSelected(Tab::QueryData)),
                    button("Delete Data").on_press(Message::TabSelected(Tab::DeleteData))
                ]
                .spacing(32)
                .align_items(Alignment::Center)
            )
            .width(Length::Fill)
            .align_x(Horizontal::Center)
        ]
        .align_items(Alignment::End)
        .spacing(4)
        .height(56);

        row![
            Space::with_width(Length::FillPortion(1)),
            column![
                Space::with_height(8),
                buttons,
                Space::with_height(8),
                Rule::horizontal(2),
                Space::with_height(16),
                self.get_panel_content(),
                Space::with_height(8)
            ]
            .width(Length::FillPortion(8)),
            Space::with_width(Length::FillPortion(1))
        ]
        .width(Length::Fill)
        .align_items(Alignment::Center)
        .into()
    }
}

impl AppState {
    fn get_next_job_id(&mut self) -> String {
        self.job_count += 1;
        self.job_count.to_string()
    }

    fn get_selected_org(&self) -> Connection {
        self.orgs
            .get(&self.selected_org.clone().unwrap())
            .unwrap()
            .clone()
    }

    fn get_state(&self, tab: Tab) -> Option<&Value> {
        return self.state.get(&tab);
    }

    fn get_panel_content(&self) -> Element<Message> {
        match &self.tab {
            Tab::Orgs => {
                let mut org_buttons = column![
                    row![
                        Text::new(format!(
                            "You have {} authorized {}. Click an org to activate it.",
                            self.orgs.len(),
                            if self.orgs.len() == 1 { "org" } else { "orgs" }
                        )),
                        Space::with_width(16),
                        Button::new("Authorize a new org").on_press(Message::AuthorizeOrg)
                    ],
                    Space::with_height(8)
                ];
                for org in &self.orgs {
                    org_buttons = org_buttons.push(row![
                        text(org.0.as_str()),
                        Button::new("Deauthorize"),
                        Button::new("Info")
                    ]);
                }
                org_buttons.into()
            }
            Tab::Jobs => {
                let mut job_col = column![text("Job")];
                let mut status_col = column![text("Status")];

                for job in self.jobs.iter() {
                    job_col = job_col.push(text(job.1 .0.clone()));
                    status_col = status_col.push(text(format!("{:?}", job.1 .1)));
                }

                column![
                    row![text("Jobs")],
                    Space::with_height(4),
                    Rule::horizontal(1),
                    Space::with_height(4),
                    scrollable(row![job_col, status_col].spacing(8))
                ]
                .into()
            }
            Tab::SchemaExplorer => {
                let state = self.get_state(Tab::SchemaExplorer);

                if let Some(state) = state {
                    let parsed_state = serde_json::from_value::<
                        <SchemaExplorer as iced::widget::Component<Message, Renderer>>::State,
                    >(*state);

                    if let Ok(state) = parsed_state {
                        return SchemaExplorer::new(&self.get_selected_org()).view(&state);
                    }
                }

                return SchemaExplorer::new(&self.get_selected_org())
                    .view(&SchemaExplorer::State::default());
            }
            Tab::AnonymousApex => AnonymousApex::new(&self.get_selected_org())
                .view(self.get_state(Tab::AnonymousApex).into()),
            Tab::DeleteData => column![
                // if let Some(sobject_names) = &self.all_sobjects {
                //     column![
                //         Text::new("Select an sObject to delete data"),
                //         Space::with_height(8),
                //         pick_list(
                //             sobject_names,
                //             self.delete_object.clone(),
                //             Message::DeleteObjectChanged
                //         )
                //         .placeholder("Select..."),
                //         Space::with_height(8),
                //     ]
                // } else {
                //     column![
                //         Text::new("Loading schema information"),
                //         Space::with_height(8),
                //     ]
                // },
                // text_input("Enter SOQL WHERE clause here", &self.delete_soql_query)
                //     .on_input(Message::DeleteInputChanged),
                // Space::with_height(8),
                // if self.delete_object.is_some() {
                //     button("Execute").on_press(Message::RunDeleteData)
                // } else {
                //     button("Execute")
                // }
            ]
            .into(),
            Tab::QueryData => column![
                // if let Some(sobject_names) = &self.all_sobjects {
                //     column![
                //         Text::new("Select an sObject to query data"),
                //         Space::with_height(8),
                //         pick_list(
                //             sobject_names,
                //             self.query_object.clone(),
                //             Message::QueryObjectChanged
                //         )
                //         .placeholder("Select..."),
                //         Space::with_height(8),
                //     ]
                // } else {
                //     column![
                //         Text::new("Loading schema information"),
                //         Space::with_height(8),
                //     ]
                // },
                // text_input("Enter SOQL query here", &self.soql_query)
                //     .on_input(Message::QueryInputChanged),
                // Space::with_height(8),
                // if self.query_object.is_some() {
                //     button("Execute").on_press(Message::RunQueryData)
                // } else {
                //     button("Execute")
                // }
            ]
            .into(),
            _ => row![].into(),
        }
    }
}
pub fn main() -> iced::Result {
    AppState::run(Settings::default())
}
