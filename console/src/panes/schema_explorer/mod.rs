use baris::{prelude::Connection, rest::describe::SObjectDescribe};
use iced::{
    widget::{column, container, pick_list, row, scrollable, text, Component, Rule, Space, Text},
    Command, Element, Length, Renderer,
};
use serde_derive::Serialize;

use crate::Message;

pub struct SchemaExplorer<Message, Identifier> {
    id: Identifier,
    conn: Connection,
    do_job: Box<dyn Fn(Command<Message>) -> Message>,
    do_future: Box<dyn Fn(Identifier, Command<Event>) -> Message>,
}

#[derive(Serialize, Default)]
pub struct SchemaExplorerState {
    all_sobjects: Option<Vec<String>>,
    selected_sobject: Option<String>,
    #[serde(skip)]
    sobject_schema: Option<SObjectDescribe>,
}

enum Event {
    SObjectSelected(String),
    SObjectsLoaded(Vec<String>),
    SObjectSchemaLoaded(SObjectDescribe),
}

impl<Identifier> SchemaExplorer<Message, Identifier> {
    pub fn new(
        id: Identifier,
        conn: &Connection,
        do_job: impl Fn(Command<Message>) -> Message,
        do_future: impl Fn(Identifier, Command<Event>) -> Message,
    ) -> Self {
        Self {
            id: id,
            conn: conn.clone(),
            do_job: Box::new(do_job),
            do_future: Box::new(do_future),
        }
    }
}

impl<Identifier> Component<Message, Renderer> for SchemaExplorer<Identifier, Message> {
    type Event = Event;
    type State = SchemaExplorerState;

    fn update(&mut self, state: &mut Self::State, event: Self::Event) -> Option<Message> {
        match event {
            Event::SObjectSelected(object_name) => {
                let on = object_name.clone();
                let future = async move { self.conn.get_type(&on).await };
                state.selected_sobject = Some(object_name);
                state.sobject_schema = None;
                Some((self.do_future)(Command::perform(future, |f| {
                    Event::SObjectSchemaLoaded(f.unwrap().get_describe().clone())
                })))
            }
            Event::SObjectsLoaded(sobjects) => {
                state.all_sobjects = Some(sobjects);
                None
            }
            Event::SObjectSchemaLoaded(describe) => {
                state.sobject_schema = Some(describe);
                None
            }
        }
    }

    fn view(&self, state: &Self::State) -> Element<'_, Self::Event, Renderer> {
        if let Some(sobject_names) = &state.all_sobjects {
            container(column![
                Text::new("Select an sObject to explore its schema"),
                Space::with_height(8),
                pick_list(
                    sobject_names,
                    state.selected_sobject.clone(),
                    Event::SObjectSelected
                )
                .placeholder("Select..."),
                Space::with_height(8),
                if let Some(schema) = &state.sobject_schema {
                    let mut field_label_col = column![text("Label")];
                    let mut field_api_name_col = column![text("API Name")];
                    let mut field_type_name_col = column![text("Type")];
                    let mut field_nullable_col = column![text("Nullable")];

                    for f in schema.fields.iter() {
                        field_label_col = field_label_col.push(text(f.label.clone()));
                        field_api_name_col = field_api_name_col.push(text(f.name.clone()));
                        field_type_name_col = field_type_name_col.push(text(f.field_type.clone()));
                        field_nullable_col = field_nullable_col.push(text(f.nillable));
                    }
                    column![
                        row![
                            column![text("Label"), text("API Name"), text("Key Prefix"),],
                            column![
                                text(schema.label.clone()),
                                text(schema.name.clone()),
                                text(schema.key_prefix.clone().unwrap_or("None".to_string())),
                            ],
                            column![
                                text("Createable"),
                                text("Readable"),
                                text("Updateable"),
                                text("Deletable"),
                                text("Queryable"),
                                text("Searchable")
                            ],
                            column![
                                text(schema.createable),
                                text(true),
                                text(schema.updateable),
                                text(schema.deletable),
                                text(schema.queryable),
                                text(schema.searchable)
                            ]
                        ]
                        .spacing(8),
                        // Field details
                        row![text("Fields")],
                        Space::with_height(4),
                        Rule::horizontal(1),
                        Space::with_height(4),
                        scrollable(container(
                            row![
                                field_label_col,
                                field_api_name_col,
                                field_type_name_col,
                                field_nullable_col
                            ]
                            .spacing(8)
                        ))
                    ]
                    .into()
                } else {
                    if let Some(name) = &state.selected_sobject {
                        column![text(format!("Loading schema details for {0}", name))]
                    } else {
                        column![]
                    }
                },
                Space::with_height(8)
            ])
            .width(Length::Fill)
            .into()
        } else {
            row![Text::new("Loading schema information")].into()
        }
    }
}
