use iced::{
    widget::{button, column, text_input, Component, Space},
    Command, Element, Renderer,
};
use serde_derive::Serialize;

use crate::{AsynchronousJobRequest, Connection, JobType};

pub struct AnonymousApex<Message, Identifier> {
    id: Identifier,
    conn: Connection,
    do_job: Box<dyn Fn(AsynchronousJobRequest) -> Message>,
}

#[derive(Serialize, Default)]
pub struct AnonymousApexState {
    anon_apex: String,
}

enum Event {
    InputChanged(String),
    RunAnonymousApex,
}

impl<Message, Identifier> AnonymousApex<Message, Identifier> {
    pub fn new(
        id: Identifier,
        conn: &Connection,
        do_job: impl Fn(AsynchronousJobRequest) -> Message,
    ) -> Self {
        Self {
            id: id,
            conn: conn.clone(),
            do_job: Box::new(do_job),
        }
    }
}

impl Component<Message, Renderer> for AnonymousApex<Message> {
    type Event = Event;
    type State = AnonymousApexState;

    fn update(&mut self, state: &mut Self::State, event: Self::Event) -> Option<Message> {
        match event {
            Event::InputChanged(content) => {
                state.anon_apex = content;
                None
            }
            Event::RunAnonymousApex => Some((self.do_job)(AsynchronousJobRequest {
                description: "Anonymous Apex".to_string(),
                job: JobType::AsynchronousApex(state.anon_apex.clone()),
            })),
        }
    }

    fn view(&self, state: &Self::State) -> Element<'_, Self::Event, Renderer> {
        column![
            text_input("Enter Anonymous Apex here", &self.anon_apex).on_input(Event::InputChanged),
            Space::with_height(8),
            button("Execute").on_press(Event::RunAnonymousApex),
        ]
        .into()
    }
}
