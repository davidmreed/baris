use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::{Error, Result};
use serde_json::{Map, Value};
use tokio::task::JoinHandle;
use tokio_stream::Stream;

use crate::{data::SObjectRepresentation, FieldValue, SObjectType};

pub fn value_from_csv(rec: &HashMap<String, String>, sobjecttype: &SObjectType) -> Result<Value> {
    let mut ret = Map::new();

    for k in rec.keys() {
        // Get the describe for this field.
        if k != "attributes" {
            let describe = sobjecttype.get_describe().get_field(k).unwrap();
            let f = &FieldValue::from_str(rec.get(k).unwrap(), &describe.soap_type)?;
            ret.insert(k.to_lowercase(), f.into());
        }
    }

    Ok(Value::Object(ret))
}

pub(crate) trait BufferedLocatorManager: Send + Sync {
    type Output: SObjectRepresentation;

    fn get_next_future(
        &mut self,
        state: Option<BufferedLocatorStreamState<Self::Output>>,
    ) -> JoinHandle<Result<BufferedLocatorStreamState<Self::Output>>>;
}

pub(crate) struct BufferedLocatorStreamState<T: SObjectRepresentation> {
    pub buffer: VecDeque<T>, // TODO: we should decouple the buffer from the locator state to enable prefetching
    pub locator: Option<String>,
    pub total_size: Option<usize>,
    pub done: bool,
}

impl<T> BufferedLocatorStreamState<T>
where
    T: SObjectRepresentation,
{
    pub fn new(
        buffer: VecDeque<T>,
        locator: Option<String>,
        total_size: Option<usize>,
        done: bool,
    ) -> BufferedLocatorStreamState<T> {
        BufferedLocatorStreamState {
            buffer,
            locator,
            total_size,
            done,
        }
    }
}

pub struct BufferedLocatorStream<T: SObjectRepresentation + Unpin> {
    manager: Box<dyn BufferedLocatorManager<Output = T>>,
    state: Option<BufferedLocatorStreamState<T>>,
    yielded: usize,
    error: Option<Error>, // TODO
    retrieve_task: Option<JoinHandle<Result<BufferedLocatorStreamState<T>>>>,
}

impl<T> BufferedLocatorStream<T>
where
    T: SObjectRepresentation + Unpin,
{
    pub(crate) fn new(
        initial_values: Option<BufferedLocatorStreamState<T>>,
        manager: Box<dyn BufferedLocatorManager<Output = T>>,
    ) -> Self {
        BufferedLocatorStream {
            manager,
            state: initial_values,
            retrieve_task: None,
            yielded: 0,
            error: None,
        }
    }

    fn try_to_yield(&mut self) -> Option<T> {
        if let Some(state) = &mut self.state {
            if let Some(item) = state.buffer.pop_front() {
                self.yielded += 1;
                Some(item)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<T> Stream for BufferedLocatorStream<T>
where
    T: SObjectRepresentation + Unpin,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // First, check if we have sObjects ready to yield.
            let sobject = self.try_to_yield();
            if let Some(sobject) = sobject {
                return Poll::Ready(Some(Ok(sobject)));
            } else if let Some(task) = &mut self.retrieve_task {
                // We have a task waiting already.
                // TODO: can we replace this task with a channel?
                let fut = unsafe { Pin::new_unchecked(task) };
                let poll = fut.poll(cx);
                if let Poll::Ready(result) = poll {
                    self.state = Some(result??);

                    self.retrieve_task = None;
                } else {
                    return Poll::Pending;
                }
            } else if let Some(state) = &self.state {
                if state.done {
                    // If we are done, return a sigil.
                    return Poll::Ready(None);
                }
            } else {
                // Create a new task to get the next state.
                let state = mem::take(&mut self.state);
                self.retrieve_task = Some(self.manager.get_next_future(state));
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if let Some(state) = &self.state {
            if let Some(total_size) = &state.total_size {
                return (total_size - self.yielded, Some(total_size - self.yielded));
            }
        }

        (0, None)
    }
}
