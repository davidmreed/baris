use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::{Error, Result};
use tokio::task::JoinHandle;
use tokio_stream::Stream;

use crate::SObject;

trait BufferedLocatorManager {
    fn get_next_future(
        &mut self,
        state: Option<&BufferedLocatorStreamState>,
    ) -> JoinHandle<Result<BufferedLocatorStreamState>>;
}

struct BufferedLocatorStreamState {
    buffer: VecDeque<SObject>,
    locator: Option<String>,
    total_size: Option<usize>,
    done: bool,
}

impl BufferedLocatorStreamState {
    pub fn new(buffer: VecDeque<SObject>, locator: Option<String>, total_size: Option<usize>, done: bool) -> BufferedLocatorStreamState {
        BufferedLocatorStreamState {
            buffer, locator, total_size, done
        }
    }
}

struct BufferedLocatorStream
{
    manager: Box<dyn BufferedLocatorManager>,
    state: Option<BufferedLocatorStreamState>,
    yielded: usize,
    error: Option<Error>,
    retrieve_task: Option<JoinHandle<Result<BufferedLocatorStreamState>>>,
}

impl BufferedLocatorStream {
    fn new(initial_values: Option<BufferedLocatorStreamState>,  manager: Box<dyn BufferedLocatorManager>) -> Result<Self> {

        Ok(BufferedLocatorStream {
            manager,
            state: initial_values,
            retrieve_task: None,
            yielded: 0,
            error: None,
        })
    }

    fn try_to_yield(&mut self, state: Option<&mut BufferedLocatorStreamState>) -> Option<SObject> {
        if let Some(state) = state {
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

impl Stream for BufferedLocatorStream {
    type Item = Result<SObject>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // First, check if we have sObjects ready to yield.
            if let Some(sobject) = self.try_to_yield((&mut self.state).as_mut()) {
                return Poll::Ready(Some(Ok(sobject)));
            }

            // Check if we have a running task that is ready to yield a new state.
            if let Some(task) = &mut self.retrieve_task {
                // We have a task waiting already.
                let fut = unsafe { Pin::new_unchecked(task) };
                let poll = fut.poll(cx);
                if let Poll::Ready(result) = poll {
                    self.state = Some(result?);

                    self.retrieve_task = None;
                } else {
                    return Poll::Pending;
                }
            } else if let Some(state) = &self.state {
                if state.done {
                    // If we are done, return a sigil.
                    return Poll::Ready(None);
                } else {
                    // Create a new task to get the next state.
                    self.retrieve_task = Some(self.manager.get_next_future(Some(&state)));
                }
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
