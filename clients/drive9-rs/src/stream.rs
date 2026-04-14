use crate::client::Client;
use crate::error::Drive9Error;
use crate::models::{CompletePart, UploadPlanV2};
use std::collections::HashMap;
use tokio::sync::{Mutex, Semaphore};

const UPLOAD_MAX_CONCURRENCY: usize = 16;

pub struct StreamWriter {
    client: Client,
    path: String,
    total_size: i64,
    expected_revision: i64,
    state: std::sync::Arc<Mutex<StreamState>>,
    sem: Semaphore,
}

struct StreamState {
    plan: Option<UploadPlanV2>,
    uploaded: HashMap<i32, CompletePart>,
    inflight: usize,
    err: Option<Drive9Error>,
    started: bool,
    completed: bool,
    aborted: bool,
    closing: bool,
}

impl StreamWriter {
    pub(crate) fn new(
        client: Client,
        path: String,
        total_size: i64,
        expected_revision: i64,
    ) -> Self {
        Self {
            client,
            path,
            total_size,
            expected_revision,
            state: std::sync::Arc::new(Mutex::new(StreamState {
                plan: None,
                uploaded: HashMap::new(),
                inflight: 0,
                err: None,
                started: false,
                completed: false,
                aborted: false,
                closing: false,
            })),
            sem: Semaphore::new(UPLOAD_MAX_CONCURRENCY),
        }
    }

    pub async fn started(&self) -> bool {
        self.state.lock().await.started
    }

    async fn init_locked(&self, state: &mut StreamState) -> Result<(), Drive9Error> {
        if state.started {
            return Ok(());
        }
        let plan = self
            .client
            .initiate_upload_v2(&self.path, self.total_size, self.expected_revision)
            .await
            .map_err(|e| {
                let msg = format!("initiate stream upload: {}", e);
                if msg.contains("v2 protocol")
                    || format!("{}", e).contains("v2 upload API not available")
                {
                    Drive9Error::Other(
                        "streaming upload requires v2 protocol: v2 upload API not available"
                            .to_string(),
                    )
                } else {
                    Drive9Error::Other(msg)
                }
            })?;
        state.plan = Some(plan);
        state.started = true;
        Ok(())
    }

    pub async fn write_part(&self, part_num: i32, data: Vec<u8>) -> Result<(), Drive9Error> {
        let mut state = self.state.lock().await;
        if let Some(ref e) = state.err {
            return Err(Drive9Error::Other(format!("{}", e)));
        }
        if state.completed {
            return Err(Drive9Error::Other(
                "stream writer already completed".to_string(),
            ));
        }
        if state.aborted {
            return Err(Drive9Error::Other(
                "stream writer already aborted".to_string(),
            ));
        }
        if state.closing {
            return Err(Drive9Error::Other("stream writer is closing".to_string()));
        }
        self.init_locked(&mut state).await?;
        let plan = state.plan.clone().unwrap();
        state.inflight += 1;
        drop(state);

        let _permit = self.sem.acquire().await.unwrap();
        let client = self.client.clone();
        let data = data.clone();
        let upload_id = plan.upload_id;

        // Note: we spawn but do not await here; fire-and-forget like Go.
        // The caller must later call complete()/abort() which wait for inflight.
        let this_state = std::sync::Arc::clone(&self.state);
        tokio::spawn(async move {
            let result = async {
                let pp = client.presign_one_part(&upload_id, part_num).await?;
                let etag = client.upload_one_part_v2(&upload_id, &pp, &data).await?;
                Ok::<String, Drive9Error>(etag)
            }
            .await;
            let mut s = this_state.lock().await;
            s.inflight -= 1;
            match result {
                Ok(etag) => {
                    s.uploaded.insert(
                        part_num,
                        CompletePart {
                            number: part_num,
                            etag,
                        },
                    );
                }
                Err(e) => {
                    if s.err.is_none() {
                        s.err = Some(e);
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn complete(
        &self,
        final_part_num: i32,
        final_part_data: Vec<u8>,
    ) -> Result<(), Drive9Error> {
        {
            let mut state = self.state.lock().await;
            if state.completed {
                return Err(Drive9Error::Other(
                    "stream writer already completed".to_string(),
                ));
            }
            if state.aborted {
                return Err(Drive9Error::Other(
                    "stream writer already aborted".to_string(),
                ));
            }
            if state.closing {
                return Err(Drive9Error::Other("stream writer is closing".to_string()));
            }
            state.closing = true;
        }
        self.wait_inflight().await;

        {
            let state = self.state.lock().await;
            if let Some(ref e) = state.err {
                return Err(Drive9Error::Other(format!("{}", e)));
            }
            if !state.started || state.plan.is_none() {
                return Err(Drive9Error::Other(
                    "stream writer was never started".to_string(),
                ));
            }
        }

        if !final_part_data.is_empty() {
            let upload_id = {
                let state = self.state.lock().await;
                state.plan.as_ref().unwrap().upload_id.clone()
            };
            let pp = self
                .client
                .presign_one_part(&upload_id, final_part_num)
                .await?;
            let etag = self
                .client
                .upload_one_part_v2(&upload_id, &pp, &final_part_data)
                .await?;
            let mut state = self.state.lock().await;
            state.uploaded.insert(
                final_part_num,
                CompletePart {
                    number: final_part_num,
                    etag,
                },
            );
        }

        let (upload_id, parts) = {
            let mut state = self.state.lock().await;
            if state.uploaded.is_empty() {
                return Err(Drive9Error::Other(
                    "no parts uploaded in stream upload".to_string(),
                ));
            }
            let max_part = *state.uploaded.keys().max().unwrap();
            let mut parts = vec![];
            for i in 1..=max_part {
                let cp = state.uploaded.get(&i).cloned().ok_or_else(|| {
                    Drive9Error::Other(format!(
                        "missing part {} in stream upload (have {} parts, max {})",
                        i,
                        state.uploaded.len(),
                        max_part
                    ))
                })?;
                parts.push(cp);
            }
            state.completed = true;
            (state.plan.as_ref().unwrap().upload_id.clone(), parts)
        };
        self.client.complete_upload_v2(&upload_id, &parts).await
    }

    pub async fn abort(&self) -> Result<(), Drive9Error> {
        {
            let mut state = self.state.lock().await;
            if state.aborted {
                return Ok(());
            }
            state.closing = true;
        }
        self.wait_inflight().await;
        let upload_id = {
            let mut state = self.state.lock().await;
            state.aborted = true;
            if !state.started {
                return Ok(());
            }
            state.plan.as_ref().unwrap().upload_id.clone()
        };
        self.client.abort_upload_v2(&upload_id).await
    }

    async fn wait_inflight(&self) {
        loop {
            {
                let state = self.state.lock().await;
                if state.inflight == 0 {
                    break;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
}
