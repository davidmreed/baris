use crate::{api::Connection, data::FieldValue, data::SObjectRepresentation};

use anyhow::Result;
use async_trait::async_trait;

use super::{
    SObjectCollectionCreateRequest, SObjectCollectionDeleteRequest, SObjectCollectionUpdateRequest,
    SObjectCollectionUpsertRequest,
};

#[async_trait]
pub trait SObjectCollectionCreateable {
    fn create_request(&self, all_or_none: bool) -> Result<SObjectCollectionCreateRequest>;
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
}

#[async_trait]
pub trait SObjectCollectionUpdateable {
    fn update_request(&self, all_or_none: bool) -> Result<SObjectCollectionUpdateRequest>;
    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
}

#[async_trait]
pub trait SObjectCollectionUpsertable {
    fn upsert_request(&self, external_id: String, all_or_none: bool) -> Result<SObjectCollectionUpsertRequest>;
    async fn upsert(
        &mut self,
        conn: &Connection,
        external_id: String,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>>;
}

#[async_trait]
pub trait SObjectCollectionDeleteable {
    fn delete_request(&self, all_or_none: bool) -> Result<SObjectCollectionDeleteRequest>;
    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
}


// TODO: Can we implement for &mut [T] and take advantage of Vec's DerefMut?
#[async_trait]
impl<T> SObjectCollectionCreateable for Vec<T>
where
    T: SObjectSerialization
{
    fn create_request(&self, all_or_none: bool) -> Result<SObjectCollectionCreateRequest> {
        SObjectCollectionCreateRequest::new(self, all_or_none)
    }

    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        Ok(conn
            .execute(&self.create_request(all_or_none)?)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                if r.success {
                    self.get_mut(i)
                        .unwrap()
                        .set_id(FieldValue::Id(r.id.unwrap()));
                }

                r.into()
            })
            .collect())
    }
}

#[async_trait]
impl<T> SObjectCollectionUpdateable for Vec<T> where T: SObjectSerialization {
    fn update_request(&self, all_or_none: bool) -> Result<SObjectCollectionUpdateRequest> {
        SObjectCollectionUpdateRequest::new(self, all_or_none)
    }

    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        Ok(conn
            .execute(&self.update_request(all_or_none)?)
            .await?
            .into_iter()
            .map(|r| r.into())
            .collect())
    }
}

#[async_trait]
impl<T> SObjectCollectionUpsertable for Vec<T> where T: SObjectSerialization {
    fn upsert_request(&self, external_id: String, all_or_none: bool) -> Result<SObjectCollectionUpdateRequest> {
        SObjectCollectionUpsertRequest::new(self, external_id, all_or_none)
    }

    async fn upsert(
        &mut self,
        conn: &Connection,
        external_id: String,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionUpsertRequest::new(self, external_id, all_or_none)?;
        Ok(conn
            .execute(&self.upsert_request(external_id, all_or_none)?)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                if r.success {
                    if let Some(true) = r.created {
                        self.get_mut(i)
                            .unwrap()
                            .set_id(FieldValue::Id(r.id.unwrap()));
                    }
                }

                r.into()
            })
            .collect())
    }
}

#[async_trait]
impl<T> SObjectCollectionDeleteable for Vec<T> where T: SObjectSerialization {
    fn delete_request(&self, all_or_none: bool) -> Result<SObjectCollectionDeleteRequest> {
        SObjectCollectionDeleteRequest::new(self, all_or_none)
    }

    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        Ok(conn
            .execute(&self.delete_request(all_or_none)?)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                if r.success {
                    self.get_mut(i).unwrap().set_id(FieldValue::Null);
                }

                r.into()
            })
            .collect())
    }
}


#[async_trait]
impl SObjectCollectionDeleteable for Vec<SalesforceId>  {
    fn delete_request(&self, all_or_none: bool) -> Result<SObjectCollectionDeleteRequest> {
        SObjectCollectionDeleteRequest::new_raw(self.iter().map(|i| i.to_string()).collect(), all_or_none)
    }

    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        Ok(conn
            .execute(&self.delete_request(all_or_none)?)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                if r.success {
                    self.get_mut(i).unwrap().set_id(FieldValue::Null);
                }

                r.into()
            })
            .collect())
    }
}