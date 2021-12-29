use crate::{
    data::{DynamicallyTypedSObject, SObjectRepresentation, SingleTypedSObject},
    Connection, FieldValue, SObjectType,
};

use anyhow::Result;
use async_trait::async_trait;

use super::{
    SObjectCollectionCreateRequest, SObjectCollectionDeleteRequest, SObjectCollectionUpdateRequest,
    SObjectCollectionUpsertRequest,
};

#[async_trait]
pub trait SObjectCollectionDynamicallyTyped {
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
    async fn upsert(
        &mut self,
        conn: &Connection,
        sobject_type: &SObjectType,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>>;
    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
}

#[async_trait]
pub trait SObjectCollectionSingleTyped {
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
    async fn upsert(
        &mut self,
        conn: &Connection,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>>;
    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
}

// TODO: Can we implement for &mut [T] and take advantage of Vec's DerefMut?
#[async_trait]
impl<T> SObjectCollectionDynamicallyTyped for Vec<T>
where
    T: SObjectRepresentation + DynamicallyTypedSObject, // TODO: Is this trait bound minimal?
{
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionCreateRequest::new(self, all_or_none)?;

        Ok(conn
            .execute(&request)
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

    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionUpdateRequest::new(self, all_or_none)?;

        Ok(conn
            .execute(&request)
            .await?
            .into_iter()
            .map(|r| r.into())
            .collect())
    }

    async fn upsert(
        &mut self,
        conn: &Connection,
        sobject_type: &SObjectType,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>> {
        let request =
            SObjectCollectionUpsertRequest::new(self, sobject_type, external_id, all_or_none)?;
        Ok(conn
            .execute(&request)
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

    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionDeleteRequest::new(self, all_or_none)?;
        Ok(conn
            .execute(&request)
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
impl<T> SObjectCollectionSingleTyped for Vec<T>
where
    T: SObjectRepresentation + SingleTypedSObject, // TODO: Is this trait bound minimal?
{
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionCreateRequest::new(self, all_or_none)?;

        Ok(conn
            .execute(&request)
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

    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionUpdateRequest::new(self, all_or_none)?;

        Ok(conn
            .execute(&request)
            .await?
            .into_iter()
            .map(|r| r.into())
            .collect())
    }

    async fn upsert(
        &mut self,
        conn: &Connection,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionUpsertRequest::new(
            self,
            &conn.get_type(T::get_type_api_name()).await?,
            external_id,
            all_or_none,
        )?;
        Ok(conn
            .execute(&request)
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

    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionDeleteRequest::new(self, all_or_none)?;
        Ok(conn
            .execute(&request)
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
