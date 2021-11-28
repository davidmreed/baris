use crate::rest::query::QueryRequest;

use std::collections::HashSet;
pub enum SoqlFilter {
    Clause(String),
    And(Box<SoqlFilter>, Box<SoqlFilter>),
    Or(Box<SoqlFilter>, Box<SoqlFilter>),
}

impl SoqlFilter {
    fn clause(c: String) -> SoqlFilter {
        SoqlFilter::Clause(c)
    }

    fn and(self, other: SoqlFilter) -> SoqlFilter {
        SoqlFilter::And(Box::new(self), Box::new(other))
    }

    fn or(self, other: SoqlFilter) -> SoqlFilter {
        SoqlFilter::Or(Box::new(self), Box::new(other))
    }
}

pub struct QueryBuilder {
    sobject_type: String,
    fields: HashSet<String>,
    lc_fields: HashSet<String>,
    fields_selector: Option<QueryFields>,
    filters: Option<SoqlFilter>,
    limit: Option<usize>,
    all: bool,
}

pub enum QueryFields {
    All,
    Standard,
    Custom,
}

/*
    QueryBuilder::sobject("Account")
        .fields(&["Name", "Industry"])
        .filter(SoqlFilter::clause("Name != 'Test"))
        .limit(200)
        .build()
        .execute(&conn)
        .await?;
*/

impl QueryBuilder {
    pub fn sobject(&self, sobject: String) -> QueryBuilder {
        QueryBuilder {
            sobject_type: sobject,
            fields: HashSet::new(),
            lc_fields: HashSet::new(),
            fields_selector: None,
            filters: None,
            limit: None,
            all: false,
        }
    }

    pub fn fields<T>(mut self, fields: &T) -> Self
    where
        T: IntoIterator<Item = String>,
    {
        self.fields.extend(fields.into_iter().filter(|i| {
            let lc = i.to_lowercase();
            if self.lc_fields.contains(&lc) {
                false
            } else {
                self.lc_fields.insert(lc);
                true
            }
        }));
        self
    }

    /*pub fn fields_selector(mut self, sel: QueryFields) -> Self {
        self.fields_selector = Some(sel);
        self
    }*/

    pub fn filter(mut self, cond: SoqlFilter) -> Self {
        self.filters = Some(cond);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn all(mut self, all: bool) -> Self {
        self.all = all;
        self
    }

    fn get_fields_soql(&self) -> String {
        format!(
            "{}",
            &self.fields.iter().collect::<Vec<&String>>().join(",")
        )
    }

    fn get_where_soql(&self) -> String {
        "".to_owned()
    }

    fn get_limit_soql(&self) -> String {
        if let Some(lim) = self.limit {
            format!("LIMIT {}", lim)
        } else {
            "".to_owned()
        }
    }

    fn build(&self) -> QueryRequest {
        QueryRequest::new(
            &format!(
                "SELECT {} FROM {} {} {} {}",
                self.get_fields_soql(),
                self.sobject_type,
                self.get_where_soql(),
                self.get_limit_soql(),
                if self.all { "ALL ROWS" } else { "" }.to_owned()
            ),
            self.all,
        )
    }
}
