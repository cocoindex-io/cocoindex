use crate::{
    ops::sdk::SetupStateCompatibility,
    prelude::*,
    setup::{CombinedState, SetupChangeType},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumnsSchema<T: Serialize + DeserializeOwned> {
    #[serde(with = "indexmap::map::serde_seq", alias = "key_fields_schema")]
    pub key_columns: IndexMap<String, T>,

    #[serde(with = "indexmap::map::serde_seq", alias = "value_fields_schema")]
    pub value_columns: IndexMap<String, T>,
}

#[derive(Debug)]
pub enum TableUpsertionAction<T> {
    Create {
        keys: IndexMap<String, T>,
        values: IndexMap<String, T>,
    },
    Update {
        columns_to_delete: IndexSet<String>,
        columns_to_upsert: IndexMap<String, T>,
    },
}

impl<T> TableUpsertionAction<T> {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Create { .. } => false,
            Self::Update {
                columns_to_delete,
                columns_to_upsert,
            } => columns_to_delete.is_empty() && columns_to_upsert.is_empty(),
        }
    }
}

#[derive(Debug)]
pub struct TableMainSetupAction<T> {
    pub drop_existing: bool,
    pub table_upsertion: Option<TableUpsertionAction<T>>,
}

impl<T: Eq + Serialize + DeserializeOwned> TableMainSetupAction<T> {
    pub fn from_states<S>(
        desired_state: Option<&S>,
        existing: &CombinedState<S>,
        existing_invalidated: bool,
    ) -> Self
    where
        for<'a> &'a S: Into<Cow<'a, TableColumnsSchema<T>>>,
        T: Clone,
    {
        let desired_cols: Option<Cow<'_, TableColumnsSchema<T>>> =
            desired_state.map(|desired| desired.into());
        let possible_existing_cols: Vec<Cow<'_, TableColumnsSchema<T>>> = existing
            .possible_versions()
            .map(|v| Into::<Cow<'_, TableColumnsSchema<T>>>::into(v))
            .collect();
        let drop_existing = desired_cols.as_ref().map_or(true, |desired| {
            existing_invalidated
                || possible_existing_cols
                    .iter()
                    .any(|v| v.key_columns != desired.key_columns)
        });

        let table_upsertion = desired_cols
            .map(|desired| {
                if !existing_invalidated
                    && existing.always_exists()
                    && possible_existing_cols
                        .iter()
                        .all(|v| desired.key_columns == v.key_columns)
                {
                    TableUpsertionAction::Update {
                        columns_to_delete: possible_existing_cols
                            .iter()
                            .flat_map(|v| v.value_columns.keys())
                            .filter(|column_name| !desired.value_columns.contains_key(*column_name))
                            .cloned()
                            .collect(),
                        columns_to_upsert: desired
                            .value_columns
                            .iter()
                            .filter(|(column_name, schema)| {
                                !possible_existing_cols
                                    .iter()
                                    .all(|v| v.value_columns.get(*column_name) == Some(schema))
                            })
                            .map(|(k, v)| (k.to_owned(), v.to_owned()))
                            .collect(),
                    }
                } else {
                    TableUpsertionAction::Create {
                        keys: desired.key_columns.to_owned(),
                        values: desired.value_columns.to_owned(),
                    }
                }
            })
            .filter(|action| !action.is_empty());
        Self {
            drop_existing,
            table_upsertion,
        }
    }

    pub fn describe_changes(&self) -> Vec<String>
    where
        T: std::fmt::Display,
    {
        let mut descriptions = vec![];
        if self.drop_existing {
            descriptions.push("Drop table".to_string());
        }
        if let Some(table_upsertion) = &self.table_upsertion {
            match table_upsertion {
                TableUpsertionAction::Create { keys, values } => {
                    descriptions.push(format!(
                        "Create table:\n  key columns: {}\n  value columns: {}\n",
                        keys.iter().map(|(k, v)| format!("{k} {v}")).join(",  "),
                        values.iter().map(|(k, v)| format!("{k} {v}")).join(",  "),
                    ));
                }
                TableUpsertionAction::Update {
                    columns_to_delete,
                    columns_to_upsert,
                } => {
                    if !columns_to_delete.is_empty() {
                        descriptions.push(format!(
                            "Delete column from table: {}",
                            columns_to_delete.iter().join(",  "),
                        ));
                    }
                    if !columns_to_upsert.is_empty() {
                        descriptions.push(format!(
                            "Add / update columns in table: {}",
                            columns_to_upsert
                                .iter()
                                .map(|(k, v)| format!("{k} {v}"))
                                .join(",  "),
                        ));
                    }
                }
            }
        }
        descriptions
    }

    pub fn change_type(&self, has_other_update: bool) -> SetupChangeType {
        match (self.drop_existing, &self.table_upsertion) {
            (_, Some(TableUpsertionAction::Create { .. })) => SetupChangeType::Create,
            (_, Some(TableUpsertionAction::Update { .. })) => SetupChangeType::Update,
            (true, None) => SetupChangeType::Delete,
            (false, None) => {
                if has_other_update {
                    SetupChangeType::Update
                } else {
                    SetupChangeType::NoChange
                }
            }
        }
    }
}

pub fn check_table_compatibility<T: Eq + Serialize + DeserializeOwned>(
    desired: &TableColumnsSchema<T>,
    existing: &TableColumnsSchema<T>,
) -> SetupStateCompatibility {
    let is_key_identical = existing.key_columns == desired.key_columns;
    if is_key_identical {
        let is_value_lossy = existing
            .value_columns
            .iter()
            .any(|(k, v)| desired.value_columns.get(k) != Some(v));
        if is_value_lossy {
            SetupStateCompatibility::PartialCompatible
        } else {
            SetupStateCompatibility::Compatible
        }
    } else {
        SetupStateCompatibility::NotCompatible
    }
}
