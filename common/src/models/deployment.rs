use crate::deployment::State;
#[cfg(feature = "openapi")]
use crate::ulid_type;
use chrono::{DateTime, Utc};
use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Attribute, Cell, CellAlignment, Color,
    ContentArrangement, Table,
};
use crossterm::style::Stylize;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};
#[cfg(feature = "openapi")]
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[cfg_attr(feature = "openapi", schema(as = shuttle_common::models::deployment::Response))]
pub struct Response {
    #[cfg_attr(feature = "openapi", schema(value_type = KnownFormat::Uuid))]
    pub id: Uuid,
    #[cfg_attr(feature = "openapi", schema(schema_with = ulid_type))]
    pub service_id: String,
    #[cfg_attr(feature = "openapi", schema(value_type = shuttle_common::deployment::State))]
    pub state: State,
    #[cfg_attr(feature = "openapi", schema(value_type = KnownFormat::DateTime))]
    pub last_update: DateTime<Utc>,
    pub git_commit_id: Option<String>,
    pub git_commit_msg: Option<String>,
    pub git_branch: Option<String>,
    pub git_dirty: Option<bool>,
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} deployment '{}' is {}",
            self.last_update
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
                .dim(),
            self.id,
            self.state.to_string().cyan()
        )
    }
}

impl State {
    /// We return a &str rather than a Color here, since `comfy-table` re-exports
    /// crossterm::style::Color and we depend on both `comfy-table` and `crossterm`
    /// we may end up with two different versions of Color.
    pub fn get_color(&self) -> &str {
        match self {
            State::Queued | State::Building | State::Built | State::Loading => "cyan",
            State::Running => "green",
            State::Completed | State::Stopped => "blue",
            State::Crashed => "red",
            State::Unknown => "yellow",
        }
    }
}

pub fn get_deployments_table(deployments: &Vec<Response>, service_name: &str, page: u32) -> String {
    if deployments.is_empty() {
        if page <= 1 {
            format!(
                "{}\n",
                "No deployments are linked to this service".yellow().bold()
            )
        } else {
            format!(
                "{}\n",
                "No more deployments linked to this service".yellow().bold()
            )
        }
    } else {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_content_arrangement(ContentArrangement::DynamicFullWidth)
            .set_header(vec![
                Cell::new("Deployment ID")
                    .set_alignment(CellAlignment::Center)
                    .add_attribute(Attribute::Bold),
                Cell::new("Status")
                    .set_alignment(CellAlignment::Center)
                    .add_attribute(Attribute::Bold),
                Cell::new("Last updated")
                    .set_alignment(CellAlignment::Center)
                    .add_attribute(Attribute::Bold),
                Cell::new("Commit ID")
                    .set_alignment(CellAlignment::Center)
                    .add_attribute(Attribute::Bold),
                Cell::new("Commit Message")
                    .set_alignment(CellAlignment::Center)
                    .add_attribute(Attribute::Bold),
                Cell::new("Branch")
                    .set_alignment(CellAlignment::Center)
                    .add_attribute(Attribute::Bold),
                Cell::new("Dirty")
                    .set_alignment(CellAlignment::Center)
                    .add_attribute(Attribute::Bold),
            ]);

        for deploy in deployments.iter() {
            let truncated_commit_id = deploy
                .git_commit_id
                .as_ref()
                .map_or(String::from(GIT_OPTION_NONE_TEXT), |val| {
                    val.chars().take(7).collect()
                });

            let truncated_commit_msg = deploy
                .git_commit_msg
                .as_ref()
                .map_or(String::from(GIT_OPTION_NONE_TEXT), |val| {
                    val.chars().take(24).collect::<String>()
                });

            table.add_row(vec![
                Cell::new(deploy.id),
                Cell::new(&deploy.state)
                    // Unwrap is safe because Color::from_str returns the color white if str is not a Color.
                    .fg(Color::from_str(deploy.state.get_color()).unwrap())
                    .set_alignment(CellAlignment::Center),
                Cell::new(deploy.last_update.format("%Y-%m-%dT%H:%M:%SZ"))
                    .set_alignment(CellAlignment::Center),
                Cell::new(truncated_commit_id),
                Cell::new(truncated_commit_msg),
                Cell::new(
                    deploy
                        .git_branch
                        .as_ref()
                        .map_or(GIT_OPTION_NONE_TEXT, |val| val as &str),
                ),
                Cell::new(
                    deploy
                        .git_dirty
                        .map_or(String::from(GIT_OPTION_NONE_TEXT), |val| val.to_string()),
                )
                .set_alignment(CellAlignment::Center),
            ]);
        }

        format!(
            r#"
Most recent {} for {}
{}

{}
"#,
            "deployments".bold(),
            service_name,
            table,
            "More projects might be available on the next page using --page.".bold()
        )
    }
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[cfg_attr(feature = "openapi", schema(as = shuttle_common::models::deployment::DeploymentRequest))]
pub struct DeploymentRequest {
    pub data: Vec<u8>,
    pub no_test: bool,
    pub git_commit_id: Option<String>,
    pub git_commit_msg: Option<String>,
    pub git_branch: Option<String>,
    pub git_dirty: Option<bool>,
}

pub const GIT_STRINGS_MAX_LENGTH: usize = 80;
const GIT_OPTION_NONE_TEXT: &str = "N/A";
pub const CREATE_SERVICE_BODY_LIMIT: usize = 50_000_000;
