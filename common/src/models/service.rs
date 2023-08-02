#[cfg(feature = "openapi")]
use crate::ulid_type;
use crossterm::style::{Color, Stylize};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

use crate::models::deployment;

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[cfg_attr(feature = "openapi", schema(as = shuttle_common::models::service::Response))]
pub struct Response {
    #[cfg_attr(feature = "openapi", schema(schema_with = ulid_type))]
    pub id: String,
    pub name: String,
}

#[derive(Deserialize, Serialize)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[cfg_attr(feature = "openapi", schema(as = shuttle_common::models::service::Summary))]
pub struct Summary {
    pub name: String,
    #[cfg_attr(feature = "openapi", schema(value_type = shuttle_common::models::deployment::Response))]
    pub deployment: Option<deployment::Response>,
    pub uri: String,
}

impl Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let deployment = if let Some(ref deployment) = self.deployment {
            format!(
                r#"
Service Name:  {}
Deployment ID: {}
Status:        {}
Last Updated:  {}
URI:           {}
"#,
                self.name.clone().bold(),
                deployment.id,
                deployment.state.to_string().with(
                    // Unwrap is safe because Color::from_str returns the color white if str is not a Color.
                    Color::from_str(deployment.state.get_color()).unwrap()
                ),
                deployment.last_update.format("%Y-%m-%dT%H:%M:%SZ"),
                self.uri,
            )
        } else {
            format!(
                "{}\n\n",
                "No deployment is currently running for this service"
                    .yellow()
                    .bold()
            )
        };

        write!(f, "{deployment}")
    }
}
