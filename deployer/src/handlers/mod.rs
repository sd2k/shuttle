mod error;

use crate::deployment::{Built, DeploymentManager, Queued};
use crate::persistence::{Deployment, Log, Persistence, ResourceManager, SecretGetter, State};
use async_trait::async_trait;
use axum::extract::{
    ws::{self, WebSocket},
    FromRequest,
};
use axum::extract::{DefaultBodyLimit, Extension, Path, Query};
use axum::handler::Handler;
use axum::headers::HeaderMapExt;
use axum::middleware::{self, from_extractor};
use axum::routing::{get, post, Router};
use axum::Json;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use fqdn::FQDN;
use hyper::{Request, StatusCode, Uri};
use serde::{de::DeserializeOwned, Deserialize};
use shuttle_common::backends::auth::{
    AdminSecretLayer, AuthPublicKey, JwtAuthenticationLayer, ScopedLayer,
};
use shuttle_common::backends::headers::XShuttleAccountName;
use shuttle_common::backends::metrics::{Metrics, TraceLayer};
use shuttle_common::claims::{Claim, Scope};
use shuttle_common::models::deployment::{
    DeploymentRequest, CREATE_SERVICE_BODY_LIMIT, GIT_STRINGS_MAX_LENGTH,
};
use shuttle_common::models::secret;
use shuttle_common::project::ProjectName;
use shuttle_common::storage_manager::StorageManager;
use shuttle_common::{request_span, LogItem};
use shuttle_service::builder::clean_crate;
use tracing::{error, field, instrument, trace, warn};
use utoipa::{IntoParams, OpenApi};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

pub use {self::error::Error, self::error::Result, self::local::set_jwt_bearer};

mod local;
mod project;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_services,
        get_service,
        create_service,
        stop_service,
        get_service_resources,
        get_deployments,
        get_deployment,
        delete_deployment,
        get_logs_subscribe,
        get_logs,
        get_secrets,
        clean_project
    ),
    components(schemas(
        shuttle_common::models::service::Summary,
        shuttle_common::resource::Response,
        shuttle_common::resource::Type,
        shuttle_common::database::Type,
        shuttle_common::database::AwsRdsEngine,
        shuttle_common::database::SharedEngine,
        shuttle_common::models::service::Response,
        shuttle_common::models::secret::Response,
        shuttle_common::models::deployment::Response,
        shuttle_common::log::Item,
        shuttle_common::models::secret::Response,
        shuttle_common::log::Level,
        shuttle_common::deployment::State
    ))
)]
pub struct ApiDoc;

#[derive(Debug, Clone, Copy, Deserialize, IntoParams)]
pub struct PaginationDetails {
    /// Page to fetch, starting from 0.
    pub page: Option<u32>,
    /// Number of results per page.
    pub limit: Option<u32>,
}

#[derive(Clone)]
pub struct RouterBuilder {
    router: Router,
    project_name: ProjectName,
    auth_uri: Uri,
}

impl RouterBuilder {
    pub fn new(
        persistence: Persistence,
        deployment_manager: DeploymentManager,
        proxy_fqdn: FQDN,
        project_name: ProjectName,
        auth_uri: Uri,
    ) -> Self {
        let router = Router::new()
            // TODO: The `/swagger-ui` responds with a 303 See Other response which is followed in
            // browsers but leads to 404 Not Found. This must be investigated.
            .merge(SwaggerUi::new("/projects/:project_name/swagger-ui").url(
                "/projects/:project_name/api-docs/openapi.json",
                ApiDoc::openapi(),
            ))
            .route(
                "/projects/:project_name/services",
                get(get_services.layer(ScopedLayer::new(vec![Scope::Service]))),
            )
            .route(
                "/projects/:project_name/services/:service_name",
                get(get_service.layer(ScopedLayer::new(vec![Scope::Service])))
                    .post(
                        create_service
                            // Set the body size limit for this endpoint to 50MB
                            .layer(DefaultBodyLimit::max(CREATE_SERVICE_BODY_LIMIT))
                            .layer(ScopedLayer::new(vec![Scope::ServiceCreate])),
                    )
                    .delete(stop_service.layer(ScopedLayer::new(vec![Scope::ServiceCreate]))),
            )
            .route(
                "/projects/:project_name/services/:service_name/resources",
                get(get_service_resources).layer(ScopedLayer::new(vec![Scope::Resources])),
            )
            .route(
                "/projects/:project_name/deployments",
                get(get_deployments).layer(ScopedLayer::new(vec![Scope::Service])),
            )
            .route(
                "/projects/:project_name/deployments/:deployment_id",
                get(get_deployment.layer(ScopedLayer::new(vec![Scope::Deployment])))
                    .delete(delete_deployment.layer(ScopedLayer::new(vec![Scope::DeploymentPush])))
                    .put(start_deployment.layer(ScopedLayer::new(vec![Scope::DeploymentPush]))),
            )
            .route(
                "/projects/:project_name/ws/deployments/:deployment_id/logs",
                get(get_logs_subscribe.layer(ScopedLayer::new(vec![Scope::Logs]))),
            )
            .route(
                "/projects/:project_name/deployments/:deployment_id/logs",
                get(get_logs.layer(ScopedLayer::new(vec![Scope::Logs]))),
            )
            .route(
                "/projects/:project_name/secrets/:service_name",
                get(get_secrets.layer(ScopedLayer::new(vec![Scope::Secret]))),
            )
            .route(
                "/projects/:project_name/clean",
                post(clean_project.layer(ScopedLayer::new(vec![Scope::DeploymentPush]))),
            )
            .layer(Extension(persistence))
            .layer(Extension(deployment_manager))
            .layer(Extension(proxy_fqdn))
            .layer(JwtAuthenticationLayer::new(AuthPublicKey::new(
                auth_uri.clone(),
            )));

        Self {
            router,
            project_name,
            auth_uri,
        }
    }

    pub fn with_admin_secret_layer(mut self, admin_secret: String) -> Self {
        self.router = self.router.layer(AdminSecretLayer::new(admin_secret));

        self
    }

    /// Sets an admin JWT bearer token on every request for use when running deployer locally.
    pub fn with_local_admin_layer(mut self) -> Self {
        warn!("Building deployer router with auth bypassed, this should only be used for local development.");
        self.router = self
            .router
            .layer(middleware::from_fn(set_jwt_bearer))
            .layer(Extension(self.auth_uri.clone()));

        self
    }

    pub fn into_router(self) -> Router {
        self.router
            .route("/projects/:project_name/status", get(get_status))
            .route_layer(from_extractor::<Metrics>())
            .layer(
                TraceLayer::new(|request| {
                    let account_name = request
                        .headers()
                        .typed_get::<XShuttleAccountName>()
                        .unwrap_or_default();

                    request_span!(
                        request,
                        account.name = account_name.0,
                        request.params.project_name = field::Empty,
                        request.params.service_name = field::Empty,
                        request.params.deployment_id = field::Empty,
                    )
                })
                .with_propagation()
                .build(),
            )
            .route_layer(from_extractor::<project::ProjectNameGuard>())
            .layer(Extension(self.project_name))
    }
}

#[instrument(skip_all)]
#[utoipa::path(
    get,
    path = "/projects/{project_name}/services",
    responses(
        (status = 200, description = "Lists the services owned by a project.", body = [shuttle_common::models::service::Response]),
        (status = 500, description = "Database error.", body = String)
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the services."),
    )
)]
pub async fn get_services(
    Extension(persistence): Extension<Persistence>,
) -> Result<Json<Vec<shuttle_common::models::service::Response>>> {
    let services = persistence
        .get_all_services()
        .await?
        .into_iter()
        .map(Into::into)
        .collect();

    Ok(Json(services))
}

#[instrument(skip_all, fields(%project_name, %service_name))]
#[instrument(skip_all)]
#[utoipa::path(
    get,
    path = "/projects/{project_name}/services/{service_name}",
    responses(
        (status = 200, description = "Gets a specific service summary.", body = shuttle_common::models::service::Summary),
        (status = 500, description = "Database error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the service."),
        ("service_name" = String, Path, description = "Name of the service.")
    )
)]
pub async fn get_service(
    Extension(persistence): Extension<Persistence>,
    Extension(proxy_fqdn): Extension<FQDN>,
    Path((project_name, service_name)): Path<(String, String)>,
) -> Result<Json<shuttle_common::models::service::Summary>> {
    if let Some(service) = persistence.get_service_by_name(&service_name).await? {
        let deployment = persistence
            .get_active_deployment(&service.id)
            .await?
            .map(Into::into);

        let response = shuttle_common::models::service::Summary {
            uri: format!("https://{proxy_fqdn}"),
            name: service.name,
            deployment,
        };

        Ok(Json(response))
    } else {
        Err(Error::NotFound("service not found".to_string()))
    }
}

#[instrument(skip_all, fields(%project_name, %service_name))]
#[utoipa::path(
    get,
    path = "/projects/{project_name}/services/{service_name}/resources",
    responses(
        (status = 200, description = "Gets a specific service resources.", body = shuttle_common::resource::Response),
        (status = 500, description = "Database error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the service."),
        ("service_name" = String, Path, description = "Name of the service.")
    )
)]
pub async fn get_service_resources(
    Extension(persistence): Extension<Persistence>,
    Path((project_name, service_name)): Path<(String, String)>,
) -> Result<Json<Vec<shuttle_common::resource::Response>>> {
    if let Some(service) = persistence.get_service_by_name(&service_name).await? {
        let resources = persistence
            .get_resources(&service.id)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(Json(resources))
    } else {
        Err(Error::NotFound("service not found".to_string()))
    }
}

#[instrument(skip_all, fields(%project_name, %service_name))]
#[utoipa::path(
    post,
    path = "/projects/{project_name}/services/{service_name}",
    responses(
        (status = 200, description = "Creates a specific service owned by a specific project.", body = shuttle_common::models::deployment::Response),
        (status = 500, description = "Database or streaming error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the service."),
        ("service_name" = String, Path, description = "Name of the service.")
    )
)]
pub async fn create_service(
    Extension(persistence): Extension<Persistence>,
    Extension(deployment_manager): Extension<DeploymentManager>,
    Extension(claim): Extension<Claim>,
    Path((project_name, service_name)): Path<(String, String)>,
    Rmp(deployment_req): Rmp<DeploymentRequest>,
) -> Result<Json<shuttle_common::models::deployment::Response>> {
    let service = persistence.get_or_create_service(&service_name).await?;
    let deployment = Deployment {
        id: Uuid::new_v4(),
        service_id: service.id,
        state: State::Queued,
        last_update: Utc::now(),
        address: None,
        is_next: false,
        git_commit_id: deployment_req
            .git_commit_id
            .map(|s| s.chars().take(GIT_STRINGS_MAX_LENGTH).collect()),
        git_commit_msg: deployment_req
            .git_commit_msg
            .map(|s| s.chars().take(GIT_STRINGS_MAX_LENGTH).collect()),
        git_branch: deployment_req
            .git_branch
            .map(|s| s.chars().take(GIT_STRINGS_MAX_LENGTH).collect()),
        git_dirty: deployment_req.git_dirty,
    };

    persistence.insert_deployment(deployment.clone()).await?;

    let queued = Queued {
        id: deployment.id,
        service_name: service.name,
        service_id: deployment.service_id,
        data: deployment_req.data,
        will_run_tests: !deployment_req.no_test,
        tracing_context: Default::default(),
        claim,
    };

    deployment_manager.queue_push(queued).await;

    Ok(Json(deployment.into()))
}

#[instrument(skip_all, fields(%project_name, %service_name))]
#[utoipa::path(
    delete,
    path = "/projects/{project_name}/services/{service_name}",
    responses(
        (status = 200, description = "Stops a specific service owned by a specific project.", body = shuttle_common::models::service::Summary),
        (status = 500, description = "Database error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the service."),
        ("service_name" = String, Path, description = "Name of the service.")
    )
)]
pub async fn stop_service(
    Extension(persistence): Extension<Persistence>,
    Extension(deployment_manager): Extension<DeploymentManager>,
    Extension(proxy_fqdn): Extension<FQDN>,
    Path((project_name, service_name)): Path<(String, String)>,
) -> Result<Json<shuttle_common::models::service::Summary>> {
    if let Some(service) = persistence.get_service_by_name(&service_name).await? {
        let running_deployment = persistence.get_active_deployment(&service.id).await?;

        if let Some(ref deployment) = running_deployment {
            deployment_manager.kill(deployment.id).await;
        } else {
            return Err(Error::NotFound("no running deployment found".to_string()));
        }

        let response = shuttle_common::models::service::Summary {
            name: service.name,
            deployment: running_deployment.map(Into::into),
            uri: format!("https://{proxy_fqdn}"),
        };

        Ok(Json(response))
    } else {
        Err(Error::NotFound("service not found".to_string()))
    }
}

#[instrument(skip(persistence))]
#[utoipa::path(
    get,
    path = "/projects/{project_name}/deployments",
    responses(
        (status = 200, description = "Gets deployments information associated to a specific project.", body = shuttle_common::models::deployment::Response),
        (status = 500, description = "Database error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the deployments."),
        PaginationDetails
    )
)]
pub async fn get_deployments(
    Extension(persistence): Extension<Persistence>,
    Path(project_name): Path<String>,
    Query(PaginationDetails { page, limit }): Query<PaginationDetails>,
) -> Result<Json<Vec<shuttle_common::models::deployment::Response>>> {
    if let Some(service) = persistence.get_service_by_name(&project_name).await? {
        let limit = limit.unwrap_or(u32::MAX);
        let page = page.unwrap_or(0);
        let deployments = persistence
            .get_deployments(&service.id, page * limit, limit)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(Json(deployments))
    } else {
        Err(Error::NotFound("service not found".to_string()))
    }
}

#[instrument(skip_all, fields(%project_name, %deployment_id))]
#[instrument(skip(persistence))]
#[utoipa::path(
    get,
    path = "/projects/{project_name}/deployments/{deployment_id}",
    responses(
        (status = 200, description = "Gets a specific deployment information.", body = shuttle_common::models::deployment::Response),
        (status = 500, description = "Database or streaming error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the deployment."),
        ("deployment_id" = String, Path, description = "The deployment id in uuid format.")
    )
)]
pub async fn get_deployment(
    Extension(persistence): Extension<Persistence>,
    Path((project_name, deployment_id)): Path<(String, Uuid)>,
) -> Result<Json<shuttle_common::models::deployment::Response>> {
    if let Some(deployment) = persistence.get_deployment(&deployment_id).await? {
        Ok(Json(deployment.into()))
    } else {
        Err(Error::NotFound("deployment not found".to_string()))
    }
}

#[instrument(skip_all, fields(%project_name, %deployment_id))]
#[utoipa::path(
    delete,
    path = "/projects/{project_name}/deployments/{deployment_id}",
    responses(
        (status = 200, description = "Deletes a specific deployment.", body = shuttle_common::models::deployment::Response),
        (status = 500, description = "Database or streaming error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the deployment."),
        ("deployment_id" = String, Path, description = "The deployment id in uuid format.")
    )
)]
pub async fn delete_deployment(
    Extension(deployment_manager): Extension<DeploymentManager>,
    Extension(persistence): Extension<Persistence>,
    Path((project_name, deployment_id)): Path<(String, Uuid)>,
) -> Result<Json<shuttle_common::models::deployment::Response>> {
    if let Some(deployment) = persistence.get_deployment(&deployment_id).await? {
        deployment_manager.kill(deployment.id).await;

        Ok(Json(deployment.into()))
    } else {
        Err(Error::NotFound("deployment not found".to_string()))
    }
}

#[instrument(skip_all, fields(%project_name, %deployment_id))]
#[utoipa::path(
    put,
    path = "/projects/{project_name}/deployments/{deployment_id}",
    responses(
        (status = 200, description = "Started a specific deployment.", body = shuttle_common::models::deployment::Response),
        (status = 500, description = "Database or streaming error.", body = String),
        (status = 404, description = "Could not find deployment to start", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the deployment."),
        ("deployment_id" = String, Path, description = "The deployment id in uuid format.")
    )
)]
pub async fn start_deployment(
    Extension(persistence): Extension<Persistence>,
    Extension(deployment_manager): Extension<DeploymentManager>,
    Extension(claim): Extension<Claim>,
    Path((project_name, deployment_id)): Path<(String, Uuid)>,
) -> Result<()> {
    if let Some(deployment) = persistence.get_runnable_deployment(&deployment_id).await? {
        let built = Built {
            id: deployment.id,
            service_name: deployment.service_name,
            service_id: deployment.service_id,
            tracing_context: Default::default(),
            is_next: deployment.is_next,
            claim,
        };
        deployment_manager.run_push(built).await;

        Ok(())
    } else {
        Err(Error::NotFound("deployment not found".to_string()))
    }
}

#[instrument(skip_all, fields(%project_name, %deployment_id))]
#[utoipa::path(
    get,
    path = "/projects/{project_name}/ws/deployments/{deployment_id}/logs",
    responses(
        (status = 200, description = "Gets the logs a specific deployment.", body = [shuttle_common::log::Item]),
        (status = 500, description = "Database or streaming error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the deployment."),
        ("deployment_id" = String, Path, description = "The deployment id in uuid format.")
    )
)]
pub async fn get_logs(
    Extension(persistence): Extension<Persistence>,
    Path((project_name, deployment_id)): Path<(String, Uuid)>,
) -> Result<Json<Vec<LogItem>>> {
    if let Some(deployment) = persistence.get_deployment(&deployment_id).await? {
        Ok(Json(
            persistence
                .get_deployment_logs(&deployment.id)
                .await?
                .into_iter()
                .filter_map(Into::into)
                .collect(),
        ))
    } else {
        Err(Error::NotFound("deployment not found".to_string()))
    }
}

#[utoipa::path(
    get,
    path = "/projects/{project_name}/deployments/{deployment_id}/logs",
    responses(
        (status = 200, description = "Subscribes to a specific deployment logs.")
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the deployment."),
        ("deployment_id" = String, Path, description = "The deployment id in uuid format.")
    )
)]
pub async fn get_logs_subscribe(
    Extension(persistence): Extension<Persistence>,
    Path((_project_name, deployment_id)): Path<(String, Uuid)>,
    ws_upgrade: ws::WebSocketUpgrade,
) -> axum::response::Response {
    ws_upgrade.on_upgrade(move |s| logs_websocket_handler(s, persistence, deployment_id))
}

async fn logs_websocket_handler(mut s: WebSocket, persistence: Persistence, id: Uuid) {
    let mut log_recv = persistence.get_log_subscriber();
    let backlog = match persistence.get_deployment_logs(&id).await {
        Ok(backlog) => backlog,
        Err(error) => {
            error!(
                error = &error as &dyn std::error::Error,
                "failed to get backlog of logs"
            );

            let _ = s
                .send(ws::Message::Text("failed to get logs".to_string()))
                .await;
            let _ = s.close().await;
            return;
        }
    };

    // Unwrap is safe because it only returns None for out of range numbers or invalid nanosecond
    let mut last_timestamp = Utc.timestamp_opt(0, 0).unwrap();

    for log in backlog.into_iter() {
        last_timestamp = log.timestamp;
        if let Some(log_item) = Option::<LogItem>::from(log) {
            let msg = serde_json::to_string(&log_item).expect("to convert log item to json");
            let sent = s.send(ws::Message::Text(msg)).await;

            // Client disconnected?
            if sent.is_err() {
                return;
            }
        }
    }

    while let Ok(log) = log_recv.recv().await {
        trace!(?log, "received log from broadcast channel");

        if log.id == id && log.timestamp > last_timestamp {
            if let Some(log_item) = Option::<LogItem>::from(Log::from(log)) {
                let msg = serde_json::to_string(&log_item).expect("to convert log item to json");
                let sent = s.send(ws::Message::Text(msg)).await;

                // Client disconnected?
                if sent.is_err() {
                    return;
                }
            }
        }
    }

    let _ = s.close().await;
}

#[instrument(skip_all, fields(%project_name, %service_name))]
#[utoipa::path(
    get,
    path = "/projects/{project_name}/secrets/{service_name}",
    responses(
        (status = 200, description = "Gets the secrets a specific service.", body = [shuttle_common::models::secret::Response]),
        (status = 500, description = "Database or streaming error.", body = String),
        (status = 404, description = "Record could not be found.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the service."),
        ("service_name" = String, Path, description = "Name of the service.")
    )
)]
pub async fn get_secrets(
    Extension(persistence): Extension<Persistence>,
    Path((project_name, service_name)): Path<(String, String)>,
) -> Result<Json<Vec<secret::Response>>> {
    if let Some(service) = persistence.get_service_by_name(&service_name).await? {
        let keys = persistence
            .get_secrets(&service.id)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(Json(keys))
    } else {
        Err(Error::NotFound("service not found".to_string()))
    }
}

#[utoipa::path(
    post,
    path = "/projects/{project_name}/clean",
    responses(
        (status = 200, description = "Clean a specific project build artifacts.", body = [String]),
        (status = 500, description = "Clean project error.", body = String),
    ),
    params(
        ("project_name" = String, Path, description = "Name of the project that owns the service."),
    )
)]
pub async fn clean_project(
    Extension(deployment_manager): Extension<DeploymentManager>,
    Path(project_name): Path<String>,
) -> Result<Json<Vec<String>>> {
    let project_path = deployment_manager
        .storage_manager()
        .service_build_path(&project_name)
        .map_err(anyhow::Error::new)?;

    let lines = clean_crate(&project_path, true).await?;

    Ok(Json(lines))
}

async fn get_status() -> String {
    "Ok".to_string()
}

pub struct Rmp<T>(T);

#[async_trait]
impl<S, B, T> FromRequest<S, B> for Rmp<T>
where
    S: Send + Sync,
    B: Send + 'static,
    Bytes: FromRequest<S, B>,
    T: DeserializeOwned,
{
    type Rejection = StatusCode;

    async fn from_request(
        req: Request<B>,
        state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state).await.map_err(|_| {
            error!("failed to collect body bytes, is the body too large?");
            StatusCode::PAYLOAD_TOO_LARGE
        })?;

        let t = rmp_serde::from_slice::<T>(&bytes).map_err(|error| {
            error!(error = %error, "failed to deserialize request body");
            StatusCode::BAD_REQUEST
        })?;

        Ok(Self(t))
    }
}
