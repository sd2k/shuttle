use std::io::Cursor;
use std::net::Ipv4Addr;
use std::ops::Sub;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::headers::HeaderMapExt;
use axum::http::Request;
use axum::response::Response;
use bollard::{Docker, API_DEFAULT_VERSION};
use fqdn::{Fqdn, FQDN};
use http::header::AUTHORIZATION;
use http::Uri;
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper::Client;
use hyper_reverse_proxy::ReverseProxy;
use instant_acme::{AccountCredentials, ChallengeType};
use once_cell::sync::Lazy;
use opentelemetry::global;
use opentelemetry_http::HeaderInjector;
use shuttle_common::backends::headers::{
    XShuttleAccountName, XShuttleAdminSecret, XShuttleProjectId,
};
use sqlx::error::DatabaseError;
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqlitePool;
use sqlx::types::Json as SqlxJson;
use sqlx::{query, Error as SqlxError, QueryBuilder, Row};
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;
use tonic::transport::Endpoint;
use tracing::{debug, instrument, trace, warn, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use ulid::Ulid;
use x509_parser::nom::AsBytes;
use x509_parser::parse_x509_certificate;
use x509_parser::prelude::parse_x509_pem;
use x509_parser::time::ASN1Time;

use crate::acme::{AccountWrapper, AcmeClient, CustomDomain};
use crate::args::ContextArgs;
use crate::project::{Project, ProjectCreating, IS_HEALTHY_TIMEOUT};
use crate::task::{self, BoxedTask, TaskBuilder};
use crate::tls::{ChainAndPrivateKey, GatewayCertResolver, RENEWAL_VALIDITY_THRESHOLD_IN_DAYS};
use crate::worker::TaskRouter;
use crate::{
    AccountName, DockerContext, Error, ErrorKind, ProjectDetails, ProjectName, AUTH_CLIENT,
};

pub static MIGRATIONS: Migrator = sqlx::migrate!("./migrations");
static PROXY_CLIENT: Lazy<ReverseProxy<HttpConnector<GaiResolver>>> =
    Lazy::new(|| ReverseProxy::new(Client::new()));

impl From<SqlxError> for Error {
    fn from(err: SqlxError) -> Self {
        debug!("internal SQLx error: {err}");
        Self::source(ErrorKind::Internal, err)
    }
}

pub struct ContainerSettingsBuilder {
    prefix: Option<String>,
    image: Option<String>,
    provisioner: Option<String>,
    auth_uri: Option<String>,
    network_name: Option<String>,
    fqdn: Option<String>,
    project_id: Option<Ulid>,
}

impl Default for ContainerSettingsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainerSettingsBuilder {
    pub fn new() -> Self {
        Self {
            prefix: None,
            image: None,
            provisioner: None,
            auth_uri: None,
            network_name: None,
            fqdn: None,
            project_id: None,
        }
    }

    pub async fn from_args(self, args: &ContextArgs) -> ContainerSettings {
        let ContextArgs {
            prefix,
            network_name,
            provisioner_host,
            auth_uri,
            image,
            proxy_fqdn,
            ..
        } = args;
        self.prefix(prefix)
            .image(image)
            .provisioner_host(provisioner_host)
            .auth_uri(auth_uri)
            .network_name(network_name)
            .fqdn(proxy_fqdn)
            .build()
            .await
    }

    pub fn prefix<S: ToString>(mut self, prefix: S) -> Self {
        self.prefix = Some(prefix.to_string());
        self
    }

    pub fn image<S: ToString>(mut self, image: S) -> Self {
        self.image = Some(image.to_string());
        self
    }

    pub fn project_id(mut self, id: Ulid) -> Self {
        self.project_id = Some(id);
        self
    }

    pub fn provisioner_host<S: ToString>(mut self, host: S) -> Self {
        self.provisioner = Some(host.to_string());
        self
    }

    pub fn auth_uri<S: ToString>(mut self, auth_uri: S) -> Self {
        self.auth_uri = Some(auth_uri.to_string());
        self
    }

    pub fn network_name<S: ToString>(mut self, name: S) -> Self {
        self.network_name = Some(name.to_string());
        self
    }

    pub fn fqdn<S: ToString>(mut self, fqdn: S) -> Self {
        self.fqdn = Some(fqdn.to_string().trim_end_matches('.').to_string());
        self
    }

    pub async fn build(mut self) -> ContainerSettings {
        let prefix = self.prefix.take().unwrap();
        let image = self.image.take().unwrap();
        let provisioner_host = self.provisioner.take().unwrap();
        let auth_uri = self.auth_uri.take().unwrap();

        let network_name = self.network_name.take().unwrap();
        let fqdn = self.fqdn.take().unwrap();
        let project_id = self.project_id.take().unwrap();

        ContainerSettings {
            prefix,
            image,
            provisioner_host,
            auth_uri,
            network_name,
            fqdn,
            project_id,
        }
    }
}

#[derive(Clone)]
pub struct ContainerSettings {
    pub prefix: String,
    pub image: String,
    pub provisioner_host: String,
    pub auth_uri: String,
    pub network_name: String,
    pub fqdn: String,
    pub project_id: Ulid,
}

impl ContainerSettings {
    pub fn builder() -> ContainerSettingsBuilder {
        ContainerSettingsBuilder::new()
    }
}

pub struct GatewayContextProvider {
    docker: Docker,
    settings: ContainerSettings,
    api_key: String,
    auth_key_uri: Uri,
}

impl GatewayContextProvider {
    pub fn new(
        docker: Docker,
        settings: ContainerSettings,
        api_key: String,
        auth_key_uri: Uri,
    ) -> Self {
        Self {
            docker,
            settings,
            api_key,
            auth_key_uri,
        }
    }

    pub fn context(&self) -> GatewayContext {
        GatewayContext {
            docker: self.docker.clone(),
            settings: self.settings.clone(),
            api_key: self.api_key.clone(),
            auth_key_uri: self.auth_key_uri.clone(),
        }
    }
}

pub struct GatewayService {
    provider: GatewayContextProvider,
    db: SqlitePool,
    task_router: TaskRouter<BoxedTask>,
    state_location: PathBuf,

    // We store these because we'll need them for the health checks
    provisioner_host: Endpoint,
    auth_host: Uri,
}

impl GatewayService {
    /// Initialize `GatewayService` and its required dependencies.
    ///
    /// * `args` - The [`Args`] with which the service was
    /// started. Will be passed as [`Context`] to workers and state.
    pub async fn init(args: ContextArgs, db: SqlitePool, state_location: PathBuf) -> Self {
        let docker = Docker::connect_with_unix(&args.docker_host, 60, API_DEFAULT_VERSION).unwrap();

        let container_settings = ContainerSettings::builder().from_args(&args).await;

        let provider = GatewayContextProvider::new(
            docker,
            container_settings,
            args.deploys_api_key,
            format!("{}auth/key", args.auth_uri).parse().unwrap(),
        );

        let task_router = TaskRouter::new();
        Self {
            provider,
            db,
            task_router,
            state_location,
            provisioner_host: Endpoint::new(format!("http://{}:8000", args.provisioner_host))
                .expect("to have a valid provisioner endpoint"),
            auth_host: args.auth_uri,
        }
    }

    pub async fn route(
        &self,
        project: &Project,
        project_name: &ProjectName,
        project_id: String,
        account_name: &AccountName,
        mut req: Request<Body>,
    ) -> Result<Response<Body>, Error> {
        let target_ip = project
            .target_ip()?
            .ok_or_else(|| Error::from_kind(ErrorKind::ProjectNotReady))?;

        let target_url = format!("http://{target_ip}:8001");

        debug!(target_url, "routing control");

        let control_key = self.control_key_from_project_name(project_name).await?;

        let headers = req.headers_mut();
        headers.typed_insert(XShuttleAccountName(account_name.to_string()));
        headers.typed_insert(XShuttleAdminSecret(control_key));
        headers.typed_insert(XShuttleProjectId(project_id));

        let cx = Span::current().context();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut HeaderInjector(headers))
        });

        let resp = PROXY_CLIENT
            .call(Ipv4Addr::LOCALHOST.into(), &target_url, req)
            .await
            .map_err(|_| Error::from_kind(ErrorKind::ProjectUnavailable))?;

        Ok(resp)
    }

    pub async fn iter_projects(
        &self,
    ) -> Result<impl ExactSizeIterator<Item = (ProjectName, AccountName)>, Error> {
        let iter = query("SELECT project_name, account_name FROM projects")
            .fetch_all(&self.db)
            .await?
            .into_iter()
            .map(|row| (row.get("project_name"), row.get("account_name")));
        Ok(iter)
    }

    pub async fn find_project(&self, project_name: &ProjectName) -> Result<Project, Error> {
        query("SELECT project_state FROM projects WHERE project_name=?1")
            .bind(project_name)
            .fetch_optional(&self.db)
            .await?
            .map(|r| {
                r.try_get::<SqlxJson<Project>, _>("project_state")
                    .unwrap()
                    .0
            })
            .ok_or_else(|| Error::from_kind(ErrorKind::ProjectNotFound))
    }

    pub async fn iter_user_projects_detailed(
        &self,
        account_name: &AccountName,
        offset: u32,
        limit: u32,
    ) -> Result<impl Iterator<Item = (ProjectName, Project)>, Error> {
        let mut query = QueryBuilder::new(
            "SELECT project_name, project_state FROM projects WHERE account_name = ",
        );

        query
            .push_bind(account_name)
            .push(" ORDER BY project_id DESC, project_name LIMIT ")
            .push_bind(limit);

        if offset > 0 {
            query.push(" OFFSET ").push_bind(offset);
        }

        let iter = query
            .build()
            .fetch_all(&self.db)
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.get("project_name"),
                    row.get::<SqlxJson<Project>, _>("project_state").0,
                )
            });
        Ok(iter)
    }

    pub async fn iter_user_projects_detailed_filtered(
        &self,
        account_name: AccountName,
        filter: String,
    ) -> Result<impl Iterator<Item = (ProjectName, Project)>, Error> {
        let iter =
            query("SELECT project_name, project_state FROM projects WHERE account_name = ?1 AND project_state = ?2")
                .bind(account_name)
                .bind(filter)
                .fetch_all(&self.db)
                .await?
                .into_iter()
                .map(|row| {
                    (
                        row.get("project_name"),
                        row.get::<SqlxJson<Project>, _>("project_state").0,
                    )
                });
        Ok(iter)
    }

    pub async fn update_project(
        &self,
        project_name: &ProjectName,
        project: &Project,
    ) -> Result<(), Error> {
        let query = match project {
            Project::Creating(state) => query(
                "UPDATE projects SET initial_key = ?1, project_state = ?2 WHERE project_name = ?3",
            )
            .bind(state.initial_key())
            .bind(SqlxJson(project))
            .bind(project_name),
            _ => query("UPDATE projects SET project_state = ?1 WHERE project_name = ?2")
                .bind(SqlxJson(project))
                .bind(project_name),
        };
        query.execute(&self.db).await?;
        Ok(())
    }

    pub async fn account_name_from_project(
        &self,
        project_name: &ProjectName,
    ) -> Result<AccountName, Error> {
        query("SELECT account_name FROM projects WHERE project_name = ?1")
            .bind(project_name)
            .fetch_optional(&self.db)
            .await?
            .map(|row| row.get("account_name"))
            .ok_or_else(|| Error::from(ErrorKind::ProjectNotFound))
    }

    pub async fn control_key_from_project_name(
        &self,
        project_name: &ProjectName,
    ) -> Result<String, Error> {
        let control_key = query("SELECT initial_key FROM projects WHERE project_name = ?1")
            .bind(project_name)
            .fetch_optional(&self.db)
            .await?
            .map(|row| row.try_get("initial_key").unwrap())
            .ok_or_else(|| Error::from(ErrorKind::ProjectNotFound))?;
        Ok(control_key)
    }

    pub async fn iter_user_projects(
        &self,
        AccountName(account_name): &AccountName,
    ) -> Result<impl Iterator<Item = ProjectName>, Error> {
        let iter = query("SELECT project_name FROM projects WHERE account_name = ?1")
            .bind(account_name)
            .fetch_all(&self.db)
            .await?
            .into_iter()
            .map(|row| row.try_get::<ProjectName, _>("project_name").unwrap());
        Ok(iter)
    }

    pub async fn create_project(
        &self,
        project_name: ProjectName,
        account_name: AccountName,
        is_admin: bool,
        idle_minutes: u64,
    ) -> Result<Project, Error> {
        if let Some(row) = query(
            r#"
        SELECT project_name, project_id, account_name, initial_key, project_state
        FROM projects
        WHERE (project_name = ?1)
        AND (account_name = ?2 OR ?3)
        "#,
        )
        .bind(&project_name)
        .bind(&account_name)
        .bind(is_admin)
        .fetch_optional(&self.db)
        .await?
        {
            // If the project already exists and belongs to this account
            let project = row.get::<SqlxJson<Project>, _>("project_state").0;
            let project_id = row.get::<String, _>("project_id");
            if project.is_destroyed() {
                // But is in `::Destroyed` state, recreate it
                let mut creating = ProjectCreating::new_with_random_initial_key(
                    project_name.clone(),
                    Ulid::from_string(project_id.as_str()).map_err(|err| {
                        Error::custom(
                            ErrorKind::Internal,
                            format!("The project id of the destroyed project is not a valid ULID: {err}"),
                        )
                    })?,
                    idle_minutes,
                );
                // Restore previous custom domain, if any
                match self.find_custom_domain_for_project(&project_id).await {
                    Ok(custom_domain) => {
                        creating = creating.with_fqdn(custom_domain.fqdn.to_string());
                    }
                    Err(error) if error.kind() == ErrorKind::CustomDomainNotFound => {
                        // no previous custom domain
                    }
                    Err(error) => return Err(error),
                }
                let project = Project::Creating(creating);
                self.update_project(&project_name, &project).await?;
                Ok(project)
            } else {
                // Otherwise it already exists
                Err(Error::from_kind(ErrorKind::ProjectAlreadyExists))
            }
        } else {
            // Check if project name is valid according to new rules if it
            // doesn't exist.
            // TODO: remove this check when we update the project name rules
            // in shuttle-common
            if project_name.is_valid() {
                // Otherwise attempt to create a new one. This will fail
                // outright if the project already exists (this happens if
                // it belongs to another account).
                self.insert_project(project_name, Ulid::new(), account_name, idle_minutes)
                    .await
            } else {
                Err(Error::from_kind(ErrorKind::InvalidProjectName))
            }
        }
    }

    pub async fn insert_project(
        &self,
        project_name: ProjectName,
        project_id: Ulid,
        account_name: AccountName,
        idle_minutes: u64,
    ) -> Result<Project, Error> {
        let project = SqlxJson(Project::Creating(
            ProjectCreating::new_with_random_initial_key(
                project_name.clone(),
                project_id,
                idle_minutes,
            ),
        ));

        query("INSERT INTO projects (project_id, project_name, account_name, initial_key, project_state) VALUES (ulid(), ?1, ?2, ?3, ?4)")
            .bind(&project_name)
            .bind(&account_name)
            .bind(project.initial_key().unwrap())
            .bind(&project)
            .execute(&self.db)
            .await
            .map_err(|err| {
                // If the error is a broken PK constraint, this is a
                // project name clash
                if let Some(db_err_code) = err.as_database_error().and_then(DatabaseError::code) {
                    if db_err_code == "2067" {  // SQLITE_CONSTRAINT_UNIQUE
                        return Error::from_kind(ErrorKind::ProjectAlreadyExists)
                    }
                }
                // Otherwise this is internal
                err.into()
            })?;

        let project = project.0;

        Ok(project)
    }

    pub async fn create_custom_domain(
        &self,
        project_name: &ProjectName,
        fqdn: &Fqdn,
        certs: &str,
        private_key: &str,
    ) -> Result<(), Error> {
        let project_id = query("SELECT project_id FROM projects WHERE project_name = ?1")
            .bind(project_name)
            .fetch_one(&self.db)
            .await?
            .get::<String, _>("project_id");

        query("INSERT OR REPLACE INTO custom_domains (fqdn, project_id, certificate, private_key) VALUES (?1, ?2, ?3, ?4)")
            .bind(fqdn.to_string())
            .bind(project_id)
            .bind(certs)
            .bind(private_key)
            .execute(&self.db)
            .await?;

        Ok(())
    }

    pub async fn iter_custom_domains(&self) -> Result<impl Iterator<Item = CustomDomain>, Error> {
        query("SELECT fqdn, project_name, certificate, private_key FROM custom_domains AS cd JOIN projects AS p ON cd.project_id = p.project_id")
            .fetch_all(&self.db)
            .await
            .map(|res| {
                res.into_iter().map(|row| CustomDomain {
                    fqdn: row.get::<&str, _>("fqdn").parse().unwrap(),
                    project_name: row.try_get("project_name").unwrap(),
                    certificate: row.get("certificate"),
                    private_key: row.get("private_key"),
                })
            })
            .map_err(|_| Error::from_kind(ErrorKind::Internal))
    }

    async fn find_custom_domain_for_project(
        &self,
        project_id: &str,
    ) -> Result<CustomDomain, Error> {
        let custom_domain = query(
            "SELECT fqdn, project_name, certificate, private_key FROM custom_domains AS cd JOIN projects AS p ON cd.project_id = p.project_id WHERE p.project_id = ?1",
        )
        .bind(project_id)
        .fetch_optional(&self.db)
        .await?
        .map(|row| CustomDomain {
            fqdn: row.get::<&str, _>("fqdn").parse().unwrap(),
            project_name: row.try_get("project_name").unwrap(),
            certificate: row.get("certificate"),
            private_key: row.get("private_key"),
        })
        .ok_or_else(|| Error::from(ErrorKind::CustomDomainNotFound))?;
        Ok(custom_domain)
    }

    pub async fn project_details_for_custom_domain(
        &self,
        fqdn: &Fqdn,
    ) -> Result<CustomDomain, Error> {
        let custom_domain = query(
            "SELECT fqdn, project_name, certificate, private_key FROM custom_domains AS cd JOIN projects AS p ON cd.project_id = p.project_id WHERE fqdn = ?1",
        )
        .bind(fqdn.to_string())
        .fetch_optional(&self.db)
        .await?
        .map(|row| CustomDomain {
            fqdn: row.get::<&str, _>("fqdn").parse().unwrap(),
            project_name: row.try_get("project_name").unwrap(),
            certificate: row.get("certificate"),
            private_key: row.get("private_key"),
        })
        .ok_or_else(|| Error::from(ErrorKind::CustomDomainNotFound))?;
        Ok(custom_domain)
    }

    pub async fn iter_projects_detailed(
        &self,
    ) -> Result<impl Iterator<Item = ProjectDetails>, Error> {
        let iter = query("SELECT project_name, account_name FROM projects")
            .fetch_all(&self.db)
            .await?
            .into_iter()
            .map(|row| ProjectDetails {
                project_name: row.try_get("project_name").unwrap(),
                account_name: row.try_get("account_name").unwrap(),
            });
        Ok(iter)
    }

    /// Returns the current certificate as a pair of the chain and private key.
    /// If the pair doesn't exist for a specific project, create both the certificate
    /// and the custom domain it will represent.
    pub async fn create_custom_domain_certificate(
        &self,
        fqdn: &Fqdn,
        acme_client: &AcmeClient,
        project_name: &ProjectName,
        creds: AccountCredentials<'_>,
    ) -> Result<(String, String), Error> {
        match self.project_details_for_custom_domain(fqdn).await {
            Ok(CustomDomain {
                certificate,
                private_key,
                ..
            }) => Ok((certificate, private_key)),
            Err(err) if err.kind() == ErrorKind::CustomDomainNotFound => {
                let (certs, private_key) = acme_client
                    .create_certificate(&fqdn.to_string(), ChallengeType::Http01, creds)
                    .await?;
                self.create_custom_domain(project_name, fqdn, &certs, &private_key)
                    .await?;
                Ok((certs, private_key))
            }
            Err(err) => Err(err),
        }
    }

    async fn create_certificate<'a>(
        &self,
        acme: &AcmeClient,
        creds: AccountCredentials<'a>,
    ) -> ChainAndPrivateKey {
        let public: FQDN = self.context().settings.fqdn.parse().unwrap();
        let identifier = format!("*.{public}");

        // Use ::Dns01 challenge because that's the only supported
        // challenge type for wildcard domains.
        let (chain, private_key) = acme
            .create_certificate(&identifier, ChallengeType::Dns01, creds)
            .await
            .unwrap();

        let mut buf = Vec::new();
        buf.extend(chain.as_bytes());
        buf.extend(private_key.as_bytes());

        ChainAndPrivateKey::parse_pem(Cursor::new(buf)).expect("Malformed PEM buffer.")
    }

    /// Fetch the gateway certificate from the state location.
    /// If not existent, create the gateway certificate and save it to the
    /// gateway state.
    pub async fn fetch_certificate(
        &self,
        acme: &AcmeClient,
        creds: AccountCredentials<'_>,
    ) -> ChainAndPrivateKey {
        let tls_path = self.state_location.join("ssl.pem");
        match ChainAndPrivateKey::load_pem(&tls_path) {
            Ok(valid) => valid,
            Err(_) => {
                warn!(
                    "no valid certificate found at {}, creating one...",
                    tls_path.display()
                );

                let certs = self.create_certificate(acme, creds).await;
                certs.clone().save_pem(&tls_path).unwrap();
                certs
            }
        }
    }

    /// Renew the gateway certificate if there less than 30 days until the current
    /// certificate expiration.
    pub(crate) async fn renew_certificate(
        &self,
        acme: &AcmeClient,
        resolver: Arc<GatewayCertResolver>,
        creds: AccountCredentials<'_>,
    ) {
        let account = AccountWrapper::from(creds).0;
        let certs = self.fetch_certificate(acme, account.credentials()).await;
        // Safe to unwrap because a 'ChainAndPrivateKey' is built from a PEM.
        let chain_and_pk = certs.into_pem().unwrap();

        let (_, pem) = parse_x509_pem(chain_and_pk.as_bytes())
            .unwrap_or_else(|_| panic!("Malformed existing PEM certificate for the gateway."));
        let (_, x509_cert) = parse_x509_certificate(pem.contents.as_bytes())
            .unwrap_or_else(|_| panic!("Malformed existing X509 certificate for the gateway."));

        // We compute the difference between the certificate expiry date and current timestamp because we want to trigger the
        // gateway certificate renewal only during it's last 30 days of validity or if the certificate is expired.
        let diff = x509_cert.validity().not_after.sub(ASN1Time::now());

        // Renew only when the difference is `None` (meaning certificate expired) or we're within the last 30 days of validity.
        if diff.is_none()
            || diff
                .expect("to be Some given we checked for None previously")
                .whole_days()
                <= RENEWAL_VALIDITY_THRESHOLD_IN_DAYS
        {
            let tls_path = self.state_location.join("ssl.pem");
            let certs = self.create_certificate(acme, account.credentials()).await;
            resolver
                .serve_default_der(certs.clone())
                .await
                .expect("Failed to serve the default certs");
            certs
                .save_pem(&tls_path)
                .expect("to save the certificate locally");
        }
    }

    pub fn context(&self) -> GatewayContext {
        self.provider.context()
    }

    /// Create a builder for a new [ProjectTask]
    pub fn new_task(self: &Arc<Self>) -> TaskBuilder {
        TaskBuilder::new(self.clone())
    }

    /// Find a project by name. And start the project if it is idle, waiting for it to start up
    pub async fn find_or_start_project(
        self: &Arc<Self>,
        project_name: &ProjectName,
        task_sender: Sender<BoxedTask>,
    ) -> Result<Project, Error> {
        let mut project = self.find_project(project_name).await?;

        // Start the project if it is idle
        if project.is_stopped() {
            trace!(%project_name, "starting up idle project");

            let handle = self
                .new_task()
                .project(project_name.clone())
                .and_then(task::start())
                .and_then(task::run_until_done())
                .and_then(task::start_idle_deploys())
                .and_then(task::check_health())
                .send(&task_sender)
                .await?;

            // Wait for project to come up and set new state
            handle.await;
            project = self.find_project(project_name).await?;
        }

        Ok(project)
    }

    /// Get project id of a project, by name.
    pub async fn project_id(self: &Arc<Self>, project_name: &ProjectName) -> Result<String, Error> {
        Ok(
            query("SELECT project_id FROM projects WHERE project_name = ?1")
                .bind(project_name)
                .fetch_one(&self.db)
                .await?
                .get::<String, _>("project_id"),
        )
    }

    pub fn task_router(&self) -> TaskRouter<BoxedTask> {
        self.task_router.clone()
    }

    pub fn credentials(&self) -> AccountCredentials<'_> {
        let creds_path = self.state_location.join("acme.json");
        if !creds_path.exists() {
            panic!(
                "no ACME credentials found at {}, cannot continue with certificate creation",
                creds_path.display()
            );
        }

        serde_json::from_reader(std::fs::File::open(creds_path).expect("Invalid credentials path"))
            .expect("Can not parse admin credentials from path")
    }

    pub fn provisioner_host(&self) -> &Endpoint {
        &self.provisioner_host
    }
    pub fn auth_uri(&self) -> &Uri {
        &self.auth_host
    }
}

#[derive(Clone)]
pub struct GatewayContext {
    docker: Docker,
    settings: ContainerSettings,
    api_key: String,
    auth_key_uri: Uri,
}

impl DockerContext for GatewayContext {
    fn docker(&self) -> &Docker {
        &self.docker
    }

    fn container_settings(&self) -> &ContainerSettings {
        &self.settings
    }
}

impl GatewayContext {
    #[instrument(skip(self), fields(auth_key_uri = %self.auth_key_uri, api_key = self.api_key))]
    pub async fn get_jwt(&self) -> String {
        let req = Request::builder()
            .uri(self.auth_key_uri.clone())
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key))
            .body(Body::empty())
            .unwrap();

        trace!("getting jwt");

        let resp = timeout(IS_HEALTHY_TIMEOUT, AUTH_CLIENT.request(req)).await;

        if let Ok(Ok(resp)) = resp {
            let body = hyper::body::to_bytes(resp.into_body())
                .await
                .unwrap_or_default();
            let convert: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();

            trace!(?convert, "got jwt response");

            convert["token"].as_str().unwrap_or_default().to_string()
        } else {
            Default::default()
        }
    }
}

#[cfg(test)]
pub mod tests {
    use fqdn::FQDN;

    use super::*;

    use crate::task::{self, TaskResult};
    use crate::tests::{assert_err_kind, World};
    use crate::{Error, ErrorKind};

    #[tokio::test]
    async fn service_create_find_delete_project() -> anyhow::Result<()> {
        let world = World::new().await;
        let svc = Arc::new(GatewayService::init(world.args(), world.pool(), "".into()).await);

        let neo: AccountName = "neo".parse().unwrap();
        let trinity: AccountName = "trinity".parse().unwrap();
        let matrix: ProjectName = "matrix".parse().unwrap();

        let creating_same_project_name = |project: &Project, project_name: &ProjectName| {
            matches!(
                project,
                Project::Creating(creating) if creating.project_name() == project_name
            )
        };

        let project = svc
            .create_project(matrix.clone(), neo.clone(), false, 0)
            .await
            .unwrap();

        assert!(creating_same_project_name(&project, &matrix));

        assert_eq!(svc.find_project(&matrix).await.unwrap(), project);
        assert_eq!(
            svc.iter_projects_detailed()
                .await
                .unwrap()
                .next()
                .expect("to get one project with its user"),
            ProjectDetails {
                project_name: matrix.clone(),
                account_name: neo.clone(),
            }
        );
        assert_eq!(
            svc.iter_user_projects_detailed(&neo, 0, u32::MAX)
                .await
                .unwrap()
                .map(|item| item.0)
                .collect::<Vec<_>>(),
            vec![matrix.clone()]
        );

        // Test project pagination, first create 20 test projects (including the one from above).
        for p in (1..20).map(|p| format!("matrix-{p}")) {
            svc.create_project(ProjectName(p.clone()), neo.clone(), false, 0)
                .await
                .unwrap();
        }

        // We need to fetch all of them from the DB since they are ordered by created_at (in the id) and project_name,
        // and created_at will be the same for some of them.
        let all_projects = svc
            .iter_user_projects_detailed(&neo, 0, u32::MAX)
            .await
            .unwrap()
            .map(|item| item.0)
            .collect::<Vec<_>>();

        assert_eq!(all_projects.len(), 20);

        // Get first 5 projects.
        let paginated = svc
            .iter_user_projects_detailed(&neo, 0, 5)
            .await
            .unwrap()
            .map(|item| item.0)
            .collect::<Vec<_>>();

        assert_eq!(all_projects[..5], paginated);

        // Get 10 projects starting at an offset of 10.
        let paginated = svc
            .iter_user_projects_detailed(&neo, 10, 10)
            .await
            .unwrap()
            .map(|item| item.0)
            .collect::<Vec<_>>();
        assert_eq!(all_projects[10..20], paginated);

        // Get 20 projects starting at an offset of 200.
        let paginated = svc
            .iter_user_projects_detailed(&neo, 200, 20)
            .await
            .unwrap()
            .collect::<Vec<_>>();

        assert!(paginated.is_empty());

        let mut work = svc
            .new_task()
            .project(matrix.clone())
            .and_then(task::destroy())
            .build();

        while let TaskResult::Pending(_) = work.poll(()).await {}
        assert!(matches!(work.poll(()).await, TaskResult::Done(())));

        // After project has been destroyed...
        assert!(matches!(
            svc.find_project(&matrix).await,
            Ok(Project::Destroyed(_))
        ));

        // If recreated by a different user
        assert!(matches!(
            svc.create_project(matrix.clone(), trinity.clone(), false, 0)
                .await,
            Err(Error {
                kind: ErrorKind::ProjectAlreadyExists,
                ..
            })
        ));

        // If recreated by the same user
        assert!(matches!(
            svc.create_project(matrix.clone(), neo, false, 0).await,
            Ok(Project::Creating(_))
        ));

        let mut work = svc
            .new_task()
            .project(matrix.clone())
            .and_then(task::destroy())
            .build();

        while let TaskResult::Pending(_) = work.poll(()).await {}
        assert!(matches!(work.poll(()).await, TaskResult::Done(())));

        // After project has been destroyed again...
        assert!(matches!(
            svc.find_project(&matrix).await,
            Ok(Project::Destroyed(_))
        ));

        // If recreated by an admin
        assert!(matches!(
            svc.create_project(matrix, trinity, true, 0).await,
            Ok(Project::Creating(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn service_create_ready_kill_restart_docker() -> anyhow::Result<()> {
        let world = World::new().await;
        let svc = Arc::new(GatewayService::init(world.args(), world.pool(), "".into()).await);

        let neo: AccountName = "neo".parse().unwrap();
        let matrix: ProjectName = "matrix".parse().unwrap();

        svc.create_project(matrix.clone(), neo.clone(), false, 0)
            .await
            .unwrap();

        let mut task = svc.new_task().project(matrix.clone()).build();

        while let TaskResult::Pending(_) = task.poll(()).await {
            // keep polling
        }

        let project = svc.find_project(&matrix).await.unwrap();
        println!("{:?}", project);
        assert!(project.is_ready());

        let container = project.container().unwrap();
        svc.context()
            .docker()
            .kill_container::<String>(container.name.unwrap().strip_prefix('/').unwrap(), None)
            .await
            .unwrap();

        println!("killed container");

        let mut ambulance_task = svc
            .new_task()
            .project(matrix.clone())
            .and_then(task::check_health())
            .build();

        // the first poll will trigger a refresh
        let _ = ambulance_task.poll(()).await;

        let project = svc.find_project(&matrix).await.unwrap();
        println!("{:?}", project);
        assert!(!project.is_ready());

        // the subsequent will trigger a restart task
        while let TaskResult::Pending(_) = ambulance_task.poll(()).await {
            // keep polling
        }

        let project = svc.find_project(&matrix).await.unwrap();
        println!("{:?}", project);
        assert!(project.is_ready());

        Ok(())
    }

    #[tokio::test]
    async fn service_create_find_custom_domain() -> anyhow::Result<()> {
        let world = World::new().await;
        let svc = Arc::new(GatewayService::init(world.args(), world.pool(), "".into()).await);

        let account: AccountName = "neo".parse().unwrap();
        let project_name: ProjectName = "matrix".parse().unwrap();
        let domain: FQDN = "neo.the.matrix".parse().unwrap();
        let certificate = "dummy certificate";
        let private_key = "dummy private key";

        assert_err_kind!(
            svc.project_details_for_custom_domain(&domain).await,
            ErrorKind::CustomDomainNotFound
        );

        let _ = svc
            .create_project(project_name.clone(), account.clone(), false, 0)
            .await
            .unwrap();

        svc.create_custom_domain(&project_name, &domain, certificate, private_key)
            .await
            .unwrap();

        let custom_domain = svc
            .project_details_for_custom_domain(&domain)
            .await
            .unwrap();

        assert_eq!(custom_domain.project_name, project_name);
        assert_eq!(custom_domain.certificate, certificate);
        assert_eq!(custom_domain.private_key, private_key);

        // Should auto replace the domain details
        let certificate = "dummy certificate update";
        let private_key = "dummy private key update";

        svc.create_custom_domain(&project_name, &domain, certificate, private_key)
            .await
            .unwrap();

        let custom_domain = svc
            .project_details_for_custom_domain(&domain)
            .await
            .unwrap();

        assert_eq!(custom_domain.project_name, project_name);
        assert_eq!(custom_domain.certificate, certificate);
        assert_eq!(custom_domain.private_key, private_key);

        Ok(())
    }

    #[tokio::test]
    async fn service_create_custom_domain_destroy_recreate_project() -> anyhow::Result<()> {
        let world = World::new().await;
        let svc = Arc::new(GatewayService::init(world.args(), world.pool(), "".into()).await);

        let account: AccountName = "neo".parse().unwrap();
        let project_name: ProjectName = "matrix".parse().unwrap();
        let domain: FQDN = "neo.the.matrix".parse().unwrap();
        let certificate = "dummy certificate";
        let private_key = "dummy private key";

        assert_err_kind!(
            svc.project_details_for_custom_domain(&domain).await,
            ErrorKind::CustomDomainNotFound
        );

        let _ = svc
            .create_project(project_name.clone(), account.clone(), false, 0)
            .await
            .unwrap();

        svc.create_custom_domain(&project_name, &domain, certificate, private_key)
            .await
            .unwrap();

        let mut work = svc
            .new_task()
            .project(project_name.clone())
            .and_then(task::destroy())
            .build();

        while let TaskResult::Pending(_) = work.poll(()).await {}
        assert!(matches!(work.poll(()).await, TaskResult::Done(())));

        let recreated_project = svc
            .create_project(project_name.clone(), account.clone(), false, 0)
            .await
            .unwrap();

        let Project::Creating(creating) = recreated_project else {
            panic!("Project should be Creating");
        };
        assert_eq!(creating.fqdn(), &Some(domain.to_string()));

        Ok(())
    }
}
