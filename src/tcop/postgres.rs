#[cfg(target_arch = "wasm32")]
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac_array;
#[cfg(not(target_arch = "wasm32"))]
use rand::RngCore;
#[cfg(not(target_arch = "wasm32"))]
use rand::rngs::OsRng;
use sha2::{Digest, Sha256};

use crate::parser::ast::{Expr, QueryExpr, Statement};
use crate::parser::sql_parser::parse_statement;
use crate::security::{self, CreateRoleOptions, RlsCommand, RlsPolicy, TablePrivilege};
use crate::tcop::engine::{
    EngineError, PlannedQuery, QueryResult, ScalarValue, copy_insert_rows,
    copy_table_binary_snapshot, copy_table_column_oids, execute_planned_query, plan_statement,
    restore_state, snapshot_state, type_oid_size,
};
use crate::txn::TransactionState;
use crate::txn::visibility::VisibilityMode;

pub type PgType = u32;

const UNNAMED: &str = "";

#[cfg(target_arch = "wasm32")]
thread_local! {
    static WASM_RANDOM_STATE: Cell<u64> = const { Cell::new(0xA5A5_5A5A_DEAD_BEEF) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDescriptionField {
    pub name: String,
    pub table_oid: u32,
    pub column_attr: i16,
    pub type_oid: PgType,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    Startup {
        user: String,
        database: Option<String>,
        parameters: Vec<(String, String)>,
    },
    Password {
        password: String,
    },
    SaslInitialResponse {
        mechanism: String,
        data: Vec<u8>,
    },
    SaslResponse {
        data: Vec<u8>,
    },
    SslRequest,
    CancelRequest {
        process_id: u32,
        secret_key: u32,
    },
    Query {
        sql: String,
    },
    Parse {
        statement_name: String,
        query: String,
        parameter_types: Vec<PgType>,
    },
    Bind {
        portal_name: String,
        statement_name: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Execute {
        portal_name: String,
        max_rows: i64,
    },
    DescribeStatement {
        statement_name: String,
    },
    DescribePortal {
        portal_name: String,
    },
    CloseStatement {
        statement_name: String,
    },
    ClosePortal {
        portal_name: String,
    },
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    CopyFail {
        message: String,
    },
    Flush,
    Sync,
    Terminate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationSasl {
        mechanisms: Vec<String>,
    },
    AuthenticationSaslContinue {
        data: Vec<u8>,
    },
    AuthenticationSaslFinal {
        data: Vec<u8>,
    },
    ParameterStatus {
        name: String,
        value: String,
    },
    BackendKeyData {
        process_id: u32,
        secret_key: u32,
    },
    NoticeResponse {
        message: String,
    },
    ReadyForQuery {
        status: ReadyForQueryStatus,
    },
    ParseComplete,
    BindComplete,
    CloseComplete,
    EmptyQueryResponse,
    DataRow {
        values: Vec<String>,
    },
    CommandComplete {
        tag: String,
        rows: u64,
    },
    ParameterDescription {
        parameter_types: Vec<PgType>,
    },
    RowDescription {
        fields: Vec<RowDescriptionField>,
    },
    NoData,
    PortalSuspended,
    CopyInResponse {
        overall_format: i8,
        column_formats: Vec<i16>,
    },
    CopyOutResponse {
        overall_format: i8,
        column_formats: Vec<i16>,
    },
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    ErrorResponse {
        message: String,
    },
    FlushComplete,
    Terminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyForQueryStatus {
    Idle,
    InTransaction,
    FailedTransaction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionError {
    pub message: String,
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SessionError {}

impl From<EngineError> for SessionError {
    fn from(value: EngineError) -> Self {
        Self {
            message: value.message,
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedStatement {
    operation: PlannedOperation,
    parameter_types: Vec<PgType>,
}

#[derive(Debug, Clone)]
struct Portal {
    operation: PlannedOperation,
    params: Vec<Option<String>>,
    result_format_codes: Vec<i16>,
    result_cache: Option<QueryResult>,
    cursor: usize,
    row_description_sent: bool,
}

#[derive(Debug, Clone)]
enum PlannedOperation {
    ParsedQuery(PlannedQuery),
    Transaction(TransactionCommand),
    Security(SecurityCommand),
    Copy(CopyCommand),
    Utility(String),
    Empty,
}

impl PlannedOperation {
    fn command_tag(&self) -> String {
        match self {
            Self::ParsedQuery(plan) => plan.command_tag().to_string(),
            Self::Transaction(TransactionCommand::Begin) => "BEGIN".to_string(),
            Self::Transaction(TransactionCommand::Commit) => "COMMIT".to_string(),
            Self::Transaction(TransactionCommand::Rollback) => "ROLLBACK".to_string(),
            Self::Transaction(TransactionCommand::Savepoint(_)) => "SAVEPOINT".to_string(),
            Self::Transaction(TransactionCommand::ReleaseSavepoint(_)) => "RELEASE".to_string(),
            Self::Transaction(TransactionCommand::RollbackToSavepoint(_)) => "ROLLBACK".to_string(),
            Self::Security(command) => command.command_tag().to_string(),
            Self::Copy(_) => "COPY".to_string(),
            Self::Utility(tag) => tag.clone(),
            Self::Empty => "EMPTY".to_string(),
        }
    }

    fn returns_data(&self) -> bool {
        matches!(self, Self::ParsedQuery(plan) if plan.returns_data())
    }

    fn is_transaction_exit(&self) -> bool {
        matches!(
            self,
            Self::Transaction(TransactionCommand::Commit | TransactionCommand::Rollback)
        )
    }

    fn allowed_in_failed_transaction(&self) -> bool {
        matches!(
            self,
            Self::Transaction(
                TransactionCommand::Commit
                    | TransactionCommand::Rollback
                    | TransactionCommand::RollbackToSavepoint(_)
            )
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TransactionCommand {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyDirection {
    FromStdin,
    ToStdout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyCommand {
    table_name: Vec<String>,
    direction: CopyDirection,
    binary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScramPendingState {
    password: String,
    client_first_bare: String,
    server_first: String,
    combined_nonce: String,
    salt: Vec<u8>,
    iterations: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthenticationState {
    None,
    AwaitingPassword,
    AwaitingSaslInitial { password: String },
    AwaitingSaslResponse { pending: ScramPendingState },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyInState {
    table_name: Vec<String>,
    binary: bool,
    column_type_oids: Vec<PgType>,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
enum SecurityCommand {
    CreateRole {
        role_name: String,
        options: CreateRoleOptions,
    },
    DropRole {
        role_name: String,
        if_exists: bool,
    },
    GrantRole {
        role_name: String,
        member: String,
    },
    RevokeRole {
        role_name: String,
        member: String,
    },
    SetRole {
        role_name: String,
    },
    ResetRole,
    GrantTablePrivileges {
        table_name: Vec<String>,
        roles: Vec<String>,
        privileges: Vec<TablePrivilege>,
    },
    RevokeTablePrivileges {
        table_name: Vec<String>,
        roles: Vec<String>,
        privileges: Vec<TablePrivilege>,
    },
    SetRowLevelSecurity {
        table_name: Vec<String>,
        enabled: bool,
    },
    CreatePolicy {
        policy_name: String,
        table_name: Vec<String>,
        command: RlsCommand,
        roles: Vec<String>,
        using_expr: Option<Expr>,
        check_expr: Option<Expr>,
    },
    DropPolicy {
        policy_name: String,
        table_name: Vec<String>,
        if_exists: bool,
    },
}

impl SecurityCommand {
    fn command_tag(&self) -> &'static str {
        match self {
            Self::CreateRole { .. } => "CREATE ROLE",
            Self::DropRole { .. } => "DROP ROLE",
            Self::GrantRole { .. } | Self::GrantTablePrivileges { .. } => "GRANT",
            Self::RevokeRole { .. } | Self::RevokeTablePrivileges { .. } => "REVOKE",
            Self::SetRole { .. } => "SET",
            Self::ResetRole => "RESET",
            Self::SetRowLevelSecurity { .. } => "ALTER TABLE",
            Self::CreatePolicy { .. } => "CREATE POLICY",
            Self::DropPolicy { .. } => "DROP POLICY",
        }
    }
}

#[derive(Debug, Clone)]
struct PendingStartup {
    user: String,
    database: Option<String>,
    parameters: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct PostgresSession {
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    xact_started: bool,
    tx_state: TransactionState,
    ignore_till_sync: bool,
    doing_extended_query_message: bool,
    send_ready_for_query: bool,
    startup_complete: bool,
    authentication_state: AuthenticationState,
    pending_startup: Option<PendingStartup>,
    copy_in_state: Option<CopyInState>,
    session_user: String,
    current_role: String,
    process_id: u32,
    secret_key: u32,
}

impl Default for PostgresSession {
    fn default() -> Self {
        Self {
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            xact_started: false,
            tx_state: TransactionState::default(),
            ignore_till_sync: false,
            doing_extended_query_message: false,
            send_ready_for_query: true,
            startup_complete: true,
            authentication_state: AuthenticationState::None,
            pending_startup: None,
            copy_in_state: None,
            session_user: "postgres".to_string(),
            current_role: "postgres".to_string(),
            process_id: 1,
            secret_key: 0xC0DE_BEEF,
        }
    }
}

impl PostgresSession {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_startup_required() -> Self {
        Self {
            startup_complete: false,
            send_ready_for_query: false,
            ..Self::default()
        }
    }

    /// Rust skeleton port of PostgreSQL's `PostgresMain` message loop.
    pub fn run<I>(&mut self, messages: I) -> Vec<BackendMessage>
    where
        I: IntoIterator<Item = FrontendMessage>,
    {
        let mut out = Vec::new();
        let mut terminated = false;

        for message in messages {
            if self.send_ready_for_query && self.startup_complete && !self.ignore_till_sync {
                out.push(BackendMessage::ReadyForQuery {
                    status: self.ready_status(),
                });
                self.send_ready_for_query = false;
            }

            if self.ignore_till_sync
                && !matches!(message, FrontendMessage::Sync | FrontendMessage::Terminate)
            {
                continue;
            }

            self.doing_extended_query_message = is_extended_query_message(&message);

            match self.dispatch(message, &mut out) {
                Ok(ControlFlow::Continue) => {}
                Ok(ControlFlow::Break) => {
                    terminated = true;
                    break;
                }
                Err(err) => {
                    out.push(BackendMessage::ErrorResponse {
                        message: err.message,
                    });
                    self.handle_error_recovery();
                }
            }
        }

        if terminated {
            out.push(BackendMessage::Terminate);
            return out;
        }

        if self.send_ready_for_query && self.startup_complete && !self.ignore_till_sync {
            out.push(BackendMessage::ReadyForQuery {
                status: self.ready_status(),
            });
            self.send_ready_for_query = false;
        }

        out
    }

    fn dispatch(
        &mut self,
        message: FrontendMessage,
        out: &mut Vec<BackendMessage>,
    ) -> Result<ControlFlow, SessionError> {
        if self.copy_in_state.is_some()
            && !matches!(
                message,
                FrontendMessage::CopyData { .. }
                    | FrontendMessage::CopyDone
                    | FrontendMessage::CopyFail { .. }
                    | FrontendMessage::Flush
                    | FrontendMessage::Terminate
            )
        {
            return Err(SessionError {
                message: "COPY from stdin is in progress".to_string(),
            });
        }

        if !self.startup_complete
            && !matches!(
                message,
                FrontendMessage::Startup { .. }
                    | FrontendMessage::Password { .. }
                    | FrontendMessage::SaslInitialResponse { .. }
                    | FrontendMessage::SaslResponse { .. }
                    | FrontendMessage::SslRequest
                    | FrontendMessage::CancelRequest { .. }
                    | FrontendMessage::Terminate
            )
        {
            return Err(SessionError {
                message: "startup packet has not been processed".to_string(),
            });
        }

        match message {
            FrontendMessage::Startup {
                user,
                database,
                parameters,
            } => {
                self.exec_startup_message(user, database, parameters, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Password { password } => {
                self.exec_password_message(password, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::SaslInitialResponse { mechanism, data } => {
                self.exec_sasl_initial_response(mechanism, data, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::SaslResponse { data } => {
                self.exec_sasl_response(data, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::SslRequest => {
                out.push(BackendMessage::NoticeResponse {
                    message: "SSL is not supported by postgrust".to_string(),
                });
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CancelRequest { .. } => {
                out.push(BackendMessage::NoticeResponse {
                    message: "cancel request ignored".to_string(),
                });
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Query { sql } => {
                self.exec_simple_query(&sql, out)?;
                self.send_ready_for_query = self.copy_in_state.is_none();
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Parse {
                statement_name,
                query,
                parameter_types,
            } => {
                self.exec_parse_message(&statement_name, &query, parameter_types, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_formats,
                params,
                result_formats,
            } => {
                self.exec_bind_message(
                    &portal_name,
                    &statement_name,
                    param_formats,
                    params,
                    result_formats,
                    out,
                )?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Execute {
                portal_name,
                max_rows,
            } => {
                self.exec_execute_message(&portal_name, max_rows, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::DescribeStatement { statement_name } => {
                self.exec_describe_statement_message(&statement_name, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::DescribePortal { portal_name } => {
                self.exec_describe_portal_message(&portal_name, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CloseStatement { statement_name } => {
                self.exec_close_statement(&statement_name, out);
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::ClosePortal { portal_name } => {
                self.exec_close_portal(&portal_name, out);
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CopyData { data } => {
                self.exec_copy_data(data)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CopyDone => {
                self.exec_copy_done(out)?;
                self.send_ready_for_query = true;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CopyFail { message } => {
                self.exec_copy_fail(message)?;
                self.send_ready_for_query = true;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Flush => {
                out.push(BackendMessage::FlushComplete);
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Sync => {
                self.ignore_till_sync = false;
                self.finish_xact_command();
                self.send_ready_for_query = true;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Terminate => Ok(ControlFlow::Break),
        }
    }

    fn exec_startup_message(
        &mut self,
        user: String,
        database: Option<String>,
        parameters: Vec<(String, String)>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        if self.startup_complete {
            return Err(SessionError {
                message: "startup packet has already been processed".to_string(),
            });
        }
        let user = security::normalize_identifier(&user);
        if !security::role_exists(&user) {
            return Err(SessionError {
                message: format!("role \"{}\" does not exist", user),
            });
        }
        if !security::can_role_login(&user) {
            return Err(SessionError {
                message: format!("role \"{}\" is not permitted to login", user),
            });
        }

        self.pending_startup = Some(PendingStartup {
            user: user.clone(),
            database: database.clone(),
            parameters: parameters.clone(),
        });

        if let Some(password) = security::role_password(&user) {
            self.authentication_state = AuthenticationState::AwaitingSaslInitial { password };
            out.push(BackendMessage::AuthenticationSasl {
                mechanisms: vec!["SCRAM-SHA-256".to_string()],
            });
            return Ok(());
        }

        self.authentication_state = AuthenticationState::None;
        self.complete_startup(user, database, parameters, out);
        Ok(())
    }

    fn exec_password_message(
        &mut self,
        password: String,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        if self.authentication_state != AuthenticationState::AwaitingPassword
            && !matches!(
                self.authentication_state,
                AuthenticationState::AwaitingSaslInitial { .. }
            )
        {
            return Err(SessionError {
                message: "password message not expected".to_string(),
            });
        }
        let pending = self.pending_startup.clone().ok_or_else(|| SessionError {
            message: "startup state is missing pending user".to_string(),
        })?;
        if !security::verify_role_password(&pending.user, &password) {
            return Err(SessionError {
                message: "password authentication failed".to_string(),
            });
        }
        self.complete_startup(pending.user, pending.database, pending.parameters, out);
        Ok(())
    }

    fn exec_sasl_initial_response(
        &mut self,
        mechanism: String,
        data: Vec<u8>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        let AuthenticationState::AwaitingSaslInitial { password } = &self.authentication_state
        else {
            return Err(SessionError {
                message: "SASL initial response not expected".to_string(),
            });
        };
        if !mechanism.eq_ignore_ascii_case("SCRAM-SHA-256") {
            return Err(SessionError {
                message: format!("unsupported SASL mechanism {}", mechanism),
            });
        }

        let client_first = std::str::from_utf8(&data).map_err(|_| SessionError {
            message: "invalid SASL payload encoding".to_string(),
        })?;
        let (_gs2, client_first_bare) =
            client_first.split_once(",,").ok_or_else(|| SessionError {
                message: "invalid SCRAM client-first message".to_string(),
            })?;
        let client_nonce = scram_attribute(client_first_bare, 'r').ok_or_else(|| SessionError {
            message: "SCRAM client-first message is missing nonce".to_string(),
        })?;
        if client_nonce.is_empty() {
            return Err(SessionError {
                message: "SCRAM client nonce cannot be empty".to_string(),
            });
        }

        let mut random_nonce = [0u8; 18];
        fill_random_bytes(&mut random_nonce);
        let combined_nonce = format!("{}{}", client_nonce, BASE64_STANDARD.encode(random_nonce));

        let mut salt = [0u8; 16];
        fill_random_bytes(&mut salt);
        let iterations = 4096u32;
        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce,
            BASE64_STANDARD.encode(salt),
            iterations
        );

        self.authentication_state = AuthenticationState::AwaitingSaslResponse {
            pending: ScramPendingState {
                password: password.clone(),
                client_first_bare: client_first_bare.to_string(),
                server_first: server_first.clone(),
                combined_nonce,
                salt: salt.to_vec(),
                iterations,
            },
        };

        out.push(BackendMessage::AuthenticationSaslContinue {
            data: server_first.into_bytes(),
        });
        Ok(())
    }

    fn exec_sasl_response(
        &mut self,
        data: Vec<u8>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        let AuthenticationState::AwaitingSaslResponse { pending } = &self.authentication_state
        else {
            return Err(SessionError {
                message: "SASL response not expected".to_string(),
            });
        };
        let client_final = std::str::from_utf8(&data).map_err(|_| SessionError {
            message: "invalid SCRAM client-final payload".to_string(),
        })?;
        let final_nonce = scram_attribute(client_final, 'r').ok_or_else(|| SessionError {
            message: "SCRAM client-final message is missing nonce".to_string(),
        })?;
        if final_nonce != pending.combined_nonce {
            return Err(SessionError {
                message: "SCRAM nonce mismatch".to_string(),
            });
        }
        let Some(client_final_without_proof) =
            client_final.rsplit_once(",p=").map(|(head, _)| head)
        else {
            return Err(SessionError {
                message: "SCRAM client-final message is missing proof".to_string(),
            });
        };
        let proof_b64 = scram_attribute(client_final, 'p').ok_or_else(|| SessionError {
            message: "SCRAM client-final proof is missing".to_string(),
        })?;
        let proof = BASE64_STANDARD
            .decode(proof_b64)
            .map_err(|_| SessionError {
                message: "SCRAM proof is not valid base64".to_string(),
            })?;
        if proof.len() != 32 {
            return Err(SessionError {
                message: "SCRAM proof has invalid length".to_string(),
            });
        }

        let auth_message = format!(
            "{},{},{}",
            pending.client_first_bare, pending.server_first, client_final_without_proof
        );
        let salted_password = pbkdf2_hmac_array::<Sha256, 32>(
            pending.password.as_bytes(),
            &pending.salt,
            pending.iterations,
        );
        let client_key = scram_hmac(&salted_password, b"Client Key")?;
        let stored_key = Sha256::digest(client_key);
        let client_signature = scram_hmac(stored_key.as_slice(), auth_message.as_bytes())?;
        let expected_client_key = proof
            .iter()
            .zip(client_signature.iter())
            .map(|(lhs, rhs)| lhs ^ rhs)
            .collect::<Vec<_>>();
        let expected_stored_key = Sha256::digest(&expected_client_key);
        if expected_stored_key.as_slice() != stored_key.as_slice() {
            return Err(SessionError {
                message: "SCRAM proof verification failed".to_string(),
            });
        }

        let server_key = scram_hmac(&salted_password, b"Server Key")?;
        let server_signature = scram_hmac(&server_key, auth_message.as_bytes())?;
        let server_final = format!("v={}", BASE64_STANDARD.encode(server_signature));
        out.push(BackendMessage::AuthenticationSaslFinal {
            data: server_final.into_bytes(),
        });

        let pending_startup = self.pending_startup.clone().ok_or_else(|| SessionError {
            message: "startup state is missing pending user".to_string(),
        })?;
        self.complete_startup(
            pending_startup.user,
            pending_startup.database,
            pending_startup.parameters,
            out,
        );
        Ok(())
    }

    fn complete_startup(
        &mut self,
        user: String,
        database: Option<String>,
        parameters: Vec<(String, String)>,
        out: &mut Vec<BackendMessage>,
    ) {
        self.startup_complete = true;
        self.authentication_state = AuthenticationState::None;
        self.pending_startup = None;
        self.session_user = user.clone();
        self.current_role = user;
        self.send_ready_for_query = false;

        out.push(BackendMessage::AuthenticationOk);
        out.push(BackendMessage::ParameterStatus {
            name: "server_version".to_string(),
            value: "16.0-postgrust".to_string(),
        });
        out.push(BackendMessage::ParameterStatus {
            name: "client_encoding".to_string(),
            value: "UTF8".to_string(),
        });
        out.push(BackendMessage::ParameterStatus {
            name: "DateStyle".to_string(),
            value: "ISO, MDY".to_string(),
        });
        out.push(BackendMessage::ParameterStatus {
            name: "integer_datetimes".to_string(),
            value: "on".to_string(),
        });
        if let Some(db) = database {
            out.push(BackendMessage::ParameterStatus {
                name: "database".to_string(),
                value: db,
            });
        }
        for (name, value) in parameters {
            let normalized = name.to_ascii_lowercase();
            if normalized == "user" || normalized == "database" {
                continue;
            }
            out.push(BackendMessage::ParameterStatus { name, value });
        }
        out.push(BackendMessage::BackendKeyData {
            process_id: self.process_id,
            secret_key: self.secret_key,
        });
        out.push(BackendMessage::ReadyForQuery {
            status: self.ready_status(),
        });
    }

    fn exec_simple_query(
        &mut self,
        query_string: &str,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();
        self.drop_unnamed_stmt();

        let statements = split_simple_query_statements(query_string);
        if statements.is_empty() {
            out.push(BackendMessage::EmptyQueryResponse);
            self.finish_xact_command();
            return Ok(());
        }

        for statement_sql in statements {
            let operation = self.plan_query_string(&statement_sql)?;
            let row_description = operation_row_description_fields(&operation, &[])?;

            if self.is_aborted_transaction_block() && !operation.allowed_in_failed_transaction() {
                return Err(SessionError {
                    message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                });
            }

            let outcome = self.execute_operation(&operation, &[])?;
            let copy_in_started = matches!(outcome, ExecutionOutcome::CopyInStart { .. });
            Self::emit_outcome(out, outcome, i64::MAX, None, row_description.as_deref())?;
            if copy_in_started {
                return Ok(());
            }
        }

        self.finish_xact_command();
        Ok(())
    }

    fn exec_parse_message(
        &mut self,
        statement_name: &str,
        query_string: &str,
        parameter_types: Vec<PgType>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();

        let operation = self.plan_query_string(query_string)?;
        if self.is_aborted_transaction_block() && !operation.allowed_in_failed_transaction() {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        let prepared = PreparedStatement {
            operation,
            parameter_types,
        };

        if statement_name.is_empty() {
            self.drop_unnamed_stmt();
            self.prepared_statements
                .insert(UNNAMED.to_string(), prepared);
        } else {
            self.prepared_statements
                .insert(statement_name.to_string(), prepared);
        }

        out.push(BackendMessage::ParseComplete);
        Ok(())
    }

    fn exec_bind_message(
        &mut self,
        portal_name: &str,
        statement_name: &str,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();

        let prepared = self.fetch_prepared_statement(statement_name)?.clone();
        if params.len() != prepared.parameter_types.len() {
            return Err(SessionError {
                message: format!(
                    "bind message supplies {} parameters, but prepared statement \"{}\" requires {}",
                    params.len(),
                    if statement_name.is_empty() {
                        "<unnamed>"
                    } else {
                        statement_name
                    },
                    prepared.parameter_types.len()
                ),
            });
        }

        let normalized_param_formats =
            normalize_format_codes(&param_formats, params.len(), "bind parameter format codes")?;
        if normalized_param_formats.iter().any(|format| *format != 0) {
            return Err(SessionError {
                message: "binary bind parameter formats are not supported yet".to_string(),
            });
        }
        let params = params
            .into_iter()
            .map(|param| match param {
                None => Ok(None),
                Some(raw) => String::from_utf8(raw).map(Some).map_err(|_| SessionError {
                    message: "bind parameter contains invalid UTF-8 text".to_string(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let result_column_count = match &prepared.operation {
            PlannedOperation::ParsedQuery(plan) if plan.returns_data() => plan.columns().len(),
            _ => 0,
        };
        let normalized_result_formats = normalize_format_codes(
            &result_formats,
            result_column_count,
            "bind result format codes",
        )?;
        if normalized_result_formats.iter().any(|format| *format != 0) {
            return Err(SessionError {
                message: "binary result formats are not supported yet".to_string(),
            });
        }

        if self.is_aborted_transaction_block()
            && !prepared.operation.allowed_in_failed_transaction()
        {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        let portal = Portal {
            operation: prepared.operation,
            params,
            result_format_codes: normalized_result_formats,
            result_cache: None,
            cursor: 0,
            row_description_sent: false,
        };

        let key = portal_key(portal_name);
        self.portals.insert(key, portal);

        out.push(BackendMessage::BindComplete);
        Ok(())
    }

    fn exec_execute_message(
        &mut self,
        portal_name: &str,
        max_rows: i64,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();

        let key = portal_key(portal_name);
        let (operation, params, result_formats, cached_result, cursor, row_desc_sent) = {
            let portal = self.portals.get(&key).ok_or_else(|| SessionError {
                message: format!("portal \"{}\" does not exist", portal_name),
            })?;
            (
                portal.operation.clone(),
                portal.params.clone(),
                portal.result_format_codes.clone(),
                portal.result_cache.clone(),
                portal.cursor,
                portal.row_description_sent,
            )
        };

        if matches!(operation, PlannedOperation::Empty) {
            out.push(BackendMessage::EmptyQueryResponse);
            return Ok(());
        }

        if self.is_aborted_transaction_block() && !operation.allowed_in_failed_transaction() {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        let outcome = if let Some(result) = cached_result {
            ExecutionOutcome::Query(result)
        } else {
            self.execute_operation(&operation, &params)?
        };
        let row_description = operation_row_description_fields(&operation, &result_formats)?;

        let portal = self.portals.get_mut(&key).ok_or_else(|| SessionError {
            message: format!("portal \"{}\" does not exist", portal_name),
        })?;

        if portal.result_cache.is_none() {
            if let ExecutionOutcome::Query(result) = &outcome {
                portal.result_cache = Some(result.clone());
            }
        }

        Self::emit_outcome(
            out,
            outcome,
            max_rows,
            Some((portal, cursor, row_desc_sent)),
            row_description.as_deref(),
        )?;

        if operation.is_transaction_exit() {
            self.finish_xact_command();
        }

        Ok(())
    }

    fn exec_describe_statement_message(
        &mut self,
        statement_name: &str,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();
        let prepared = self.fetch_prepared_statement(statement_name)?;

        if self.is_aborted_transaction_block() && prepared.operation.returns_data() {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        out.push(BackendMessage::ParameterDescription {
            parameter_types: prepared.parameter_types.clone(),
        });

        match &prepared.operation {
            PlannedOperation::ParsedQuery(plan) if plan.returns_data() => {
                out.push(BackendMessage::RowDescription {
                    fields: describe_fields_for_plan(plan, &[])?,
                });
            }
            _ => out.push(BackendMessage::NoData),
        }

        Ok(())
    }

    fn exec_describe_portal_message(
        &mut self,
        portal_name: &str,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();
        let key = portal_key(portal_name);
        let portal = self.portals.get(&key).ok_or_else(|| SessionError {
            message: format!("portal \"{}\" does not exist", portal_name),
        })?;

        if self.is_aborted_transaction_block() && portal.operation.returns_data() {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        match &portal.operation {
            PlannedOperation::ParsedQuery(plan) if plan.returns_data() => {
                out.push(BackendMessage::RowDescription {
                    fields: describe_fields_for_plan(plan, &portal.result_format_codes)?,
                });
            }
            _ => out.push(BackendMessage::NoData),
        }

        Ok(())
    }

    fn exec_close_statement(&mut self, statement_name: &str, out: &mut Vec<BackendMessage>) {
        if statement_name.is_empty() {
            self.drop_unnamed_stmt();
        } else {
            self.prepared_statements.remove(statement_name);
        }
        out.push(BackendMessage::CloseComplete);
    }

    fn exec_close_portal(&mut self, portal_name: &str, out: &mut Vec<BackendMessage>) {
        let key = portal_key(portal_name);
        self.portals.remove(&key);
        out.push(BackendMessage::CloseComplete);
    }

    fn emit_outcome(
        out: &mut Vec<BackendMessage>,
        outcome: ExecutionOutcome,
        max_rows: i64,
        portal_state: Option<(&mut Portal, usize, bool)>,
        row_description: Option<&[RowDescriptionField]>,
    ) -> Result<(), SessionError> {
        match outcome {
            ExecutionOutcome::Command(completion) => {
                out.push(BackendMessage::CommandComplete {
                    tag: completion.tag,
                    rows: completion.rows,
                });
                Ok(())
            }
            ExecutionOutcome::Query(result) => {
                if let Some((portal, prev_cursor, prev_desc_sent)) = portal_state {
                    if !prev_desc_sent {
                        let fields = row_description
                            .map(|fields| fields.to_vec())
                            .unwrap_or_else(|| {
                                infer_row_description_fields(&result.columns, &result.rows)
                            });
                        out.push(BackendMessage::RowDescription { fields });
                        portal.row_description_sent = true;
                    }

                    let limit = if max_rows <= 0 {
                        usize::MAX
                    } else {
                        max_rows as usize
                    };
                    let start = prev_cursor.min(result.rows.len());
                    let end = if limit == usize::MAX {
                        result.rows.len()
                    } else {
                        start.saturating_add(limit).min(result.rows.len())
                    };

                    for row in &result.rows[start..end] {
                        out.push(BackendMessage::DataRow {
                            values: row.iter().map(ScalarValue::render).collect(),
                        });
                    }

                    portal.cursor = end;
                    if end < result.rows.len() && max_rows > 0 {
                        out.push(BackendMessage::PortalSuspended);
                    } else {
                        out.push(BackendMessage::CommandComplete {
                            tag: result.command_tag,
                            rows: result.rows_affected,
                        });
                    }
                    return Ok(());
                }

                let fields = row_description
                    .map(|fields| fields.to_vec())
                    .unwrap_or_else(|| infer_row_description_fields(&result.columns, &result.rows));
                out.push(BackendMessage::RowDescription { fields });
                for row in &result.rows {
                    out.push(BackendMessage::DataRow {
                        values: row.iter().map(ScalarValue::render).collect(),
                    });
                }
                out.push(BackendMessage::CommandComplete {
                    tag: result.command_tag,
                    rows: result.rows_affected,
                });
                Ok(())
            }
            ExecutionOutcome::CopyInStart { column_formats } => {
                out.push(BackendMessage::CopyInResponse {
                    overall_format: 1,
                    column_formats,
                });
                Ok(())
            }
            ExecutionOutcome::CopyOut {
                column_formats,
                data,
                rows,
            } => {
                out.push(BackendMessage::CopyOutResponse {
                    overall_format: 1,
                    column_formats,
                });
                if !data.is_empty() {
                    out.push(BackendMessage::CopyData { data });
                }
                out.push(BackendMessage::CopyDone);
                out.push(BackendMessage::CommandComplete {
                    tag: "COPY".to_string(),
                    rows,
                });
                Ok(())
            }
        }
    }

    fn plan_query_string(&mut self, query: &str) -> Result<PlannedOperation, SessionError> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(PlannedOperation::Empty);
        }

        if let Some(txn) = parse_transaction_command(trimmed)? {
            return Ok(PlannedOperation::Transaction(txn));
        }

        if let Some(security_cmd) = parse_security_command(trimmed)? {
            return Ok(PlannedOperation::Security(security_cmd));
        }

        if let Some(copy_cmd) = parse_copy_command(trimmed)? {
            return Ok(PlannedOperation::Copy(copy_cmd));
        }

        if starts_like_engine_statement(trimmed) {
            let statement = parse_statement(trimmed).map_err(|err| SessionError {
                message: format!("parse error: {}", err),
            })?;
            if self.tx_state.in_explicit_block()
                && let Some(message) = top_level_only_statement_error(&statement)
            {
                return Err(SessionError {
                    message: message.to_string(),
                });
            }

            if !self.tx_state.in_explicit_block() {
                let planned = plan_statement(statement).map_err(SessionError::from)?;
                return Ok(PlannedOperation::ParsedQuery(planned));
            }

            let baseline = snapshot_state();
            let working = self
                .tx_state
                .working_snapshot()
                .cloned()
                .or_else(|| self.tx_state.base_snapshot().cloned())
                .ok_or_else(|| SessionError {
                    message: "transaction state missing working snapshot".to_string(),
                })?;

            restore_state(working);
            let planned = plan_statement(statement);
            restore_state(baseline);
            let planned = planned.map_err(SessionError::from)?;
            return Ok(PlannedOperation::ParsedQuery(planned));
        }

        let tag = first_keyword_uppercase(trimmed).unwrap_or_else(|| "UTILITY".to_string());
        Ok(PlannedOperation::Utility(tag))
    }

    fn execute_operation(
        &mut self,
        operation: &PlannedOperation,
        params: &[Option<String>],
    ) -> Result<ExecutionOutcome, SessionError> {
        let role = self.current_role.clone();
        security::with_current_role(&role, || match operation {
            PlannedOperation::ParsedQuery(plan) => {
                let result = match self.tx_state.visibility_mode() {
                    VisibilityMode::Global => {
                        execute_planned_query(plan, params).map_err(SessionError::from)?
                    }
                    VisibilityMode::TransactionLocal => {
                        self.execute_query_in_transaction_scope(plan, params)?
                    }
                };
                if plan.returns_data() {
                    Ok(ExecutionOutcome::Query(result))
                } else {
                    Ok(ExecutionOutcome::Command(Completion {
                        tag: result.command_tag,
                        rows: result.rows_affected,
                    }))
                }
            }
            PlannedOperation::Transaction(command) => {
                self.apply_transaction_command(command.clone())?;
                Ok(ExecutionOutcome::Command(Completion {
                    tag: operation.command_tag(),
                    rows: 0,
                }))
            }
            PlannedOperation::Security(command) => {
                match self.tx_state.visibility_mode() {
                    VisibilityMode::Global => self.execute_security_command(command)?,
                    VisibilityMode::TransactionLocal => {
                        self.execute_security_in_transaction_scope(command)?
                    }
                }
                Ok(ExecutionOutcome::Command(Completion {
                    tag: operation.command_tag(),
                    rows: 0,
                }))
            }
            PlannedOperation::Copy(command) => match command.direction {
                CopyDirection::FromStdin => {
                    if !command.binary {
                        return Err(SessionError {
                            message: "COPY FROM STDIN requires BINARY in this implementation"
                                .to_string(),
                        });
                    }
                    let column_type_oids = self.copy_column_type_oids(&command.table_name)?;
                    self.copy_in_state = Some(CopyInState {
                        table_name: command.table_name.clone(),
                        binary: command.binary,
                        column_type_oids: column_type_oids.clone(),
                        payload: Vec::new(),
                    });
                    let column_formats = vec![1i16; column_type_oids.len()];
                    Ok(ExecutionOutcome::CopyInStart { column_formats })
                }
                CopyDirection::ToStdout => {
                    if !command.binary {
                        return Err(SessionError {
                            message: "COPY TO STDOUT requires BINARY in this implementation"
                                .to_string(),
                        });
                    }
                    let snapshot = match self.tx_state.visibility_mode() {
                        VisibilityMode::Global => self.copy_snapshot(&command.table_name)?,
                        VisibilityMode::TransactionLocal => {
                            self.copy_snapshot_in_transaction_scope(&command.table_name)?
                        }
                    };
                    let column_formats = vec![1i16; snapshot.columns.len()];
                    let data = encode_copy_binary_stream(&snapshot.columns, &snapshot.rows)?;
                    Ok(ExecutionOutcome::CopyOut {
                        column_formats,
                        data,
                        rows: snapshot.rows.len() as u64,
                    })
                }
            },
            PlannedOperation::Utility(tag) => Ok(ExecutionOutcome::Command(Completion {
                tag: tag.clone(),
                rows: 0,
            })),
            PlannedOperation::Empty => Ok(ExecutionOutcome::Command(Completion {
                tag: "EMPTY".to_string(),
                rows: 0,
            })),
        })
    }

    fn fetch_prepared_statement(
        &self,
        statement_name: &str,
    ) -> Result<&PreparedStatement, SessionError> {
        let key = if statement_name.is_empty() {
            UNNAMED
        } else {
            statement_name
        };
        self.prepared_statements
            .get(key)
            .ok_or_else(|| SessionError {
                message: if statement_name.is_empty() {
                    "unnamed prepared statement does not exist".to_string()
                } else {
                    format!("prepared statement \"{}\" does not exist", statement_name)
                },
            })
    }

    fn execute_query_in_transaction_scope(
        &mut self,
        plan: &PlannedQuery,
        params: &[Option<String>],
    ) -> Result<QueryResult, SessionError> {
        let baseline = snapshot_state();
        let working = self
            .tx_state
            .working_snapshot()
            .cloned()
            .or_else(|| self.tx_state.base_snapshot().cloned())
            .ok_or_else(|| SessionError {
                message: "transaction state missing working snapshot".to_string(),
            })?;

        restore_state(working);
        let executed = execute_planned_query(plan, params);
        let next_working = executed.as_ref().ok().map(|_| snapshot_state());
        restore_state(baseline);

        match executed {
            Ok(result) => {
                if let Some(snapshot) = next_working {
                    self.tx_state.set_working_snapshot(snapshot);
                }
                Ok(result)
            }
            Err(err) => Err(SessionError::from(err)),
        }
    }

    fn execute_security_in_transaction_scope(
        &mut self,
        command: &SecurityCommand,
    ) -> Result<(), SessionError> {
        let baseline = snapshot_state();
        let working = self
            .tx_state
            .working_snapshot()
            .cloned()
            .or_else(|| self.tx_state.base_snapshot().cloned())
            .ok_or_else(|| SessionError {
                message: "transaction state missing working snapshot".to_string(),
            })?;

        restore_state(working);
        let executed = self.execute_security_command(command);
        let next_working = executed.as_ref().ok().map(|_| snapshot_state());
        restore_state(baseline);

        if let Some(snapshot) = next_working {
            self.tx_state.set_working_snapshot(snapshot);
        }
        executed
    }

    fn execute_security_command(&mut self, command: &SecurityCommand) -> Result<(), SessionError> {
        match command {
            SecurityCommand::CreateRole { role_name, options } => {
                security::create_role(&self.current_role, role_name, options.clone())
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::DropRole {
                role_name,
                if_exists,
            } => {
                if security::normalize_identifier(role_name) == self.session_user {
                    return Err(SessionError {
                        message: "current session user cannot be dropped".to_string(),
                    });
                }
                match security::drop_role(&self.current_role, role_name) {
                    Ok(()) => Ok(()),
                    Err(message) if *if_exists && message.contains("does not exist") => Ok(()),
                    Err(message) => Err(SessionError { message }),
                }
            }
            SecurityCommand::GrantRole { role_name, member } => {
                security::grant_role(&self.current_role, role_name, member)
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::RevokeRole { role_name, member } => {
                security::revoke_role(&self.current_role, role_name, member)
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::SetRole { role_name } => {
                let normalized = security::normalize_identifier(role_name);
                if !security::role_exists(&normalized) {
                    return Err(SessionError {
                        message: format!("role \"{}\" does not exist", role_name),
                    });
                }
                if !security::can_set_role(&self.session_user, &normalized) {
                    return Err(SessionError {
                        message: format!("permission denied to set role \"{}\"", role_name),
                    });
                }
                self.current_role = normalized;
                Ok(())
            }
            SecurityCommand::ResetRole => {
                self.current_role = self.session_user.clone();
                Ok(())
            }
            SecurityCommand::GrantTablePrivileges {
                table_name,
                roles,
                privileges,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                security::grant_table_privileges(
                    &self.current_role,
                    relation_oid,
                    &relation_name,
                    roles,
                    privileges,
                )
                .map_err(|message| SessionError { message })
            }
            SecurityCommand::RevokeTablePrivileges {
                table_name,
                roles,
                privileges,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                security::revoke_table_privileges(
                    &self.current_role,
                    relation_oid,
                    &relation_name,
                    roles,
                    privileges,
                )
                .map_err(|message| SessionError { message })
            }
            SecurityCommand::SetRowLevelSecurity {
                table_name,
                enabled,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                if *enabled {
                    security::enable_rls(&self.current_role, relation_oid, &relation_name)
                } else {
                    security::disable_rls(&self.current_role, relation_oid, &relation_name)
                }
                .map_err(|message| SessionError { message })
            }
            SecurityCommand::CreatePolicy {
                policy_name,
                table_name,
                command,
                roles,
                using_expr,
                check_expr,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                let policy = RlsPolicy {
                    name: policy_name.clone(),
                    relation_oid,
                    command: *command,
                    roles: roles.clone(),
                    using_expr: using_expr.clone(),
                    check_expr: check_expr.clone(),
                };
                security::create_policy(&self.current_role, policy, &relation_name)
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::DropPolicy {
                policy_name,
                table_name,
                if_exists,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                security::drop_policy(
                    &self.current_role,
                    relation_oid,
                    &relation_name,
                    policy_name,
                    *if_exists,
                )
                .map_err(|message| SessionError { message })
            }
        }
    }

    fn copy_column_type_oids(&self, table_name: &[String]) -> Result<Vec<PgType>, SessionError> {
        security::with_current_role(&self.current_role, || copy_table_column_oids(table_name))
            .map_err(SessionError::from)
    }

    fn copy_snapshot(
        &self,
        table_name: &[String],
    ) -> Result<crate::tcop::engine::CopyBinarySnapshot, SessionError> {
        security::with_current_role(&self.current_role, || {
            copy_table_binary_snapshot(table_name)
        })
        .map_err(SessionError::from)
    }

    fn copy_snapshot_in_transaction_scope(
        &mut self,
        table_name: &[String],
    ) -> Result<crate::tcop::engine::CopyBinarySnapshot, SessionError> {
        let baseline = snapshot_state();
        let working = self
            .tx_state
            .working_snapshot()
            .cloned()
            .or_else(|| self.tx_state.base_snapshot().cloned())
            .ok_or_else(|| SessionError {
                message: "transaction state missing working snapshot".to_string(),
            })?;
        restore_state(working);
        let result = security::with_current_role(&self.current_role, || {
            copy_table_binary_snapshot(table_name)
        });
        restore_state(baseline);
        result.map_err(SessionError::from)
    }

    fn exec_copy_data(&mut self, data: Vec<u8>) -> Result<(), SessionError> {
        let Some(state) = self.copy_in_state.as_mut() else {
            return Err(SessionError {
                message: "COPY data was sent without COPY IN state".to_string(),
            });
        };
        if !state.binary {
            return Err(SessionError {
                message: "COPY text protocol is not implemented".to_string(),
            });
        }
        state.payload.extend_from_slice(&data);
        Ok(())
    }

    fn exec_copy_done(&mut self, out: &mut Vec<BackendMessage>) -> Result<(), SessionError> {
        let state = self.copy_in_state.take().ok_or_else(|| SessionError {
            message: "COPY done was sent without COPY IN state".to_string(),
        })?;
        let rows = parse_copy_binary_stream(&state.payload, &state.column_type_oids)?;

        let inserted = match self.tx_state.visibility_mode() {
            VisibilityMode::Global => security::with_current_role(&self.current_role, || {
                copy_insert_rows(&state.table_name, rows)
            })
            .map_err(SessionError::from)?,
            VisibilityMode::TransactionLocal => {
                self.copy_insert_rows_in_transaction_scope(&state.table_name, rows)?
            }
        };

        out.push(BackendMessage::CommandComplete {
            tag: "COPY".to_string(),
            rows: inserted,
        });
        self.finish_xact_command();
        Ok(())
    }

    fn copy_insert_rows_in_transaction_scope(
        &mut self,
        table_name: &[String],
        rows: Vec<Vec<ScalarValue>>,
    ) -> Result<u64, SessionError> {
        let baseline = snapshot_state();
        let working = self
            .tx_state
            .working_snapshot()
            .cloned()
            .or_else(|| self.tx_state.base_snapshot().cloned())
            .ok_or_else(|| SessionError {
                message: "transaction state missing working snapshot".to_string(),
            })?;
        restore_state(working);
        let executed =
            security::with_current_role(&self.current_role, || copy_insert_rows(table_name, rows));
        let next_working = executed.as_ref().ok().map(|_| snapshot_state());
        restore_state(baseline);
        if let Some(snapshot) = next_working {
            self.tx_state.set_working_snapshot(snapshot);
        }
        executed.map_err(SessionError::from)
    }

    fn exec_copy_fail(&mut self, message: String) -> Result<(), SessionError> {
        if self.copy_in_state.is_none() {
            return Err(SessionError {
                message: "COPY fail was sent without COPY IN state".to_string(),
            });
        }
        self.copy_in_state = None;
        self.finish_xact_command();
        Err(SessionError {
            message: format!("COPY failed: {}", message),
        })
    }

    fn apply_transaction_command(
        &mut self,
        command: TransactionCommand,
    ) -> Result<(), SessionError> {
        match command {
            TransactionCommand::Begin => {
                self.tx_state.begin();
                Ok(())
            }
            TransactionCommand::Commit => {
                if let Some(snapshot) = self.tx_state.commit() {
                    restore_state(snapshot);
                }
                Ok(())
            }
            TransactionCommand::Rollback => {
                self.tx_state.rollback();
                Ok(())
            }
            TransactionCommand::Savepoint(name) => {
                self.tx_state
                    .savepoint(name)
                    .map_err(|message| SessionError { message })?;
                Ok(())
            }
            TransactionCommand::ReleaseSavepoint(name) => {
                self.tx_state
                    .release_savepoint(name)
                    .map_err(|message| SessionError { message })?;
                Ok(())
            }
            TransactionCommand::RollbackToSavepoint(name) => {
                self.tx_state
                    .rollback_to_savepoint(name)
                    .map_err(|message| SessionError { message })?;
                Ok(())
            }
        }
    }

    fn drop_unnamed_stmt(&mut self) {
        self.prepared_statements.remove(UNNAMED);
    }

    fn start_xact_command(&mut self) {
        self.xact_started = true;
    }

    fn finish_xact_command(&mut self) {
        self.xact_started = false;
    }

    fn is_aborted_transaction_block(&self) -> bool {
        self.tx_state.is_aborted()
    }

    fn ready_status(&self) -> ReadyForQueryStatus {
        if self.is_aborted_transaction_block() {
            ReadyForQueryStatus::FailedTransaction
        } else if self.tx_state.in_explicit_block() || self.xact_started {
            ReadyForQueryStatus::InTransaction
        } else {
            ReadyForQueryStatus::Idle
        }
    }

    fn handle_error_recovery(&mut self) {
        if self.doing_extended_query_message {
            self.ignore_till_sync = true;
        } else {
            self.send_ready_for_query = true;
        }

        self.xact_started = false;
        self.copy_in_state = None;
        self.tx_state.mark_failed();
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ExecutionOutcome {
    Query(QueryResult),
    Command(Completion),
    CopyInStart {
        column_formats: Vec<i16>,
    },
    CopyOut {
        column_formats: Vec<i16>,
        data: Vec<u8>,
        rows: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Completion {
    tag: String,
    rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlFlow {
    Continue,
    Break,
}

fn is_extended_query_message(message: &FrontendMessage) -> bool {
    matches!(
        message,
        FrontendMessage::Parse { .. }
            | FrontendMessage::Bind { .. }
            | FrontendMessage::Execute { .. }
            | FrontendMessage::DescribeStatement { .. }
            | FrontendMessage::DescribePortal { .. }
            | FrontendMessage::CloseStatement { .. }
            | FrontendMessage::ClosePortal { .. }
    )
}

fn starts_like_engine_statement(query: &str) -> bool {
    let first = first_keyword_uppercase(query).unwrap_or_default();
    first == "SELECT"
        || first == "WITH"
        || first == "CREATE"
        || first == "INSERT"
        || first == "UPDATE"
        || first == "DELETE"
        || first == "MERGE"
        || first == "DROP"
        || first == "TRUNCATE"
        || first == "ALTER"
        || first == "REFRESH"
        || query.starts_with('(')
}

fn parse_transaction_command(query: &str) -> Result<Option<TransactionCommand>, SessionError> {
    let tokens = query
        .split_whitespace()
        .map(|token| token.trim_matches(';'))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let Some(first) = tokens.first() else {
        return Ok(None);
    };
    let first_upper = first.to_ascii_uppercase();

    let parse_name = |token: Option<&&str>, command: &str| -> Result<String, SessionError> {
        token
            .map(|value| value.to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| SessionError {
                message: format!("{command} requires a savepoint name"),
            })
    };

    match first_upper.as_str() {
        "BEGIN" => Ok(Some(TransactionCommand::Begin)),
        "START" => Ok(Some(TransactionCommand::Begin)),
        "COMMIT" | "END" => Ok(Some(TransactionCommand::Commit)),
        "ROLLBACK" => {
            if tokens.len() >= 2 && tokens[1].eq_ignore_ascii_case("TO") {
                let name = if tokens.len() >= 3 && tokens[2].eq_ignore_ascii_case("SAVEPOINT") {
                    parse_name(tokens.get(3), "ROLLBACK TO SAVEPOINT")?
                } else {
                    parse_name(tokens.get(2), "ROLLBACK TO SAVEPOINT")?
                };
                return Ok(Some(TransactionCommand::RollbackToSavepoint(name)));
            }
            Ok(Some(TransactionCommand::Rollback))
        }
        "SAVEPOINT" => {
            let name = parse_name(tokens.get(1), "SAVEPOINT")?;
            Ok(Some(TransactionCommand::Savepoint(name)))
        }
        "RELEASE" => {
            let name = if tokens.len() >= 2 && tokens[1].eq_ignore_ascii_case("SAVEPOINT") {
                parse_name(tokens.get(2), "RELEASE SAVEPOINT")?
            } else {
                parse_name(tokens.get(1), "RELEASE SAVEPOINT")?
            };
            Ok(Some(TransactionCommand::ReleaseSavepoint(name)))
        }
        _ => Ok(None),
    }
}

fn parse_copy_command(query: &str) -> Result<Option<CopyCommand>, SessionError> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    if !starts_with_keyword(trimmed, "COPY ") {
        return Ok(None);
    }

    let upper = trimmed.to_ascii_uppercase();
    if let Some(to_pos) = upper.find(" TO ") {
        let table_name = security::parse_qualified_name(trimmed["COPY ".len()..to_pos].trim());
        let target = trimmed[to_pos + " TO ".len()..].trim();
        if !target.eq_ignore_ascii_case("STDOUT BINARY")
            && !target.eq_ignore_ascii_case("STDOUT (FORMAT BINARY)")
        {
            return Err(SessionError {
                message: "only COPY ... TO STDOUT BINARY is supported".to_string(),
            });
        }
        return Ok(Some(CopyCommand {
            table_name,
            direction: CopyDirection::ToStdout,
            binary: true,
        }));
    }

    if let Some(from_pos) = upper.find(" FROM ") {
        let table_name = security::parse_qualified_name(trimmed["COPY ".len()..from_pos].trim());
        let target = trimmed[from_pos + " FROM ".len()..].trim();
        if !target.eq_ignore_ascii_case("STDIN BINARY")
            && !target.eq_ignore_ascii_case("STDIN (FORMAT BINARY)")
        {
            return Err(SessionError {
                message: "only COPY ... FROM STDIN BINARY is supported".to_string(),
            });
        }
        return Ok(Some(CopyCommand {
            table_name,
            direction: CopyDirection::FromStdin,
            binary: true,
        }));
    }

    Err(SessionError {
        message: "unsupported COPY command (expected COPY <table> TO/FROM STDOUT/STDIN BINARY)"
            .to_string(),
    })
}

fn parse_security_command(query: &str) -> Result<Option<SecurityCommand>, SessionError> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if starts_with_keyword(trimmed, "CREATE ROLE ") {
        return parse_create_role_command(trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "DROP ROLE ") {
        return parse_drop_role_command(trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "GRANT ") {
        return parse_grant_command(trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "REVOKE ") {
        return parse_revoke_command(trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "SET ROLE ") {
        return parse_set_role_command(trimmed).map(Some);
    }
    if trimmed.eq_ignore_ascii_case("RESET ROLE") {
        return Ok(Some(SecurityCommand::ResetRole));
    }
    if starts_with_keyword(trimmed, "ALTER TABLE ")
        && contains_keyword(trimmed, "ROW LEVEL SECURITY")
    {
        return parse_alter_table_rls_command(trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "CREATE POLICY ") {
        return parse_create_policy_command(trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "DROP POLICY ") {
        return parse_drop_policy_command(trimmed).map(Some);
    }

    Ok(None)
}

fn parse_create_role_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let tokens = query.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 3 {
        return Err(SessionError {
            message: "CREATE ROLE requires a role name".to_string(),
        });
    }
    let role_name = security::normalize_identifier(tokens[2]);
    let mut options = CreateRoleOptions::default();

    let mut idx = 3usize;
    while idx < tokens.len() {
        let token = tokens[idx];
        if token.eq_ignore_ascii_case("SUPERUSER") {
            options.superuser = true;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case("NOSUPERUSER") {
            options.superuser = false;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case("LOGIN") {
            options.login = true;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case("NOLOGIN") {
            options.login = false;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case("PASSWORD") {
            idx += 1;
            if idx >= tokens.len() {
                return Err(SessionError {
                    message: "CREATE ROLE PASSWORD requires a value".to_string(),
                });
            }
            options.password = Some(parse_sql_string_literal(tokens[idx])?);
            idx += 1;
            continue;
        }
        return Err(SessionError {
            message: format!("unsupported CREATE ROLE option {}", token),
        });
    }

    Ok(SecurityCommand::CreateRole { role_name, options })
}

fn parse_drop_role_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let tokens = query.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 3 {
        return Err(SessionError {
            message: "DROP ROLE requires a role name".to_string(),
        });
    }
    let mut if_exists = false;
    let role_index = if tokens.len() >= 5
        && tokens[2].eq_ignore_ascii_case("IF")
        && tokens[3].eq_ignore_ascii_case("EXISTS")
    {
        if_exists = true;
        4
    } else {
        2
    };
    let role_name = tokens.get(role_index).ok_or_else(|| SessionError {
        message: "DROP ROLE requires a role name".to_string(),
    })?;

    Ok(SecurityCommand::DropRole {
        role_name: security::normalize_identifier(role_name),
        if_exists,
    })
}

fn parse_grant_command(query: &str) -> Result<SecurityCommand, SessionError> {
    if contains_keyword(query, " ON ") {
        return parse_grant_table_privileges_command(query);
    }

    let upper = query.to_ascii_uppercase();
    let to_pos = upper.find(" TO ").ok_or_else(|| SessionError {
        message: "GRANT role requires TO clause".to_string(),
    })?;
    let role_name = query["GRANT ".len()..to_pos].trim();
    let member = query[to_pos + " TO ".len()..].trim();
    if role_name.is_empty() || member.is_empty() {
        return Err(SessionError {
            message: "GRANT role requires role and member names".to_string(),
        });
    }

    Ok(SecurityCommand::GrantRole {
        role_name: security::normalize_identifier(role_name),
        member: security::normalize_identifier(member),
    })
}

fn parse_revoke_command(query: &str) -> Result<SecurityCommand, SessionError> {
    if contains_keyword(query, " ON ") {
        return parse_revoke_table_privileges_command(query);
    }

    let upper = query.to_ascii_uppercase();
    let from_pos = upper.find(" FROM ").ok_or_else(|| SessionError {
        message: "REVOKE role requires FROM clause".to_string(),
    })?;
    let role_name = query["REVOKE ".len()..from_pos].trim();
    let member = query[from_pos + " FROM ".len()..].trim();
    if role_name.is_empty() || member.is_empty() {
        return Err(SessionError {
            message: "REVOKE role requires role and member names".to_string(),
        });
    }

    Ok(SecurityCommand::RevokeRole {
        role_name: security::normalize_identifier(role_name),
        member: security::normalize_identifier(member),
    })
}

fn parse_grant_table_privileges_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let upper = query.to_ascii_uppercase();
    let on_pos = upper.find(" ON ").ok_or_else(|| SessionError {
        message: "GRANT requires ON TABLE clause".to_string(),
    })?;
    let to_pos = upper.rfind(" TO ").ok_or_else(|| SessionError {
        message: "GRANT requires TO clause".to_string(),
    })?;
    if to_pos <= on_pos {
        return Err(SessionError {
            message: "GRANT clause order is invalid".to_string(),
        });
    }

    let privilege_text = query["GRANT ".len()..on_pos].trim();
    let mut table_text = query[on_pos + " ON ".len()..to_pos].trim();
    if starts_with_keyword(table_text, "TABLE ") {
        table_text = &table_text["TABLE ".len()..];
    }
    let role_text = query[to_pos + " TO ".len()..].trim();

    let privileges = parse_privilege_list(privilege_text)?;
    let roles = parse_identifier_list(role_text);
    if roles.is_empty() {
        return Err(SessionError {
            message: "GRANT requires at least one target role".to_string(),
        });
    }

    Ok(SecurityCommand::GrantTablePrivileges {
        table_name: security::parse_qualified_name(table_text),
        roles,
        privileges,
    })
}

fn parse_revoke_table_privileges_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let upper = query.to_ascii_uppercase();
    let on_pos = upper.find(" ON ").ok_or_else(|| SessionError {
        message: "REVOKE requires ON TABLE clause".to_string(),
    })?;
    let from_pos = upper.rfind(" FROM ").ok_or_else(|| SessionError {
        message: "REVOKE requires FROM clause".to_string(),
    })?;
    if from_pos <= on_pos {
        return Err(SessionError {
            message: "REVOKE clause order is invalid".to_string(),
        });
    }

    let privilege_text = query["REVOKE ".len()..on_pos].trim();
    let mut table_text = query[on_pos + " ON ".len()..from_pos].trim();
    if starts_with_keyword(table_text, "TABLE ") {
        table_text = &table_text["TABLE ".len()..];
    }
    let role_text = query[from_pos + " FROM ".len()..].trim();

    let privileges = parse_privilege_list(privilege_text)?;
    let roles = parse_identifier_list(role_text);
    if roles.is_empty() {
        return Err(SessionError {
            message: "REVOKE requires at least one target role".to_string(),
        });
    }

    Ok(SecurityCommand::RevokeTablePrivileges {
        table_name: security::parse_qualified_name(table_text),
        roles,
        privileges,
    })
}

fn parse_privilege_list(input: &str) -> Result<Vec<TablePrivilege>, SessionError> {
    let mut privileges = Vec::new();
    for item in input.split(',') {
        let raw = item.trim();
        if raw.is_empty() {
            continue;
        }
        let upper = raw.to_ascii_uppercase();
        if upper == "ALL" || upper == "ALL PRIVILEGES" {
            privileges.extend_from_slice(TablePrivilege::all());
            continue;
        }
        let Some(privilege) = TablePrivilege::from_keyword(upper.trim()) else {
            return Err(SessionError {
                message: format!("unsupported privilege {}", raw),
            });
        };
        privileges.push(privilege);
    }
    privileges.sort_by_key(|privilege| *privilege as u8);
    privileges.dedup();
    if privileges.is_empty() {
        return Err(SessionError {
            message: "no privileges specified".to_string(),
        });
    }
    Ok(privileges)
}

fn parse_set_role_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let tokens = query.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 3 {
        return Err(SessionError {
            message: "SET ROLE requires a role name".to_string(),
        });
    }
    Ok(SecurityCommand::SetRole {
        role_name: security::normalize_identifier(tokens[2]),
    })
}

fn parse_alter_table_rls_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let tokens = query.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 7 {
        return Err(SessionError {
            message: "ALTER TABLE ... ROW LEVEL SECURITY command is incomplete".to_string(),
        });
    }
    if !tokens[0].eq_ignore_ascii_case("ALTER")
        || !tokens[1].eq_ignore_ascii_case("TABLE")
        || !tokens[4].eq_ignore_ascii_case("ROW")
        || !tokens[5].eq_ignore_ascii_case("LEVEL")
        || !tokens[6].eq_ignore_ascii_case("SECURITY")
    {
        return Err(SessionError {
            message: "unsupported ALTER TABLE command".to_string(),
        });
    }
    let enabled = if tokens[3].eq_ignore_ascii_case("ENABLE") {
        true
    } else if tokens[3].eq_ignore_ascii_case("DISABLE") {
        false
    } else {
        return Err(SessionError {
            message: "ALTER TABLE expects ENABLE or DISABLE for ROW LEVEL SECURITY".to_string(),
        });
    };

    Ok(SecurityCommand::SetRowLevelSecurity {
        table_name: security::parse_qualified_name(tokens[2]),
        enabled,
    })
}

fn parse_create_policy_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let rest = strip_prefix_keyword(query, "CREATE POLICY ").ok_or_else(|| SessionError {
        message: "CREATE POLICY syntax error".to_string(),
    })?;
    let (policy_name, after_name) = split_once_whitespace(rest).ok_or_else(|| SessionError {
        message: "CREATE POLICY requires a policy name".to_string(),
    })?;
    let after_name = after_name.trim_start();
    let after_on = strip_prefix_keyword(after_name, "ON ").ok_or_else(|| SessionError {
        message: "CREATE POLICY requires ON <table>".to_string(),
    })?;
    let (table_name_text, mut tail) =
        split_until_keywords(after_on, &[" FOR ", " TO ", " USING ", " WITH CHECK "]);
    if table_name_text.is_empty() {
        return Err(SessionError {
            message: "CREATE POLICY requires a table name".to_string(),
        });
    }

    let mut command = RlsCommand::All;
    let mut roles = vec!["public".to_string()];
    let mut using_expr = None;
    let mut check_expr = None;

    while !tail.trim().is_empty() {
        if let Some(after_for) = strip_prefix_keyword(tail, "FOR ") {
            let (cmd, next) = split_once_whitespace(after_for).unwrap_or((after_for, ""));
            let cmd_upper = cmd.trim().to_ascii_uppercase();
            command = RlsCommand::from_keyword(&cmd_upper).ok_or_else(|| SessionError {
                message: format!("unsupported policy command {}", cmd),
            })?;
            tail = next;
            continue;
        }
        if let Some(after_to) = strip_prefix_keyword(tail, "TO ") {
            let (role_text, next) = split_until_keywords(after_to, &[" USING ", " WITH CHECK "]);
            let parsed_roles = parse_identifier_list(role_text);
            if parsed_roles.is_empty() {
                return Err(SessionError {
                    message: "CREATE POLICY TO requires at least one role".to_string(),
                });
            }
            roles = parsed_roles;
            tail = next;
            continue;
        }
        if let Some(after_using) = strip_prefix_keyword(tail, "USING ") {
            let (expr_text, consumed) = extract_parenthesized_expression(after_using)?;
            using_expr = Some(parse_policy_predicate(expr_text)?);
            tail = &after_using[consumed..];
            continue;
        }
        if let Some(after_check) = strip_prefix_keyword(tail, "WITH CHECK ") {
            let (expr_text, consumed) = extract_parenthesized_expression(after_check)?;
            check_expr = Some(parse_policy_predicate(expr_text)?);
            tail = &after_check[consumed..];
            continue;
        }
        return Err(SessionError {
            message: "unsupported CREATE POLICY clause".to_string(),
        });
    }

    Ok(SecurityCommand::CreatePolicy {
        policy_name: security::normalize_identifier(policy_name),
        table_name: security::parse_qualified_name(table_name_text),
        command,
        roles,
        using_expr,
        check_expr,
    })
}

fn parse_drop_policy_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let rest = strip_prefix_keyword(query, "DROP POLICY ").ok_or_else(|| SessionError {
        message: "DROP POLICY syntax error".to_string(),
    })?;
    let mut if_exists = false;
    let rest = if let Some(after_if_exists) = strip_prefix_keyword(rest, "IF EXISTS ") {
        if_exists = true;
        after_if_exists
    } else {
        rest
    };
    let upper = rest.to_ascii_uppercase();
    let on_pos = upper.find(" ON ").ok_or_else(|| SessionError {
        message: "DROP POLICY requires ON <table>".to_string(),
    })?;
    let policy_name = rest[..on_pos].trim();
    let table_name = rest[on_pos + " ON ".len()..].trim();
    if policy_name.is_empty() || table_name.is_empty() {
        return Err(SessionError {
            message: "DROP POLICY requires policy and table names".to_string(),
        });
    }
    Ok(SecurityCommand::DropPolicy {
        policy_name: security::normalize_identifier(policy_name),
        table_name: security::parse_qualified_name(table_name),
        if_exists,
    })
}

fn parse_policy_predicate(raw: &str) -> Result<Expr, SessionError> {
    let sql = format!("SELECT 1 WHERE {}", raw);
    let statement = parse_statement(&sql).map_err(|err| SessionError {
        message: format!("parse error: {}", err),
    })?;
    let Statement::Query(query) = statement else {
        return Err(SessionError {
            message: "policy expression must be a valid SQL predicate".to_string(),
        });
    };
    let QueryExpr::Select(select) = query.body else {
        return Err(SessionError {
            message: "policy expression must be a SELECT predicate".to_string(),
        });
    };
    select.where_clause.ok_or_else(|| SessionError {
        message: "policy expression is empty".to_string(),
    })
}

fn parse_identifier_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(security::normalize_identifier)
        .filter(|name| !name.is_empty())
        .collect()
}

fn parse_sql_string_literal(raw: &str) -> Result<String, SessionError> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('\'') || !trimmed.ends_with('\'') || trimmed.len() < 2 {
        return Err(SessionError {
            message: format!("expected SQL string literal, got {}", raw),
        });
    }
    Ok(trimmed[1..trimmed.len() - 1].replace("''", "'"))
}

fn starts_with_keyword(haystack: &str, keyword: &str) -> bool {
    haystack.len() >= keyword.len() && haystack[..keyword.len()].eq_ignore_ascii_case(keyword)
}

fn strip_prefix_keyword<'a>(haystack: &'a str, keyword: &str) -> Option<&'a str> {
    starts_with_keyword(haystack, keyword).then_some(&haystack[keyword.len()..])
}

fn contains_keyword(haystack: &str, keyword: &str) -> bool {
    haystack
        .to_ascii_uppercase()
        .contains(&keyword.to_ascii_uppercase())
}

fn split_once_whitespace(input: &str) -> Option<(&str, &str)> {
    let trimmed = input.trim_start();
    let split = trimmed.find(char::is_whitespace)?;
    let left = trimmed[..split].trim();
    let right = trimmed[split..].trim_start();
    Some((left, right))
}

fn split_until_keywords<'a>(input: &'a str, keywords: &[&str]) -> (&'a str, &'a str) {
    let trimmed = input.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    let mut next_idx = trimmed.len();
    for keyword in keywords {
        let keyword_upper = keyword.to_ascii_uppercase();
        if let Some(idx) = upper.find(&keyword_upper) {
            if idx < next_idx {
                next_idx = idx;
            }
        }
    }
    if next_idx == trimmed.len() {
        (trimmed.trim(), "")
    } else {
        (trimmed[..next_idx].trim(), trimmed[next_idx..].trim_start())
    }
}

fn extract_parenthesized_expression(input: &str) -> Result<(&str, usize), SessionError> {
    let trimmed = input.trim_start();
    let leading_ws = input.len() - trimmed.len();
    if !trimmed.starts_with('(') {
        return Err(SessionError {
            message: "expected parenthesized expression".to_string(),
        });
    }

    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let chars = trimmed.char_indices().collect::<Vec<_>>();
    let mut end_idx = None;
    let mut i = 0usize;
    while i < chars.len() {
        let (byte_idx, ch) = chars[i];
        if in_single {
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1].1 == '\'' {
                    i += 1;
                } else {
                    in_single = false;
                }
            }
            i += 1;
            continue;
        }
        if in_double {
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1].1 == '"' {
                    i += 1;
                } else {
                    in_double = false;
                }
            }
            i += 1;
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return Err(SessionError {
                        message: "invalid parenthesized expression".to_string(),
                    });
                }
                depth -= 1;
                if depth == 0 {
                    end_idx = Some(byte_idx);
                    break;
                }
            }
            _ => {}
        }
        i += 1;
    }

    let end_idx = end_idx.ok_or_else(|| SessionError {
        message: "unterminated parenthesized expression".to_string(),
    })?;
    let expr = &trimmed[1..end_idx];
    let consumed = leading_ws + end_idx + 1;
    Ok((expr.trim(), consumed))
}

fn first_keyword_uppercase(query: &str) -> Option<String> {
    query
        .split_whitespace()
        .next()
        .map(|kw| kw.trim_matches(';').to_ascii_uppercase())
}

fn split_simple_query_statements(query: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();

    let chars: Vec<char> = query.chars().collect();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;

    while i < chars.len() {
        let ch = chars[i];

        if in_single {
            current.push(ch);
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1] == '\'' {
                    current.push(chars[i + 1]);
                    i += 1;
                } else {
                    in_single = false;
                }
            }
            i += 1;
            continue;
        }

        if in_double {
            current.push(ch);
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] == '"' {
                    current.push(chars[i + 1]);
                    i += 1;
                } else {
                    in_double = false;
                }
            }
            i += 1;
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            ';' => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
        i += 1;
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

fn operation_row_description_fields(
    operation: &PlannedOperation,
    result_formats: &[i16],
) -> Result<Option<Vec<RowDescriptionField>>, SessionError> {
    match operation {
        PlannedOperation::ParsedQuery(plan) if plan.returns_data() => {
            Ok(Some(describe_fields_for_plan(plan, result_formats)?))
        }
        _ => Ok(None),
    }
}

fn describe_fields_for_plan(
    plan: &PlannedQuery,
    result_formats: &[i16],
) -> Result<Vec<RowDescriptionField>, SessionError> {
    if !plan.returns_data() {
        return Ok(Vec::new());
    }
    let columns = plan.columns();
    let type_oids = plan.column_type_oids();
    if columns.len() != type_oids.len() {
        return Err(SessionError {
            message: "planned query metadata is inconsistent".to_string(),
        });
    }
    let format_codes =
        normalize_format_codes(result_formats, columns.len(), "result column format codes")?;
    Ok(columns
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            default_field_description(
                name,
                type_oids[idx],
                type_oid_size(type_oids[idx]),
                format_codes[idx],
            )
        })
        .collect())
}

fn normalize_format_codes(
    raw_formats: &[i16],
    field_count: usize,
    context: &str,
) -> Result<Vec<i16>, SessionError> {
    let formats = if raw_formats.is_empty() {
        vec![0; field_count]
    } else if raw_formats.len() == 1 {
        vec![raw_formats[0]; field_count]
    } else if raw_formats.len() == field_count {
        raw_formats.to_vec()
    } else {
        return Err(SessionError {
            message: format!(
                "{context} must contain 0, 1, or {field_count} entries (got {})",
                raw_formats.len()
            ),
        });
    };
    for format in &formats {
        if *format != 0 && *format != 1 {
            return Err(SessionError {
                message: format!("{context} contains unsupported format code {}", format),
            });
        }
    }
    Ok(formats)
}

fn infer_row_description_fields(
    columns: &[String],
    rows: &[Vec<ScalarValue>],
) -> Vec<RowDescriptionField> {
    let mut fields = Vec::with_capacity(columns.len());
    for (idx, name) in columns.iter().enumerate() {
        let first_non_null = rows.iter().find_map(|row| row.get(idx));
        let (type_oid, type_size) = match first_non_null {
            Some(ScalarValue::Bool(_)) => (16, 1),
            Some(ScalarValue::Int(_)) => (20, 8),
            Some(ScalarValue::Float(_)) => (701, 8),
            Some(ScalarValue::Text(_)) => (25, -1),
            Some(ScalarValue::Null) | None => (25, -1),
        };
        fields.push(default_field_description(name, type_oid, type_size, 0));
    }
    fields
}

fn default_field_description(
    name: &str,
    type_oid: PgType,
    type_size: i16,
    format_code: i16,
) -> RowDescriptionField {
    RowDescriptionField {
        name: name.to_string(),
        table_oid: 0,
        column_attr: 0,
        type_oid,
        type_size,
        type_modifier: -1,
        format_code,
    }
}

fn scram_attribute<'a>(message: &'a str, key: char) -> Option<&'a str> {
    message.split(',').find_map(|part| {
        let (k, value) = part.split_once('=')?;
        (k.len() == 1 && k.chars().next() == Some(key)).then_some(value)
    })
}

fn scram_hmac(key: &[u8], data: &[u8]) -> Result<[u8; 32], SessionError> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| SessionError {
        message: "SCRAM HMAC key initialization failed".to_string(),
    })?;
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

#[cfg(not(target_arch = "wasm32"))]
fn fill_random_bytes(out: &mut [u8]) {
    OsRng.fill_bytes(out);
}

#[cfg(target_arch = "wasm32")]
fn fill_random_bytes(out: &mut [u8]) {
    WASM_RANDOM_STATE.with(|state| {
        let mut value = state.get();
        for byte in out.iter_mut() {
            value = value
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (value >> 32) as u8;
        }
        state.set(value);
    });
}

fn encode_copy_binary_stream(
    columns: &[crate::tcop::engine::CopyBinaryColumn],
    rows: &[Vec<ScalarValue>],
) -> Result<Vec<u8>, SessionError> {
    let mut out = Vec::new();
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());

    for row in rows {
        if row.len() != columns.len() {
            return Err(SessionError {
                message: "COPY row width does not match relation column count".to_string(),
            });
        }
        out.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for (value, column) in row.iter().zip(columns.iter()) {
            encode_copy_binary_field(&mut out, value, column.type_oid)?;
        }
    }
    out.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(out)
}

fn encode_copy_binary_field(
    out: &mut Vec<u8>,
    value: &ScalarValue,
    type_oid: PgType,
) -> Result<(), SessionError> {
    if matches!(value, ScalarValue::Null) {
        out.extend_from_slice(&(-1i32).to_be_bytes());
        return Ok(());
    }

    let bytes = match (type_oid, value) {
        (16, ScalarValue::Bool(v)) => vec![if *v { 1 } else { 0 }],
        (20, ScalarValue::Int(v)) => v.to_be_bytes().to_vec(),
        (701, ScalarValue::Float(v)) => v.to_bits().to_be_bytes().to_vec(),
        (25, ScalarValue::Text(v)) => v.as_bytes().to_vec(),
        (25, other) => other.render().into_bytes(),
        (20, ScalarValue::Text(v)) => v
            .trim()
            .parse::<i64>()
            .map_err(|_| SessionError {
                message: "COPY integer field is invalid".to_string(),
            })?
            .to_be_bytes()
            .to_vec(),
        (701, ScalarValue::Text(v)) => v
            .trim()
            .parse::<f64>()
            .map_err(|_| SessionError {
                message: "COPY float field is invalid".to_string(),
            })?
            .to_bits()
            .to_be_bytes()
            .to_vec(),
        (16, ScalarValue::Text(v)) => match v.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => vec![1],
            "false" | "f" | "0" => vec![0],
            _ => {
                return Err(SessionError {
                    message: "COPY boolean field is invalid".to_string(),
                });
            }
        },
        _ => {
            return Err(SessionError {
                message: format!("COPY binary type oid {} is not supported", type_oid),
            });
        }
    };
    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    out.extend_from_slice(&bytes);
    Ok(())
}

fn parse_copy_binary_stream(
    payload: &[u8],
    column_type_oids: &[PgType],
) -> Result<Vec<Vec<ScalarValue>>, SessionError> {
    let mut idx = 0usize;
    let read_bytes = |idx: &mut usize, n: usize| -> Result<&[u8], SessionError> {
        if *idx + n > payload.len() {
            return Err(SessionError {
                message: "COPY binary payload is truncated".to_string(),
            });
        }
        let out = &payload[*idx..*idx + n];
        *idx += n;
        Ok(out)
    };
    let read_i16 = |idx: &mut usize| -> Result<i16, SessionError> {
        let bytes = read_bytes(idx, 2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    };
    let read_i32 = |idx: &mut usize| -> Result<i32, SessionError> {
        let bytes = read_bytes(idx, 4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    };

    let signature = read_bytes(&mut idx, 11)?;
    if signature != b"PGCOPY\n\xff\r\n\0" {
        return Err(SessionError {
            message: "invalid COPY binary signature".to_string(),
        });
    }
    let _flags = read_i32(&mut idx)?;
    let extension_len = read_i32(&mut idx)?;
    if extension_len < 0 {
        return Err(SessionError {
            message: "invalid COPY extension length".to_string(),
        });
    }
    let _ = read_bytes(&mut idx, extension_len as usize)?;

    let mut rows = Vec::new();
    loop {
        let field_count = read_i16(&mut idx)?;
        if field_count == -1 {
            break;
        }
        if field_count < 0 {
            return Err(SessionError {
                message: "invalid COPY row field count".to_string(),
            });
        }
        if field_count as usize != column_type_oids.len() {
            return Err(SessionError {
                message: format!(
                    "COPY row has {} columns but relation expects {}",
                    field_count,
                    column_type_oids.len()
                ),
            });
        }

        let mut row = Vec::with_capacity(field_count as usize);
        for type_oid in column_type_oids {
            let len = read_i32(&mut idx)?;
            if len == -1 {
                row.push(ScalarValue::Null);
                continue;
            }
            if len < -1 {
                return Err(SessionError {
                    message: "invalid COPY field length".to_string(),
                });
            }
            let raw = read_bytes(&mut idx, len as usize)?;
            let value = match *type_oid {
                16 => {
                    if raw.len() != 1 {
                        return Err(SessionError {
                            message: "COPY boolean field length must be 1".to_string(),
                        });
                    }
                    ScalarValue::Bool(raw[0] != 0)
                }
                20 => {
                    if raw.len() != 8 {
                        return Err(SessionError {
                            message: "COPY int8 field length must be 8".to_string(),
                        });
                    }
                    ScalarValue::Int(i64::from_be_bytes([
                        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                    ]))
                }
                701 => {
                    if raw.len() != 8 {
                        return Err(SessionError {
                            message: "COPY float8 field length must be 8".to_string(),
                        });
                    }
                    let bits = u64::from_be_bytes([
                        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                    ]);
                    ScalarValue::Float(f64::from_bits(bits))
                }
                25 => ScalarValue::Text(String::from_utf8(raw.to_vec()).map_err(|_| {
                    SessionError {
                        message: "COPY text field is not valid utf8".to_string(),
                    }
                })?),
                other => {
                    return Err(SessionError {
                        message: format!("unsupported COPY type oid {}", other),
                    });
                }
            };
            row.push(value);
        }
        rows.push(row);
    }

    if idx != payload.len() {
        return Err(SessionError {
            message: "COPY payload has trailing bytes".to_string(),
        });
    }
    Ok(rows)
}

fn portal_key(name: &str) -> String {
    if name.is_empty() {
        UNNAMED.to_string()
    } else {
        name.to_string()
    }
}

fn top_level_only_statement_error(statement: &Statement) -> Option<&'static str> {
    match statement {
        Statement::RefreshMaterializedView(refresh) if refresh.concurrently => Some(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY cannot be executed from a transaction block",
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
    use crate::tcop::engine::reset_global_storage_for_tests;

    fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
        with_global_state_lock(|| {
            reset_global_catalog_for_tests();
            reset_global_storage_for_tests();
            f()
        })
    }

    fn parse_bind_execute_sync_flow() -> Vec<FrontendMessage> {
        vec![
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT 1".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::Bind {
                portal_name: "p1".to_string(),
                statement_name: "s1".to_string(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
            FrontendMessage::Execute {
                portal_name: "p1".to_string(),
                max_rows: 0,
            },
            FrontendMessage::Sync,
        ]
    }

    #[test]
    fn simple_query_flow_emits_ready_and_completion() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([FrontendMessage::Query {
                sql: "SELECT 1".to_string(),
            }])
        });

        assert_eq!(
            out[0],
            BackendMessage::ReadyForQuery {
                status: ReadyForQueryStatus::Idle
            }
        );
        assert!(matches!(out[1], BackendMessage::RowDescription { .. }));
        assert!(matches!(out[2], BackendMessage::DataRow { .. }));
        assert!(matches!(
            out[3],
            BackendMessage::CommandComplete { ref tag, .. } if tag == "SELECT"
        ));
        assert_eq!(
            out[4],
            BackendMessage::ReadyForQuery {
                status: ReadyForQueryStatus::Idle
            }
        );
    }

    #[test]
    fn extended_query_flow_requires_sync_for_ready() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run(parse_bind_execute_sync_flow())
        });

        assert_eq!(
            out[0],
            BackendMessage::ReadyForQuery {
                status: ReadyForQueryStatus::Idle
            }
        );
        assert!(matches!(out[1], BackendMessage::ParseComplete));
        assert!(matches!(out[2], BackendMessage::BindComplete));
        assert!(matches!(out[3], BackendMessage::RowDescription { .. }));
        assert!(matches!(out[4], BackendMessage::DataRow { .. }));
        assert!(matches!(
            out[5],
            BackendMessage::CommandComplete { ref tag, .. } if tag == "SELECT"
        ));
        assert_eq!(
            out[6],
            BackendMessage::ReadyForQuery {
                status: ReadyForQueryStatus::Idle
            }
        );
    }

    #[test]
    fn describe_statement_uses_planned_type_metadata() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Parse {
                    statement_name: "s1".to_string(),
                    query: "SELECT 1::int8 AS id, 'x'::text AS name, true AS ok".to_string(),
                    parameter_types: vec![],
                },
                FrontendMessage::DescribeStatement {
                    statement_name: "s1".to_string(),
                },
                FrontendMessage::Sync,
            ])
        });

        let fields = out
            .iter()
            .find_map(|msg| {
                if let BackendMessage::RowDescription { fields } = msg {
                    Some(fields.clone())
                } else {
                    None
                }
            })
            .expect("row description should be present");
        assert_eq!(
            fields.iter().map(|field| field.type_oid).collect::<Vec<_>>(),
            vec![20, 25, 16]
        );
    }

    #[test]
    fn bind_rejects_binary_result_format_codes() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Parse {
                    statement_name: "s1".to_string(),
                    query: "SELECT 1".to_string(),
                    parameter_types: vec![],
                },
                FrontendMessage::Bind {
                    portal_name: "p1".to_string(),
                    statement_name: "s1".to_string(),
                    param_formats: vec![],
                    params: vec![],
                    result_formats: vec![1],
                },
                FrontendMessage::Sync,
            ])
        });

        assert!(out.iter().any(|msg| {
            matches!(
                msg,
                BackendMessage::ErrorResponse { message }
                    if message.contains("binary result formats are not supported yet")
            )
        }));
    }

    #[test]
    fn extended_protocol_error_skips_until_sync() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Parse {
                    statement_name: "bad".to_string(),
                    query: "SELECT FROM".to_string(),
                    parameter_types: vec![],
                },
                FrontendMessage::Parse {
                    statement_name: "s1".to_string(),
                    query: "SELECT 1".to_string(),
                    parameter_types: vec![],
                },
                FrontendMessage::Sync,
                FrontendMessage::Parse {
                    statement_name: "s2".to_string(),
                    query: "SELECT 2".to_string(),
                    parameter_types: vec![],
                },
                FrontendMessage::Sync,
            ])
        });

        assert!(matches!(out[1], BackendMessage::ErrorResponse { .. }));
        assert!(matches!(out[2], BackendMessage::ReadyForQuery { .. }));
        assert!(matches!(out[3], BackendMessage::ParseComplete));
        assert!(matches!(out[4], BackendMessage::ReadyForQuery { .. }));
    }

    #[test]
    fn aborted_transaction_block_allows_only_exit() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Query {
                    sql: "BEGIN".to_string(),
                },
                FrontendMessage::Parse {
                    statement_name: "bad".to_string(),
                    query: "SELECT FROM".to_string(),
                    parameter_types: vec![],
                },
                FrontendMessage::Sync,
                FrontendMessage::Query {
                    sql: "SELECT 1".to_string(),
                },
                FrontendMessage::Query {
                    sql: "ROLLBACK".to_string(),
                },
            ])
        });

        assert!(out.iter().any(|m| {
            matches!(
                m,
                BackendMessage::ErrorResponse { message }
                    if message.contains("aborted")
            )
        }));
        assert!(out.iter().any(|m| {
            matches!(
                m,
                BackendMessage::CommandComplete { tag, .. } if tag == "ROLLBACK"
            )
        }));
    }

    #[test]
    fn rollback_restores_engine_state() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Query {
                    sql: "BEGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE TABLE t (id int8)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO t VALUES (1)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "ROLLBACK".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT * FROM t".to_string(),
                },
            ])
        });

        assert!(out.iter().any(|m| {
            matches!(
                m,
                BackendMessage::CommandComplete { tag, .. } if tag == "ROLLBACK"
            )
        }));
        assert!(out.iter().any(|m| {
            matches!(
                m,
                BackendMessage::ErrorResponse { message } if message.contains("does not exist")
            )
        }));
    }

    #[test]
    fn rollback_to_savepoint_restores_partial_state() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Query {
                    sql: "BEGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE TABLE t (id int8)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO t VALUES (1)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SAVEPOINT s1".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO t VALUES (2)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "ROLLBACK TO SAVEPOINT s1".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT * FROM t ORDER BY 1".to_string(),
                },
                FrontendMessage::Query {
                    sql: "COMMIT".to_string(),
                },
            ])
        });

        let data_rows = out
            .iter()
            .filter_map(|message| {
                if let BackendMessage::DataRow { values } = message {
                    Some(values.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(data_rows, vec![vec!["1".to_string()]]);
    }

    #[test]
    fn failed_transaction_can_recover_with_rollback_to_savepoint() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Query {
                    sql: "BEGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SAVEPOINT s1".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT FROM".to_string(),
                },
                FrontendMessage::Query {
                    sql: "ROLLBACK TO SAVEPOINT s1".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT 1".to_string(),
                },
                FrontendMessage::Query {
                    sql: "COMMIT".to_string(),
                },
            ])
        });

        assert!(out.iter().any(|m| {
            matches!(m, BackendMessage::ErrorResponse { message } if message.contains("parse error"))
        }));
        assert!(out.iter().any(|m| {
            matches!(m, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
        }));
    }

    #[test]
    fn refresh_materialized_view_concurrently_is_rejected_in_transaction_block() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Query {
                    sql: "CREATE TABLE users (id int8 PRIMARY KEY)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO users VALUES (1)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users".to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE UNIQUE INDEX uq_mv_users_id ON mv_users (id)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "BEGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_users".to_string(),
                },
                FrontendMessage::Query {
                    sql: "ROLLBACK".to_string(),
                },
            ])
        });

        assert!(out.iter().any(|m| {
            matches!(
                m,
                BackendMessage::ErrorResponse { message }
                    if message.contains("cannot be executed from a transaction block")
            )
        }));
        assert!(out.iter().any(|m| {
            matches!(
                m,
                BackendMessage::CommandComplete { tag, .. } if tag == "ROLLBACK"
            )
        }));
    }

    #[test]
    fn explicit_transaction_keeps_writes_session_local_until_commit() {
        with_isolated_state(|| {
            let mut session_a = PostgresSession::new();
            let mut session_b = PostgresSession::new();

            session_a.run([FrontendMessage::Query {
                sql: "CREATE TABLE t (id int8)".to_string(),
            }]);

            let a_uncommitted = session_a.run([
                FrontendMessage::Query {
                    sql: "BEGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO t VALUES (1)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT count(*) FROM t".to_string(),
                },
            ]);
            assert!(a_uncommitted.iter().any(|m| {
                matches!(m, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
            }));

            let b_before_commit = session_b.run([FrontendMessage::Query {
                sql: "SELECT count(*) FROM t".to_string(),
            }]);
            assert!(b_before_commit.iter().any(|m| {
                matches!(m, BackendMessage::DataRow { values } if values == &vec!["0".to_string()])
            }));

            session_a.run([FrontendMessage::Query {
                sql: "COMMIT".to_string(),
            }]);

            let b_after_commit = session_b.run([FrontendMessage::Query {
                sql: "SELECT count(*) FROM t".to_string(),
            }]);
            assert!(b_after_commit.iter().any(|m| {
                matches!(m, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
            }));
        });
    }

    #[test]
    fn rollback_discards_session_local_writes() {
        with_isolated_state(|| {
            let mut session_a = PostgresSession::new();
            let mut session_b = PostgresSession::new();

            session_a.run([FrontendMessage::Query {
                sql: "CREATE TABLE t (id int8)".to_string(),
            }]);

            session_a.run([
                FrontendMessage::Query {
                    sql: "BEGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO t VALUES (1)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "ROLLBACK".to_string(),
                },
            ]);

            let b_after_rollback = session_b.run([FrontendMessage::Query {
                sql: "SELECT count(*) FROM t".to_string(),
            }]);
            assert!(b_after_rollback.iter().any(|m| {
                matches!(m, BackendMessage::DataRow { values } if values == &vec!["0".to_string()])
            }));
        });
    }

    #[test]
    fn table_privileges_can_be_granted_and_revoked() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Query {
                    sql: "CREATE TABLE docs (id int8)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO docs VALUES (1)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE ROLE app LOGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SET ROLE app".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT * FROM docs".to_string(),
                },
                FrontendMessage::Query {
                    sql: "RESET ROLE".to_string(),
                },
                FrontendMessage::Query {
                    sql: "GRANT SELECT ON TABLE docs TO app".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SET ROLE app".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT * FROM docs".to_string(),
                },
                FrontendMessage::Query {
                    sql: "RESET ROLE".to_string(),
                },
                FrontendMessage::Query {
                    sql: "REVOKE SELECT ON TABLE docs FROM app".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SET ROLE app".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT * FROM docs".to_string(),
                },
            ])
        });

        let permission_errors = out
            .iter()
            .filter(|msg| {
                matches!(
                    msg,
                    BackendMessage::ErrorResponse { message }
                        if message.contains("missing SELECT privilege")
                )
            })
            .count();
        assert_eq!(permission_errors, 2);
        assert!(out.iter().any(
            |msg| matches!(msg, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
        ));
    }

    #[test]
    fn rls_policies_filter_rows_and_enforce_with_check() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Query {
                    sql: "CREATE TABLE docs (id int8, owner text)".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO docs VALUES (1, 'app'), (2, 'other')".to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE ROLE app LOGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "GRANT SELECT, INSERT ON TABLE docs TO app".to_string(),
                },
                FrontendMessage::Query {
                    sql: "ALTER TABLE docs ENABLE ROW LEVEL SECURITY".to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE POLICY p_sel ON docs FOR SELECT TO app USING (owner = 'app')"
                        .to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE POLICY p_ins ON docs FOR INSERT TO app WITH CHECK (owner = 'app')"
                        .to_string(),
                },
                FrontendMessage::Query {
                    sql: "SET ROLE app".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT id FROM docs ORDER BY 1".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO docs VALUES (3, 'other')".to_string(),
                },
                FrontendMessage::Query {
                    sql: "INSERT INTO docs VALUES (4, 'app')".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT id FROM docs ORDER BY 1".to_string(),
                },
            ])
        });

        let data_rows = out
            .iter()
            .filter_map(|msg| match msg {
                BackendMessage::DataRow { values } => Some(values.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            data_rows,
            vec![
                vec!["1".to_string()],
                vec!["1".to_string()],
                vec!["4".to_string()]
            ]
        );
        assert!(out.iter().any(|msg| {
            matches!(
                msg,
                BackendMessage::ErrorResponse { message }
                    if message.contains("row-level security policy")
            )
        }));
    }

    #[test]
    fn startup_required_session_performs_handshake_and_query() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new_startup_required();
            session.run([
                FrontendMessage::Startup {
                    user: "postgres".to_string(),
                    database: Some("public".to_string()),
                    parameters: vec![("application_name".to_string(), "tests".to_string())],
                },
                FrontendMessage::Query {
                    sql: "SELECT 1".to_string(),
                },
            ])
        });

        assert!(
            out.iter()
                .any(|msg| matches!(msg, BackendMessage::AuthenticationOk))
        );
        assert!(out.iter().any(
            |msg| matches!(msg, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
        ));
    }

    #[test]
    fn startup_password_authentication_round_trip() {
        with_isolated_state(|| {
            let mut admin = PostgresSession::new();
            admin.run([FrontendMessage::Query {
                sql: "CREATE ROLE alice LOGIN PASSWORD 's3cr3t'".to_string(),
            }]);

            let mut session = PostgresSession::new_startup_required();
            let startup_out = session.run([FrontendMessage::Startup {
                user: "alice".to_string(),
                database: None,
                parameters: Vec::new(),
            }]);
            assert!(startup_out.iter().any(|msg| matches!(
                msg,
                BackendMessage::AuthenticationCleartextPassword
            ) || matches!(
                msg,
                BackendMessage::AuthenticationSasl { .. }
            )));

            let auth_out = session.run([
                FrontendMessage::Password {
                    password: "s3cr3t".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SELECT 1".to_string(),
                },
            ]);
            assert!(
                auth_out
                    .iter()
                    .any(|msg| matches!(msg, BackendMessage::AuthenticationOk))
            );
            assert!(auth_out.iter().any(
                |msg| matches!(msg, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
            ));
        });
    }

    #[test]
    fn security_changes_inside_transaction_rollback() {
        let out = with_isolated_state(|| {
            let mut session = PostgresSession::new();
            session.run([
                FrontendMessage::Query {
                    sql: "BEGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "CREATE ROLE temp LOGIN".to_string(),
                },
                FrontendMessage::Query {
                    sql: "ROLLBACK".to_string(),
                },
                FrontendMessage::Query {
                    sql: "SET ROLE temp".to_string(),
                },
            ])
        });

        assert!(out.iter().any(|msg| {
            matches!(
                msg,
                BackendMessage::ErrorResponse { message }
                    if message.contains("does not exist")
            )
        }));
    }
}
