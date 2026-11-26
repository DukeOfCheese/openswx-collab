//! The [`Store`](super::Store) and [`Tx`](super::Tx) implementations, written once for both
//! databases.
//!
//! [`store_impl!`] expands, in `sqlite.rs` and in `postgres.rs`, to the two trait implementations
//! (one line per method) and to the `q` module: one `async fn` per query, taking the database's
//! connection type, which binds the arguments of the shared SQL text in [`super::sql`]. The few
//! statements the dialects cannot share (row locks, the catalogue) are named
//! `sql::<dialect>::NAME`; there is no other dialect switch.
//!
//! What a store brings itself: its struct with a `migrator` field, `reader()` (a connection for
//! reads), `begin_tx()`, and its transaction type with `conn()`, `commit_tx()`, `rollback_tx()` and
//! `savepoint_tx()`.

/// See the module documentation. `$Store` and `$Tx` are the store and its transaction type,
/// `$Conn` its connection type, `$dialect` the module of [`super::sql`] with its own statements.
macro_rules! store_impl {
    ($Store:ident, $Tx:ident, $Conn:ty, $dialect:ident) => {
        impl $Store {
            /// Brings the schema up to date as `open` promises: an empty database is always built;
            /// an older one is upgraded only when `auto_migrate` is true, and is otherwise an error
            /// telling the operator to run `migrate`; a newer one, or one this server did not
            /// write, is always an error ([`SchemaError`]).
            async fn bring_up_to_date(&self, auto_migrate: bool) -> Result<()> {
                self.check_unchanged().await?;
                let status = self.schema_status().await?;
                let upgrade = status.current.is_some()
                    && status.unknown.is_empty()
                    && !status.pending.is_empty();
                if upgrade && !auto_migrate {
                    bail!(SchemaError::Outdated {
                        current: status.current,
                        latest: status.latest,
                    });
                }
                if !status.is_current() {
                    // Builds an empty database; refuses a newer or foreign one.
                    self.migrate().await?;
                }
                Ok(())
            }

            /// Applies the migrations this binary carries that the database has not had, and
            /// answers the schema status afterwards. Refuses a database written by a newer server,
            /// or not by this one.
            pub async fn migrate(&self) -> Result<SchemaStatus> {
                self.refuse_nested()?;
                self.check_unchanged().await?;
                let status = self.schema_status().await?;
                if !status.unknown.is_empty() {
                    bail!(SchemaError::Newer {
                        unknown: status.unknown,
                        latest: status.latest,
                    });
                }
                if status.current.is_none() && q::has_tables(&mut *self.reader().await?).await? {
                    bail!(SchemaError::Foreign);
                }
                if !status.pending.is_empty() {
                    self.run_migrations()
                        .await
                        .context("migrating the database")?;
                }
                self.schema_status().await
            }

            /// Fails when a migration the database has had differs from the one this binary
            /// carries under the same version (sqlx checks this only when it has something to
            /// apply).
            async fn check_unchanged(&self) -> Result<()> {
                for (version, checksum) in q::applied_checksums(&mut *self.reader().await?).await? {
                    let changed = self.migrator.iter().any(|m| {
                        m.version == version
                            && !m.migration_type.is_down_migration()
                            && *m.checksum != *checksum
                    });
                    if changed {
                        bail!(SchemaError::Changed { version });
                    }
                }
                Ok(())
            }
        }

        #[async_trait]
        impl Store for $Store {
            async fn health(&self) -> Result<()> {
                q::health(&mut *self.reader().await?).await
            }

            async fn schema_status(&self) -> Result<SchemaStatus> {
                let applied: Vec<i64> = q::applied_checksums(&mut *self.reader().await?)
                    .await?
                    .into_iter()
                    .map(|(version, _)| version)
                    .collect();
                let known: Vec<i64> = self
                    .migrator
                    .iter()
                    .filter(|m| !m.migration_type.is_down_migration())
                    .map(|m| m.version)
                    .collect();
                Ok(SchemaStatus::compare(&applied, &known))
            }

            async fn begin(&self) -> Result<Box<dyn Tx>> {
                Ok(Box::new(self.begin_tx().await?))
            }

            async fn begin_journal(&self) -> Result<Box<dyn Tx>> {
                Ok(Box::new(self.begin_journal_tx().await?))
            }

            async fn deleted_projects(&self, after: &str, limit: i64) -> Result<Vec<Id>> {
                q::deleted_projects(&mut *self.reader().await?, after, limit).await
            }

            async fn deleted_orgs(&self, after: &str, limit: i64) -> Result<Vec<Id>> {
                q::deleted_orgs(&mut *self.reader().await?, after, limit).await
            }

            async fn documents_of_deleted_project(
                &self,
                project_id: &str,
                after: &str,
                limit: i64,
            ) -> Result<Vec<Id>> {
                let mut c = self.reader().await?;
                q::documents_of_deleted_project(&mut c, project_id, after, limit).await
            }

            async fn user_count(&self) -> Result<u64> {
                q::user_count(&mut *self.reader().await?).await
            }

            async fn user_by_id(&self, id: &str) -> Result<Option<User>> {
                q::user_by_id(&mut *self.reader().await?, id).await
            }

            async fn user_by_email(&self, email: &str) -> Result<Option<User>> {
                q::user_by_email(&mut *self.reader().await?, email).await
            }

            async fn session_by_hash(&self, token_hash: &str) -> Result<Option<Session>> {
                q::session_by_hash(&mut *self.reader().await?, token_hash).await
            }

            async fn password_reset_by_hash(
                &self,
                token_hash: &str,
            ) -> Result<Option<PasswordReset>> {
                q::password_reset_by_hash(&mut *self.reader().await?, token_hash).await
            }

            async fn oauth_identity(
                &self,
                provider: &str,
                subject: &str,
            ) -> Result<Option<OAuthIdentity>> {
                q::oauth_identity(&mut *self.reader().await?, provider, subject).await
            }

            async fn user_identities(&self, user_id: &str) -> Result<Vec<OAuthIdentity>> {
                q::user_identities(&mut *self.reader().await?, user_id).await
            }

            async fn org_by_id(&self, id: &str) -> Result<Option<Org>> {
                q::org_by_id(&mut *self.reader().await?, id).await
            }

            async fn org_member(&self, org_id: &str, user_id: &str) -> Result<Option<OrgMember>> {
                q::org_member(&mut *self.reader().await?, org_id, user_id).await
            }

            async fn orgs_of_user(&self, user_id: &str) -> Result<Vec<OrgSummary>> {
                q::orgs_of_user(&mut *self.reader().await?, user_id).await
            }

            async fn org_members(&self, org_id: &str) -> Result<Vec<Member>> {
                q::org_members(&mut *self.reader().await?, org_id).await
            }

            async fn org_member_count(&self, org_id: &str) -> Result<u64> {
                q::org_member_count(&mut *self.reader().await?, org_id).await
            }

            async fn project_by_id(&self, id: &str) -> Result<Option<Project>> {
                q::project_by_id(&mut *self.reader().await?, id).await
            }

            async fn project_member(
                &self,
                project_id: &str,
                user_id: &str,
            ) -> Result<Option<ProjectMember>> {
                q::project_member(&mut *self.reader().await?, project_id, user_id).await
            }

            async fn projects_in_org(&self, org_id: &str) -> Result<Vec<Project>> {
                q::projects_in_org(&mut *self.reader().await?, org_id).await
            }

            async fn member_projects(
                &self,
                org_id: &str,
                user_id: &str,
            ) -> Result<Vec<ProjectWithRole>> {
                q::member_projects(&mut *self.reader().await?, org_id, user_id).await
            }

            async fn project_members(&self, project_id: &str) -> Result<Vec<Member>> {
                q::project_members(&mut *self.reader().await?, project_id).await
            }

            async fn invite_by_id(&self, id: &str) -> Result<Option<Invite>> {
                q::invite_by_id(&mut *self.reader().await?, id).await
            }

            async fn invite_by_hash(&self, token_hash: &str) -> Result<Option<Invite>> {
                q::invite_by_hash(&mut *self.reader().await?, token_hash).await
            }

            async fn open_invites(&self, org_id: &str, now: f64) -> Result<Vec<Invite>> {
                q::open_invites(&mut *self.reader().await?, org_id, now).await
            }

            async fn document_by_id(&self, id: &str) -> Result<Option<Document>> {
                q::document_by_id(&mut *self.reader().await?, id).await
            }

            async fn documents_in_project(&self, project_id: &str) -> Result<Vec<Document>> {
                q::documents_in_project(&mut *self.reader().await?, project_id).await
            }

            async fn latest_snapshot(&self, document_id: &str) -> Result<Option<SnapshotRow>> {
                q::latest_snapshot(&mut *self.reader().await?, document_id).await
            }

            async fn folder_by_id(&self, id: &str) -> Result<Option<Folder>> {
                q::folder_by_id(&mut *self.reader().await?, id).await
            }

            async fn folders_in_project(&self, project_id: &str) -> Result<Vec<Folder>> {
                q::folders_in_project(&mut *self.reader().await?, project_id).await
            }

            async fn changes_between(
                &self,
                document_id: &str,
                after: i64,
                upto: i64,
                limit: i64,
            ) -> Result<Vec<ChangeRow>> {
                q::changes_between(&mut *self.reader().await?, document_id, after, upto, limit)
                    .await
            }

            async fn change_by_seq(
                &self,
                document_id: &str,
                seq: i64,
            ) -> Result<Option<ChangeRow>> {
                q::change_by_seq(&mut *self.reader().await?, document_id, seq).await
            }

            async fn change_events_between(
                &self,
                document_id: &str,
                after: i64,
                upto: i64,
                limit: i64,
            ) -> Result<Vec<ChangeEventRow>> {
                q::change_events_between(
                    &mut *self.reader().await?,
                    document_id,
                    after,
                    upto,
                    limit,
                )
                .await
            }

            async fn change_seq_by_client(
                &self,
                document_id: &str,
                client_id: &str,
                change_id: &str,
            ) -> Result<Option<i64>> {
                q::change_seq_by_client(
                    &mut *self.reader().await?,
                    document_id,
                    client_id,
                    change_id,
                )
                .await
            }

            async fn recent_change_ids(
                &self,
                document_id: &str,
                client_id: &str,
                limit: i64,
            ) -> Result<Vec<(String, i64)>> {
                q::recent_change_ids(&mut *self.reader().await?, document_id, client_id, limit)
                    .await
            }

            async fn undo_target(
                &self,
                document_id: &str,
                client_id: &str,
                change_id: &str,
            ) -> Result<Option<UndoTarget>> {
                q::undo_target(
                    &mut *self.reader().await?,
                    document_id,
                    client_id,
                    change_id,
                )
                .await
            }

            async fn change_sizes(
                &self,
                document_id: &str,
                before: i64,
                after: i64,
                limit: i64,
            ) -> Result<Vec<ChangeSize>> {
                q::change_sizes(
                    &mut *self.reader().await?,
                    document_id,
                    before,
                    after,
                    limit,
                )
                .await
            }

            async fn change_sizes_after(
                &self,
                document_id: &str,
                after: i64,
                upto: i64,
                limit: i64,
            ) -> Result<Vec<ChangeSize>> {
                let mut c = self.reader().await?;
                q::change_sizes_after(&mut c, document_id, after, upto, limit).await
            }

            async fn history(
                &self,
                document_id: &str,
                before: i64,
                after: i64,
                limit: i64,
            ) -> Result<Vec<HistoryRow>> {
                q::history(
                    &mut *self.reader().await?,
                    document_id,
                    before,
                    after,
                    limit,
                )
                .await
            }

            async fn versions(&self, document_id: &str) -> Result<Vec<VersionWithAuthor>> {
                q::versions(&mut *self.reader().await?, document_id).await
            }

            async fn version_by_name(
                &self,
                document_id: &str,
                name: &str,
            ) -> Result<Option<Version>> {
                q::version_by_name(&mut *self.reader().await?, document_id, name).await
            }

            async fn snapshot_at_or_before(
                &self,
                document_id: &str,
                seq: i64,
            ) -> Result<Option<SnapshotRow>> {
                q::snapshot_at_or_before(&mut *self.reader().await?, document_id, seq).await
            }

            async fn snapshot_row(
                &self,
                document_id: &str,
                epoch: i64,
                seq: i64,
            ) -> Result<Option<SnapshotRow>> {
                q::snapshot_row(&mut *self.reader().await?, document_id, epoch, seq).await
            }

            async fn room_lease(&self, document_id: &str) -> Result<Option<RoomLease>> {
                q::room_lease(&mut *self.reader().await?, document_id).await
            }

            async fn assembly_refs(&self, assembly_id: &str) -> Result<Vec<AssemblyRefRow>> {
                q::assembly_refs(&mut *self.reader().await?, assembly_id).await
            }

            async fn parts_by_ids(&self, ids: &[Id]) -> Result<Vec<PartRow>> {
                q::parts_by_ids(&mut *self.reader().await?, ids).await
            }

            async fn assemblies_using(&self, part_id: &str) -> Result<Vec<AssemblyUse>> {
                q::assemblies_using(&mut *self.reader().await?, part_id).await
            }

            async fn versions_of(&self, ids: &[Id]) -> Result<Vec<VersionWithAuthor>> {
                q::versions_of(&mut *self.reader().await?, ids).await
            }

            async fn refs_to_part(&self, part_id: &str) -> Result<Vec<AssemblyRefRow>> {
                q::refs_to_part(&mut *self.reader().await?, part_id).await
            }

            async fn pinned_before(&self, part_id: &str, seq: i64) -> Result<Vec<PinnedComponent>> {
                q::pinned_before(&mut *self.reader().await?, part_id, seq).await
            }

            async fn user_memberships(&self, user_id: &str) -> Result<Vec<Membership>> {
                q::user_memberships(&mut *self.reader().await?, user_id).await
            }

            async fn user_project_roles(&self, user_id: &str) -> Result<Vec<ProjectWithRole>> {
                q::user_project_roles(&mut *self.reader().await?, user_id).await
            }

            async fn user_sessions(&self, user_id: &str) -> Result<Vec<Session>> {
                q::user_sessions(&mut *self.reader().await?, user_id).await
            }

            async fn changes_by_author(
                &self,
                user_id: &str,
                after_document: &str,
                after_seq: i64,
                limit: i64,
            ) -> Result<Vec<AuthoredChange>> {
                let mut c = self.reader().await?;
                q::changes_by_author(&mut c, user_id, after_document, after_seq, limit).await
            }

            async fn versions_by_author(
                &self,
                user_id: &str,
                after: &str,
                limit: i64,
            ) -> Result<Vec<Version>> {
                q::versions_by_author(&mut *self.reader().await?, user_id, after, limit).await
            }
        }

        #[async_trait]
        impl Tx for $Tx<'_> {
            async fn commit(self: Box<Self>) -> Result<()> {
                self.commit_tx().await
            }

            async fn rollback(self: Box<Self>) -> Result<()> {
                self.rollback_tx().await
            }

            async fn savepoint(&mut self) -> Result<Box<dyn Tx + '_>> {
                Ok(Box::new(self.savepoint_tx().await?))
            }

            async fn user_count(&mut self) -> Result<u64> {
                let n = q::user_count(self.conn()).await?;
                if n > 0 {
                    return Ok(n);
                }
                // Nobody yet: whoever goes on to make the first account holds the accounts lock
                // while it does, and counts under it.
                q::lock_accounts(self.conn()).await?;
                q::user_count(self.conn()).await
            }

            async fn user_by_id(&mut self, id: &str) -> Result<Option<User>> {
                q::user_by_id(self.conn(), id).await
            }

            async fn user_by_email(&mut self, email: &str) -> Result<Option<User>> {
                q::user_by_email(self.conn(), email).await
            }

            async fn insert_user(&mut self, user: &User) -> Result<()> {
                q::insert_user(self.conn(), user).await
            }

            async fn set_user_name(&mut self, id: &str, name: &str) -> Result<()> {
                q::set_user_name(self.conn(), id, name).await
            }

            async fn set_user_password(
                &mut self,
                id: &str,
                password_hash: Option<&str>,
            ) -> Result<()> {
                q::set_user_password(self.conn(), id, password_hash).await
            }

            async fn set_user_email_verified(&mut self, id: &str, verified: bool) -> Result<()> {
                q::set_user_email_verified(self.conn(), id, verified).await
            }

            async fn set_user_disabled(&mut self, id: &str, disabled: bool) -> Result<()> {
                q::set_user_disabled(self.conn(), id, disabled).await
            }

            async fn insert_session(&mut self, session: &Session) -> Result<()> {
                q::insert_session(self.conn(), session).await
            }

            async fn delete_session(&mut self, token_hash: &str) -> Result<()> {
                q::delete_session(self.conn(), token_hash).await
            }

            async fn delete_user_sessions(&mut self, user_id: &str) -> Result<()> {
                q::delete_user_sessions(self.conn(), user_id).await
            }

            async fn delete_other_user_sessions(
                &mut self,
                user_id: &str,
                keep: &str,
            ) -> Result<()> {
                q::delete_other_user_sessions(self.conn(), user_id, keep).await
            }

            async fn delete_expired_user_sessions(
                &mut self,
                user_id: &str,
                now: f64,
            ) -> Result<()> {
                q::delete_expired_user_sessions(self.conn(), user_id, now).await
            }

            async fn insert_password_reset(&mut self, reset: &PasswordReset) -> Result<()> {
                q::insert_password_reset(self.conn(), reset).await
            }

            async fn take_password_reset(
                &mut self,
                token_hash: &str,
            ) -> Result<Option<PasswordReset>> {
                q::take_password_reset(self.conn(), token_hash).await
            }

            async fn delete_user_password_resets(&mut self, user_id: &str) -> Result<()> {
                q::delete_user_password_resets(self.conn(), user_id).await
            }

            async fn delete_expired_password_resets(&mut self, now: f64) -> Result<()> {
                q::delete_expired_password_resets(self.conn(), now).await
            }

            async fn oauth_identity(
                &mut self,
                provider: &str,
                subject: &str,
            ) -> Result<Option<OAuthIdentity>> {
                q::oauth_identity(self.conn(), provider, subject).await
            }

            async fn user_identities(&mut self, user_id: &str) -> Result<Vec<OAuthIdentity>> {
                q::user_identities(self.conn(), user_id).await
            }

            async fn insert_oauth_identity(&mut self, identity: &OAuthIdentity) -> Result<()> {
                q::insert_oauth_identity(self.conn(), identity).await
            }

            async fn delete_user_identities(
                &mut self,
                user_id: &str,
                provider: &str,
            ) -> Result<()> {
                q::delete_user_identities(self.conn(), user_id, provider).await
            }

            async fn insert_oauth_state(&mut self, state: &OAuthState) -> Result<()> {
                q::insert_oauth_state(self.conn(), state).await
            }

            async fn take_oauth_state(&mut self, state_hash: &str) -> Result<Option<OAuthState>> {
                q::take_oauth_state(self.conn(), state_hash).await
            }

            async fn delete_expired_oauth_states(&mut self, now: f64) -> Result<()> {
                q::delete_expired_oauth_states(self.conn(), now).await
            }

            async fn insert_login_code(&mut self, code: &LoginCode) -> Result<()> {
                q::insert_login_code(self.conn(), code).await
            }

            async fn take_login_code(&mut self, code_hash: &str) -> Result<Option<LoginCode>> {
                q::take_login_code(self.conn(), code_hash).await
            }

            async fn delete_expired_login_codes(&mut self, now: f64) -> Result<()> {
                q::delete_expired_login_codes(self.conn(), now).await
            }

            async fn insert_org(&mut self, org: &Org) -> Result<()> {
                q::insert_org(self.conn(), org).await
            }

            async fn set_org_name(&mut self, id: &str, name: &str) -> Result<()> {
                q::set_org_name(self.conn(), id, name).await
            }

            async fn org_by_id(&mut self, id: &str) -> Result<Option<Org>> {
                q::org_by_id(self.conn(), id).await
            }

            async fn org_member(
                &mut self,
                org_id: &str,
                user_id: &str,
            ) -> Result<Option<OrgMember>> {
                q::org_member(self.conn(), org_id, user_id).await
            }

            async fn insert_org_member(&mut self, member: &OrgMember) -> Result<()> {
                q::insert_org_member(self.conn(), member).await
            }

            async fn set_org_member_role(
                &mut self,
                org_id: &str,
                user_id: &str,
                role: &str,
            ) -> Result<()> {
                q::set_org_member_role(self.conn(), org_id, user_id, role).await
            }

            async fn delete_org_member(&mut self, org_id: &str, user_id: &str) -> Result<()> {
                q::delete_org_member(self.conn(), org_id, user_id).await
            }

            async fn org_member_count(&mut self, org_id: &str) -> Result<u64> {
                q::org_member_count(self.conn(), org_id).await
            }

            async fn org_owner_count(&mut self, org_id: &str) -> Result<u64> {
                q::org_owner_count(self.conn(), org_id).await
            }

            async fn delete_project_members_in_org(
                &mut self,
                org_id: &str,
                user_id: &str,
            ) -> Result<()> {
                q::delete_project_members_in_org(self.conn(), org_id, user_id).await
            }

            async fn invite_by_hash(&mut self, token_hash: &str) -> Result<Option<Invite>> {
                q::invite_by_hash(self.conn(), token_hash).await
            }

            async fn insert_invite(&mut self, invite: &Invite) -> Result<()> {
                q::insert_invite(self.conn(), invite).await
            }

            async fn claim_invite(&mut self, id: &str, user_id: &str) -> Result<bool> {
                q::claim_invite(self.conn(), id, user_id).await
            }

            async fn delete_invite(&mut self, org_id: &str, id: &str) -> Result<bool> {
                q::delete_invite(self.conn(), org_id, id).await
            }

            async fn insert_project(&mut self, project: &Project) -> Result<()> {
                q::insert_project(self.conn(), project).await
            }

            async fn set_project_name(&mut self, id: &str, name: &str) -> Result<()> {
                q::set_project_name(self.conn(), id, name).await
            }

            async fn project_by_id(&mut self, id: &str) -> Result<Option<Project>> {
                q::project_by_id(self.conn(), id).await
            }

            async fn project_member(
                &mut self,
                project_id: &str,
                user_id: &str,
            ) -> Result<Option<ProjectMember>> {
                q::project_member(self.conn(), project_id, user_id).await
            }

            async fn insert_project_member(&mut self, member: &ProjectMember) -> Result<()> {
                q::insert_project_member(self.conn(), member).await
            }

            async fn set_project_member_role(
                &mut self,
                project_id: &str,
                user_id: &str,
                role: &str,
            ) -> Result<()> {
                q::set_project_member_role(self.conn(), project_id, user_id, role).await
            }

            async fn delete_project_member(
                &mut self,
                project_id: &str,
                user_id: &str,
            ) -> Result<bool> {
                q::delete_project_member(self.conn(), project_id, user_id).await
            }

            async fn project_document_ids(&mut self, project_id: &str) -> Result<Vec<Id>> {
                q::project_document_ids(self.conn(), project_id).await
            }

            async fn mark_project_documents_deleted(
                &mut self,
                project_id: &str,
                now: f64,
            ) -> Result<()> {
                q::mark_project_documents_deleted(self.conn(), project_id, now).await
            }

            async fn mark_project_deleted(&mut self, id: &str, now: f64) -> Result<bool> {
                q::mark_project_deleted(self.conn(), id, now).await
            }

            async fn mark_org_deleted(&mut self, id: &str, now: f64) -> Result<bool> {
                q::mark_org_deleted(self.conn(), id, now).await
            }

            async fn purge_document_rows(&mut self, document_id: &str, limit: i64) -> Result<u64> {
                q::purge_document_rows(self.conn(), document_id, limit).await
            }

            async fn purge_document(&mut self, id: &str) -> Result<bool> {
                q::purge_document(self.conn(), id).await
            }

            async fn purge_project_rows(&mut self, project_id: &str, limit: i64) -> Result<u64> {
                q::purge_project_rows(self.conn(), project_id, limit).await
            }

            async fn purge_project(&mut self, id: &str) -> Result<bool> {
                q::purge_project(self.conn(), id).await
            }

            async fn purge_org(&mut self, id: &str) -> Result<bool> {
                q::purge_org(self.conn(), id).await
            }

            async fn insert_document(&mut self, document: &Document) -> Result<()> {
                q::insert_document(self.conn(), document).await
            }

            async fn insert_snapshot(&mut self, snapshot: &SnapshotRow) -> Result<()> {
                q::insert_snapshot(self.conn(), snapshot).await
            }

            async fn document_by_id(&mut self, id: &str) -> Result<Option<Document>> {
                q::document_by_id(self.conn(), id).await
            }

            async fn set_document_name(&mut self, id: &str, name: &str) -> Result<()> {
                q::set_document_name(self.conn(), id, name).await
            }

            async fn set_document_folder(
                &mut self,
                id: &str,
                folder_id: Option<&str>,
            ) -> Result<()> {
                q::set_document_folder(self.conn(), id, folder_id).await
            }

            async fn mark_document_deleted(&mut self, id: &str, now: f64) -> Result<bool> {
                q::mark_document_deleted(self.conn(), id, now).await
            }

            async fn insert_folder(&mut self, folder: &Folder) -> Result<()> {
                q::insert_folder(self.conn(), folder).await
            }

            async fn folder_by_id(&mut self, id: &str) -> Result<Option<Folder>> {
                q::folder_by_id(self.conn(), id).await
            }

            async fn folders_in_project(&mut self, project_id: &str) -> Result<Vec<Folder>> {
                q::folders_in_project(self.conn(), project_id).await
            }

            async fn set_folder_name(&mut self, id: &str, name: &str) -> Result<()> {
                q::set_folder_name(self.conn(), id, name).await
            }

            async fn set_folder_parent(&mut self, id: &str, parent_id: Option<&str>) -> Result<()> {
                q::set_folder_parent(self.conn(), id, parent_id).await
            }

            async fn move_child_folders(
                &mut self,
                folder_id: &str,
                parent_id: Option<&str>,
            ) -> Result<()> {
                q::move_child_folders(self.conn(), folder_id, parent_id).await
            }

            async fn move_folder_documents(
                &mut self,
                folder_id: &str,
                parent_id: Option<&str>,
            ) -> Result<()> {
                q::move_folder_documents(self.conn(), folder_id, parent_id).await
            }

            async fn delete_folder(&mut self, id: &str) -> Result<()> {
                q::delete_folder(self.conn(), id).await
            }

            async fn insert_change(&mut self, change: &NewChange<'_>) -> Result<bool> {
                q::insert_change(self.conn(), change).await
            }

            async fn insert_changes(&mut self, changes: &[NewChange<'_>]) -> Result<u64> {
                q::insert_changes(self.conn(), changes).await
            }

            async fn set_document_head(
                &mut self,
                id: &str,
                head_seq: i64,
                updated_at: f64,
            ) -> Result<()> {
                q::set_document_head(self.conn(), id, head_seq, updated_at).await
            }

            async fn set_reverted_by(
                &mut self,
                document_id: &str,
                seq: i64,
                by: i64,
            ) -> Result<bool> {
                q::set_reverted_by(self.conn(), document_id, seq, by).await
            }

            async fn change_broke(
                &mut self,
                document_id: &str,
                seq: i64,
            ) -> Result<Option<Option<String>>> {
                q::change_broke(self.conn(), document_id, seq).await
            }

            async fn set_change_broke(
                &mut self,
                document_id: &str,
                seq: i64,
                broke: &str,
            ) -> Result<()> {
                q::set_change_broke(self.conn(), document_id, seq, broke).await
            }

            async fn insert_version(&mut self, version: &Version) -> Result<bool> {
                q::insert_version(self.conn(), version).await
            }

            async fn delete_version(&mut self, document_id: &str, id: &str) -> Result<bool> {
                q::delete_version(self.conn(), document_id, id).await
            }

            async fn replace_assembly_refs(
                &mut self,
                assembly_id: &str,
                rows: &[AssemblyRefRow],
            ) -> Result<()> {
                q::replace_assembly_refs(self.conn(), assembly_id, rows).await
            }

            async fn lock_live_document(&mut self, id: &str) -> Result<bool> {
                q::lock_live_document(self.conn(), id).await
            }

            async fn lock_accounts(&mut self) -> Result<()> {
                q::lock_accounts(self.conn()).await
            }

            async fn lock_user(&mut self, id: &str) -> Result<bool> {
                q::lock_user(self.conn(), id).await
            }

            async fn lock_org(&mut self, id: &str) -> Result<bool> {
                q::lock_org(self.conn(), id).await
            }

            async fn lock_documents(&mut self, ids: &[Id]) -> Result<()> {
                q::lock_documents(self.conn(), ids).await
            }

            async fn assemblies_using(&mut self, part_id: &str) -> Result<Vec<AssemblyUse>> {
                q::assemblies_using(self.conn(), part_id).await
            }

            async fn assemblies_using_project(
                &mut self,
                project_id: &str,
            ) -> Result<Vec<AssemblyUse>> {
                q::assemblies_using_project(self.conn(), project_id).await
            }

            async fn snapshot_row(
                &mut self,
                document_id: &str,
                epoch: i64,
                seq: i64,
            ) -> Result<Option<SnapshotRow>> {
                q::snapshot_row(self.conn(), document_id, epoch, seq).await
            }

            async fn room_lease(&mut self, document_id: &str) -> Result<Option<RoomLease>> {
                q::room_lease(self.conn(), document_id).await
            }

            async fn lock_room_lease(&mut self, document_id: &str) -> Result<Option<i64>> {
                q::lock_room_lease(self.conn(), document_id).await
            }

            async fn insert_room_lease(&mut self, lease: &RoomLease) -> Result<bool> {
                q::insert_room_lease(self.conn(), lease).await
            }

            async fn take_room_lease(
                &mut self,
                document_id: &str,
                node_id: &str,
                expires_at: f64,
            ) -> Result<Option<i64>> {
                q::take_room_lease(self.conn(), document_id, node_id, expires_at).await
            }

            async fn orgs_owned_alone(&mut self, user_id: &str) -> Result<Vec<Org>> {
                q::orgs_owned_alone(self.conn(), user_id).await
            }

            async fn active_admin_count(&mut self) -> Result<u64> {
                q::active_admin_count(self.conn()).await
            }

            async fn other_active_user_count(&mut self, user_id: &str) -> Result<u64> {
                q::other_active_user_count(self.conn(), user_id).await
            }

            async fn user_org_ids(&mut self, user_id: &str) -> Result<Vec<Id>> {
                q::user_org_ids(self.conn(), user_id).await
            }

            async fn erase_user(
                &mut self,
                id: &str,
                email: &str,
                name: &str,
                now: f64,
            ) -> Result<()> {
                q::erase_user(self.conn(), id, email, name, now).await
            }

            async fn org_document_ids(&mut self, org_id: &str) -> Result<Vec<Id>> {
                q::org_document_ids(self.conn(), org_id).await
            }

            async fn org_member_ids(&mut self, org_id: &str) -> Result<Vec<Id>> {
                q::org_member_ids(self.conn(), org_id).await
            }
        }

        /// One function per query; the trait methods call these with a reader or the transaction's
        /// connection.
        mod q {
            use super::*;

            pub async fn health(c: &mut $Conn) -> Result<()> {
                sqlx::query(sql::HEALTH).execute(c).await?;
                Ok(())
            }

            /// Versions and checksums recorded in `_sqlx_migrations`; none when the table does not exist.
            pub async fn applied_checksums(c: &mut $Conn) -> Result<Vec<(i64, Vec<u8>)>> {
                let exists: i64 = sqlx::query_scalar(sql::$dialect::MIGRATION_RECORD_EXISTS)
                    .fetch_one(&mut *c)
                    .await?;
                if exists == 0 {
                    return Ok(Vec::new());
                }
                Ok(sqlx::query_as(sql::APPLIED_MIGRATIONS)
                    .bind(true)
                    .fetch_all(c)
                    .await?)
            }

            /// Whether the database (on PostgreSQL: the schema in use) has any table besides the
            /// database's own and the migration record.
            pub async fn has_tables(c: &mut $Conn) -> Result<bool> {
                let n: i64 = sqlx::query_scalar(sql::$dialect::OTHER_TABLES)
                    .fetch_one(c)
                    .await?;
                Ok(n > 0)
            }

            pub async fn user_count(c: &mut $Conn) -> Result<u64> {
                let n: i64 = sqlx::query_scalar(sql::USER_COUNT).fetch_one(c).await?;
                Ok(u64::try_from(n)?)
            }

            pub async fn user_by_id(c: &mut $Conn, id: &str) -> Result<Option<User>> {
                Ok(sqlx::query_as(sql::USER_BY_ID)
                    .bind(id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn user_by_email(c: &mut $Conn, email: &str) -> Result<Option<User>> {
                Ok(sqlx::query_as(sql::USER_BY_EMAIL)
                    .bind(email)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn insert_user(c: &mut $Conn, u: &User) -> Result<()> {
                sqlx::query(sql::INSERT_USER)
                    .bind(&u.id)
                    .bind(&u.email)
                    .bind(&u.name)
                    .bind(&u.password_hash)
                    .bind(u.is_server_admin)
                    .bind(u.disabled)
                    .bind(u.email_verified)
                    .bind(u.created_at)
                    .bind(u.deleted_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_user_name(c: &mut $Conn, id: &str, name: &str) -> Result<()> {
                sqlx::query(sql::SET_USER_NAME)
                    .bind(id)
                    .bind(name)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_user_password(
                c: &mut $Conn,
                id: &str,
                password_hash: Option<&str>,
            ) -> Result<()> {
                sqlx::query(sql::SET_USER_PASSWORD)
                    .bind(id)
                    .bind(password_hash)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_user_email_verified(
                c: &mut $Conn,
                id: &str,
                verified: bool,
            ) -> Result<()> {
                sqlx::query(sql::SET_USER_EMAIL_VERIFIED)
                    .bind(id)
                    .bind(verified)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_user_disabled(c: &mut $Conn, id: &str, disabled: bool) -> Result<()> {
                sqlx::query(sql::SET_USER_DISABLED)
                    .bind(id)
                    .bind(disabled)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn insert_session(c: &mut $Conn, s: &Session) -> Result<()> {
                sqlx::query(sql::INSERT_SESSION)
                    .bind(&s.token_hash)
                    .bind(&s.user_id)
                    .bind(s.created_at)
                    .bind(s.expires_at)
                    .bind(&s.label)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn session_by_hash(
                c: &mut $Conn,
                token_hash: &str,
            ) -> Result<Option<Session>> {
                Ok(sqlx::query_as(sql::SESSION_BY_HASH)
                    .bind(token_hash)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn delete_session(c: &mut $Conn, token_hash: &str) -> Result<()> {
                sqlx::query(sql::DELETE_SESSION)
                    .bind(token_hash)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn delete_user_sessions(c: &mut $Conn, user_id: &str) -> Result<()> {
                sqlx::query(sql::DELETE_USER_SESSIONS)
                    .bind(user_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn delete_other_user_sessions(
                c: &mut $Conn,
                user_id: &str,
                keep: &str,
            ) -> Result<()> {
                sqlx::query(sql::DELETE_OTHER_USER_SESSIONS)
                    .bind(user_id)
                    .bind(keep)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn delete_expired_user_sessions(
                c: &mut $Conn,
                user_id: &str,
                now: f64,
            ) -> Result<()> {
                sqlx::query(sql::DELETE_EXPIRED_USER_SESSIONS)
                    .bind(user_id)
                    .bind(now)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn insert_password_reset(c: &mut $Conn, r: &PasswordReset) -> Result<()> {
                sqlx::query(sql::INSERT_PASSWORD_RESET)
                    .bind(&r.token_hash)
                    .bind(&r.user_id)
                    .bind(r.expires_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn password_reset_by_hash(
                c: &mut $Conn,
                token_hash: &str,
            ) -> Result<Option<PasswordReset>> {
                Ok(sqlx::query_as(sql::PASSWORD_RESET_BY_HASH)
                    .bind(token_hash)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn take_password_reset(
                c: &mut $Conn,
                token_hash: &str,
            ) -> Result<Option<PasswordReset>> {
                Ok(sqlx::query_as(sql::TAKE_PASSWORD_RESET)
                    .bind(token_hash)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn delete_user_password_resets(c: &mut $Conn, user_id: &str) -> Result<()> {
                sqlx::query(sql::DELETE_USER_PASSWORD_RESETS)
                    .bind(user_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn delete_expired_password_resets(c: &mut $Conn, now: f64) -> Result<()> {
                sqlx::query(sql::DELETE_EXPIRED_PASSWORD_RESETS)
                    .bind(now)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn oauth_identity(
                c: &mut $Conn,
                provider: &str,
                subject: &str,
            ) -> Result<Option<OAuthIdentity>> {
                Ok(sqlx::query_as(sql::OAUTH_IDENTITY)
                    .bind(provider)
                    .bind(subject)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn user_identities(
                c: &mut $Conn,
                user_id: &str,
            ) -> Result<Vec<OAuthIdentity>> {
                Ok(sqlx::query_as(sql::USER_IDENTITIES)
                    .bind(user_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn insert_oauth_identity(c: &mut $Conn, i: &OAuthIdentity) -> Result<()> {
                sqlx::query(sql::INSERT_OAUTH_IDENTITY)
                    .bind(&i.provider)
                    .bind(&i.subject)
                    .bind(&i.user_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn delete_user_identities(
                c: &mut $Conn,
                user_id: &str,
                provider: &str,
            ) -> Result<()> {
                sqlx::query(sql::DELETE_USER_IDENTITIES)
                    .bind(user_id)
                    .bind(provider)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn insert_oauth_state(c: &mut $Conn, s: &OAuthState) -> Result<()> {
                sqlx::query(sql::INSERT_OAUTH_STATE)
                    .bind(&s.state_hash)
                    .bind(&s.provider)
                    .bind(&s.redirect_uri)
                    .bind(&s.invite)
                    .bind(&s.challenge)
                    .bind(&s.link_user_id)
                    .bind(s.expires_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn take_oauth_state(
                c: &mut $Conn,
                state_hash: &str,
            ) -> Result<Option<OAuthState>> {
                Ok(sqlx::query_as(sql::TAKE_OAUTH_STATE)
                    .bind(state_hash)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn delete_expired_oauth_states(c: &mut $Conn, now: f64) -> Result<()> {
                sqlx::query(sql::DELETE_EXPIRED_OAUTH_STATES)
                    .bind(now)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn insert_login_code(c: &mut $Conn, l: &LoginCode) -> Result<()> {
                sqlx::query(sql::INSERT_LOGIN_CODE)
                    .bind(&l.code_hash)
                    .bind(&l.user_id)
                    .bind(&l.challenge)
                    .bind(&l.link_provider)
                    .bind(&l.link_subject)
                    .bind(&l.link_email)
                    .bind(l.expires_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn take_login_code(
                c: &mut $Conn,
                code_hash: &str,
            ) -> Result<Option<LoginCode>> {
                Ok(sqlx::query_as(sql::TAKE_LOGIN_CODE)
                    .bind(code_hash)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn delete_expired_login_codes(c: &mut $Conn, now: f64) -> Result<()> {
                sqlx::query(sql::DELETE_EXPIRED_LOGIN_CODES)
                    .bind(now)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn insert_org(c: &mut $Conn, o: &Org) -> Result<()> {
                sqlx::query(sql::INSERT_ORG)
                    .bind(&o.id)
                    .bind(&o.name)
                    .bind(o.created_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_org_name(c: &mut $Conn, id: &str, name: &str) -> Result<()> {
                sqlx::query(sql::SET_ORG_NAME)
                    .bind(id)
                    .bind(name)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn org_by_id(c: &mut $Conn, id: &str) -> Result<Option<Org>> {
                Ok(sqlx::query_as(sql::ORG_BY_ID)
                    .bind(id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn org_member(
                c: &mut $Conn,
                org_id: &str,
                user_id: &str,
            ) -> Result<Option<OrgMember>> {
                Ok(sqlx::query_as(sql::ORG_MEMBER)
                    .bind(org_id)
                    .bind(user_id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn orgs_of_user(c: &mut $Conn, user_id: &str) -> Result<Vec<OrgSummary>> {
                Ok(sqlx::query_as(sql::ORGS_OF_USER)
                    .bind(user_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn org_members(c: &mut $Conn, org_id: &str) -> Result<Vec<Member>> {
                Ok(sqlx::query_as(sql::ORG_MEMBERS)
                    .bind(org_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn insert_org_member(c: &mut $Conn, m: &OrgMember) -> Result<()> {
                sqlx::query(sql::INSERT_ORG_MEMBER)
                    .bind(&m.org_id)
                    .bind(&m.user_id)
                    .bind(&m.role)
                    .bind(m.created_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_org_member_role(
                c: &mut $Conn,
                org_id: &str,
                user_id: &str,
                role: &str,
            ) -> Result<()> {
                sqlx::query(sql::SET_ORG_MEMBER_ROLE)
                    .bind(org_id)
                    .bind(user_id)
                    .bind(role)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn delete_org_member(
                c: &mut $Conn,
                org_id: &str,
                user_id: &str,
            ) -> Result<()> {
                sqlx::query(sql::DELETE_ORG_MEMBER)
                    .bind(org_id)
                    .bind(user_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn org_member_count(c: &mut $Conn, org_id: &str) -> Result<u64> {
                let n: i64 = sqlx::query_scalar(sql::ORG_MEMBER_COUNT)
                    .bind(org_id)
                    .fetch_one(c)
                    .await?;
                Ok(u64::try_from(n)?)
            }

            pub async fn org_owner_count(c: &mut $Conn, org_id: &str) -> Result<u64> {
                let n: i64 = sqlx::query_scalar(sql::ORG_OWNER_COUNT)
                    .bind(org_id)
                    .bind("owner")
                    .fetch_one(c)
                    .await?;
                Ok(u64::try_from(n)?)
            }

            pub async fn delete_project_members_in_org(
                c: &mut $Conn,
                org_id: &str,
                user_id: &str,
            ) -> Result<()> {
                sqlx::query(sql::DELETE_PROJECT_MEMBERS_IN_ORG)
                    .bind(org_id)
                    .bind(user_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn insert_invite(c: &mut $Conn, i: &Invite) -> Result<()> {
                sqlx::query(sql::INSERT_INVITE)
                    .bind(&i.id)
                    .bind(&i.token_hash)
                    .bind(&i.org_id)
                    .bind(&i.email)
                    .bind(&i.role)
                    .bind(&i.projects)
                    .bind(&i.created_by)
                    .bind(i.created_at)
                    .bind(i.expires_at)
                    .bind(&i.accepted_by)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn invite_by_id(c: &mut $Conn, id: &str) -> Result<Option<Invite>> {
                Ok(sqlx::query_as(sql::INVITE_BY_ID)
                    .bind(id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn invite_by_hash(c: &mut $Conn, token_hash: &str) -> Result<Option<Invite>> {
                Ok(sqlx::query_as(sql::INVITE_BY_HASH)
                    .bind(token_hash)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn open_invites(
                c: &mut $Conn,
                org_id: &str,
                now: f64,
            ) -> Result<Vec<Invite>> {
                Ok(sqlx::query_as(sql::OPEN_INVITES)
                    .bind(org_id)
                    .bind(now)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn claim_invite(c: &mut $Conn, id: &str, user_id: &str) -> Result<bool> {
                let done = sqlx::query(sql::CLAIM_INVITE)
                    .bind(id)
                    .bind(user_id)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn delete_invite(c: &mut $Conn, org_id: &str, id: &str) -> Result<bool> {
                let done = sqlx::query(sql::DELETE_INVITE)
                    .bind(org_id)
                    .bind(id)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() > 0)
            }

            pub async fn insert_project(c: &mut $Conn, p: &Project) -> Result<()> {
                sqlx::query(sql::INSERT_PROJECT)
                    .bind(&p.id)
                    .bind(&p.org_id)
                    .bind(&p.name)
                    .bind(p.created_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_project_name(c: &mut $Conn, id: &str, name: &str) -> Result<()> {
                sqlx::query(sql::SET_PROJECT_NAME)
                    .bind(id)
                    .bind(name)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn project_by_id(c: &mut $Conn, id: &str) -> Result<Option<Project>> {
                Ok(sqlx::query_as(sql::PROJECT_BY_ID)
                    .bind(id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn projects_in_org(c: &mut $Conn, org_id: &str) -> Result<Vec<Project>> {
                Ok(sqlx::query_as(sql::PROJECTS_IN_ORG)
                    .bind(org_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn member_projects(
                c: &mut $Conn,
                org_id: &str,
                user_id: &str,
            ) -> Result<Vec<ProjectWithRole>> {
                Ok(sqlx::query_as(sql::MEMBER_PROJECTS)
                    .bind(org_id)
                    .bind(user_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn project_member(
                c: &mut $Conn,
                project_id: &str,
                user_id: &str,
            ) -> Result<Option<ProjectMember>> {
                Ok(sqlx::query_as(sql::PROJECT_MEMBER)
                    .bind(project_id)
                    .bind(user_id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn project_members(c: &mut $Conn, project_id: &str) -> Result<Vec<Member>> {
                Ok(sqlx::query_as(sql::PROJECT_MEMBERS)
                    .bind(project_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn insert_project_member(c: &mut $Conn, m: &ProjectMember) -> Result<()> {
                sqlx::query(sql::INSERT_PROJECT_MEMBER)
                    .bind(&m.project_id)
                    .bind(&m.user_id)
                    .bind(&m.role)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_project_member_role(
                c: &mut $Conn,
                project_id: &str,
                user_id: &str,
                role: &str,
            ) -> Result<()> {
                sqlx::query(sql::SET_PROJECT_MEMBER_ROLE)
                    .bind(project_id)
                    .bind(user_id)
                    .bind(role)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn delete_project_member(
                c: &mut $Conn,
                project_id: &str,
                user_id: &str,
            ) -> Result<bool> {
                let done = sqlx::query(sql::DELETE_PROJECT_MEMBER)
                    .bind(project_id)
                    .bind(user_id)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() > 0)
            }

            pub async fn project_document_ids(c: &mut $Conn, project_id: &str) -> Result<Vec<Id>> {
                Ok(sqlx::query_scalar(sql::PROJECT_DOCUMENT_IDS)
                    .bind(project_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn mark_project_documents_deleted(
                c: &mut $Conn,
                project_id: &str,
                now: f64,
            ) -> Result<()> {
                // Locked in ascending id order (the lock order of `crate::store`), then marked
                // in one statement.
                sqlx::query(sql::$dialect::LOCK_PROJECT_DOCUMENTS)
                    .bind(project_id)
                    .execute(&mut *c)
                    .await?;
                sqlx::query(sql::MARK_PROJECT_DOCUMENTS_DELETED)
                    .bind(project_id)
                    .bind(now)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn mark_project_deleted(c: &mut $Conn, id: &str, now: f64) -> Result<bool> {
                let done = sqlx::query(sql::MARK_PROJECT_DELETED)
                    .bind(id)
                    .bind(now)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn mark_org_deleted(c: &mut $Conn, id: &str, now: f64) -> Result<bool> {
                let done = sqlx::query(sql::MARK_ORG_DELETED)
                    .bind(id)
                    .bind(now)
                    .execute(&mut *c)
                    .await?;
                if done.rows_affected() != 1 {
                    return Ok(false);
                }
                // Document rows in ascending id order before they are marked (the lock order).
                sqlx::query(sql::$dialect::LOCK_ORG_DOCUMENTS)
                    .bind(id)
                    .execute(&mut *c)
                    .await?;
                for statement in [
                    sql::MARK_ORG_DOCUMENTS_DELETED,
                    sql::MARK_ORG_PROJECTS_DELETED,
                ] {
                    sqlx::query(statement)
                        .bind(id)
                        .bind(now)
                        .execute(&mut *c)
                        .await?;
                }
                for statement in [
                    sql::DELETE_ORG_PROJECT_MEMBERS,
                    sql::DELETE_ORG_INVITES,
                    sql::DELETE_ORG_MEMBERS,
                ] {
                    sqlx::query(statement).bind(id).execute(&mut *c).await?;
                }
                Ok(true)
            }

            pub async fn deleted_projects(
                c: &mut $Conn,
                after: &str,
                limit: i64,
            ) -> Result<Vec<Id>> {
                Ok(sqlx::query_scalar(sql::DELETED_PROJECTS)
                    .bind(after)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn deleted_orgs(c: &mut $Conn, after: &str, limit: i64) -> Result<Vec<Id>> {
                Ok(sqlx::query_scalar(sql::DELETED_ORGS)
                    .bind(after)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn documents_of_deleted_project(
                c: &mut $Conn,
                project_id: &str,
                after: &str,
                limit: i64,
            ) -> Result<Vec<Id>> {
                Ok(sqlx::query_scalar(sql::DOCUMENTS_OF_DELETED_PROJECT)
                    .bind(project_id)
                    .bind(after)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn purge_document_rows(
                c: &mut $Conn,
                document_id: &str,
                limit: i64,
            ) -> Result<u64> {
                let mut gone = 0;
                for statement in [
                    sql::PURGE_CHANGES,
                    sql::PURGE_SNAPSHOTS,
                    sql::PURGE_VERSIONS,
                    sql::PURGE_ASSEMBLY_REFS,
                ] {
                    let left = limit - i64::try_from(gone)?;
                    if left <= 0 {
                        break;
                    }
                    gone += sqlx::query(statement)
                        .bind(document_id)
                        .bind(left)
                        .execute(&mut *c)
                        .await?
                        .rows_affected();
                }
                Ok(gone)
            }

            pub async fn purge_document(c: &mut $Conn, id: &str) -> Result<bool> {
                let done = sqlx::query(sql::PURGE_DOCUMENT).bind(id).execute(c).await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn purge_project_rows(
                c: &mut $Conn,
                project_id: &str,
                limit: i64,
            ) -> Result<u64> {
                let done = sqlx::query(sql::PURGE_FOLDERS)
                    .bind(project_id)
                    .bind(limit)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected())
            }

            pub async fn purge_project(c: &mut $Conn, id: &str) -> Result<bool> {
                let done = sqlx::query(sql::PURGE_PROJECT).bind(id).execute(c).await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn purge_org(c: &mut $Conn, id: &str) -> Result<bool> {
                let done = sqlx::query(sql::PURGE_ORG).bind(id).execute(c).await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn insert_document(c: &mut $Conn, d: &Document) -> Result<()> {
                sqlx::query(sql::INSERT_DOCUMENT)
                    .bind(&d.id)
                    .bind(&d.project_id)
                    .bind(&d.folder_id)
                    .bind(&d.name)
                    .bind(&d.kind)
                    .bind(d.format_version)
                    .bind(d.head_seq)
                    .bind(&d.created_by)
                    .bind(d.created_at)
                    .bind(d.updated_at)
                    .bind(d.deleted_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn document_by_id(c: &mut $Conn, id: &str) -> Result<Option<Document>> {
                Ok(sqlx::query_as(sql::DOCUMENT_BY_ID)
                    .bind(id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn documents_in_project(
                c: &mut $Conn,
                project_id: &str,
            ) -> Result<Vec<Document>> {
                Ok(sqlx::query_as(sql::DOCUMENTS_IN_PROJECT)
                    .bind(project_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn set_document_name(c: &mut $Conn, id: &str, name: &str) -> Result<()> {
                sqlx::query(sql::SET_DOCUMENT_NAME)
                    .bind(id)
                    .bind(name)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_document_folder(
                c: &mut $Conn,
                id: &str,
                folder_id: Option<&str>,
            ) -> Result<()> {
                sqlx::query(sql::SET_DOCUMENT_FOLDER)
                    .bind(id)
                    .bind(folder_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn mark_document_deleted(c: &mut $Conn, id: &str, now: f64) -> Result<bool> {
                let done = sqlx::query(sql::MARK_DOCUMENT_DELETED)
                    .bind(id)
                    .bind(now)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn insert_snapshot(c: &mut $Conn, s: &SnapshotRow) -> Result<()> {
                sqlx::query(sql::INSERT_SNAPSHOT)
                    .bind(&s.document_id)
                    .bind(s.epoch)
                    .bind(s.seq)
                    .bind(&s.key)
                    .bind(s.size)
                    .bind(s.created_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn latest_snapshot(
                c: &mut $Conn,
                document_id: &str,
            ) -> Result<Option<SnapshotRow>> {
                Ok(sqlx::query_as(sql::LATEST_SNAPSHOT)
                    .bind(document_id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn insert_folder(c: &mut $Conn, f: &Folder) -> Result<()> {
                sqlx::query(sql::INSERT_FOLDER)
                    .bind(&f.id)
                    .bind(&f.project_id)
                    .bind(&f.parent_id)
                    .bind(&f.name)
                    .bind(f.created_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn folder_by_id(c: &mut $Conn, id: &str) -> Result<Option<Folder>> {
                Ok(sqlx::query_as(sql::FOLDER_BY_ID)
                    .bind(id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn folders_in_project(
                c: &mut $Conn,
                project_id: &str,
            ) -> Result<Vec<Folder>> {
                Ok(sqlx::query_as(sql::FOLDERS_IN_PROJECT)
                    .bind(project_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn set_folder_name(c: &mut $Conn, id: &str, name: &str) -> Result<()> {
                sqlx::query(sql::SET_FOLDER_NAME)
                    .bind(id)
                    .bind(name)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_folder_parent(
                c: &mut $Conn,
                id: &str,
                parent_id: Option<&str>,
            ) -> Result<()> {
                sqlx::query(sql::SET_FOLDER_PARENT)
                    .bind(id)
                    .bind(parent_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn move_child_folders(
                c: &mut $Conn,
                folder_id: &str,
                parent_id: Option<&str>,
            ) -> Result<()> {
                sqlx::query(sql::MOVE_CHILD_FOLDERS)
                    .bind(folder_id)
                    .bind(parent_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn move_folder_documents(
                c: &mut $Conn,
                folder_id: &str,
                parent_id: Option<&str>,
            ) -> Result<()> {
                // Document rows are locked in ascending id order (the lock order of `crate::store`).
                sqlx::query(sql::$dialect::LOCK_FOLDER_DOCUMENTS)
                    .bind(folder_id)
                    .execute(&mut *c)
                    .await?;
                sqlx::query(sql::MOVE_FOLDER_DOCUMENTS)
                    .bind(folder_id)
                    .bind(parent_id)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn delete_folder(c: &mut $Conn, id: &str) -> Result<()> {
                sqlx::query(sql::DELETE_FOLDER).bind(id).execute(c).await?;
                Ok(())
            }

            pub async fn insert_change(c: &mut $Conn, r: &NewChange<'_>) -> Result<bool> {
                let done = sqlx::query(sql::INSERT_CHANGE)
                    .bind(r.document_id)
                    .bind(r.seq)
                    .bind(r.parent_seq)
                    .bind(r.branch_id)
                    .bind(r.epoch)
                    .bind(r.author_id)
                    .bind(r.client_id)
                    .bind(r.change_id)
                    .bind(r.created_at)
                    .bind(r.command)
                    .bind(r.ops)
                    .bind(r.inverse)
                    .bind(r.paths)
                    .bind(r.reads)
                    .bind(r.undo_of)
                    .bind(r.reverts)
                    .bind(r.reverted_by)
                    .bind(r.broke)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn insert_changes(c: &mut $Conn, rows: &[NewChange<'_>]) -> Result<u64> {
                let mut inserted = 0;
                for chunk in rows.chunks(sql::CHANGES_PER_INSERT) {
                    if let [one] = chunk {
                        inserted += u64::from(insert_change(&mut *c, one).await?);
                        continue;
                    }
                    // Not kept prepared: its text differs with every row count.
                    let text = sql::insert_changes(chunk.len());
                    let mut query = sqlx::query(&text).persistent(false);
                    for r in chunk {
                        query = query
                            .bind(r.document_id)
                            .bind(r.seq)
                            .bind(r.parent_seq)
                            .bind(r.branch_id)
                            .bind(r.epoch)
                            .bind(r.author_id)
                            .bind(r.client_id)
                            .bind(r.change_id)
                            .bind(r.created_at)
                            .bind(r.command)
                            .bind(r.ops)
                            .bind(r.inverse)
                            .bind(r.paths)
                            .bind(r.reads)
                            .bind(r.undo_of)
                            .bind(r.reverts)
                            .bind(r.reverted_by)
                            .bind(r.broke);
                    }
                    inserted += query.execute(&mut *c).await?.rows_affected();
                }
                Ok(inserted)
            }

            pub async fn changes_between(
                c: &mut $Conn,
                document_id: &str,
                after: i64,
                upto: i64,
                limit: i64,
            ) -> Result<Vec<ChangeRow>> {
                Ok(sqlx::query_as(sql::CHANGES_BETWEEN)
                    .bind(document_id)
                    .bind(after)
                    .bind(upto)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn change_by_seq(
                c: &mut $Conn,
                document_id: &str,
                seq: i64,
            ) -> Result<Option<ChangeRow>> {
                Ok(sqlx::query_as(sql::CHANGE_BY_SEQ)
                    .bind(document_id)
                    .bind(seq)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn change_events_between(
                c: &mut $Conn,
                document_id: &str,
                after: i64,
                upto: i64,
                limit: i64,
            ) -> Result<Vec<ChangeEventRow>> {
                Ok(sqlx::query_as(sql::CHANGE_EVENTS_BETWEEN)
                    .bind(document_id)
                    .bind(after)
                    .bind(upto)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn change_seq_by_client(
                c: &mut $Conn,
                document_id: &str,
                client_id: &str,
                change_id: &str,
            ) -> Result<Option<i64>> {
                // (No stored change has such an id: the journal refuses the zero character, which
                // PostgreSQL could not even be asked about.)
                if client_id.contains('\0') || change_id.contains('\0') {
                    return Ok(None);
                }
                Ok(sqlx::query_scalar(sql::CHANGE_SEQ_BY_CLIENT)
                    .bind(document_id)
                    .bind(client_id)
                    .bind(change_id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn recent_change_ids(
                c: &mut $Conn,
                document_id: &str,
                client_id: &str,
                limit: i64,
            ) -> Result<Vec<(String, i64)>> {
                if client_id.contains('\0') {
                    return Ok(Vec::new()); // (as above)
                }
                Ok(sqlx::query_as(sql::RECENT_CHANGE_IDS)
                    .bind(document_id)
                    .bind(client_id)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn set_document_head(
                c: &mut $Conn,
                id: &str,
                head_seq: i64,
                updated_at: f64,
            ) -> Result<()> {
                sqlx::query(sql::SET_DOCUMENT_HEAD)
                    .bind(id)
                    .bind(head_seq)
                    .bind(updated_at)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn set_reverted_by(
                c: &mut $Conn,
                document_id: &str,
                seq: i64,
                by: i64,
            ) -> Result<bool> {
                let done = sqlx::query(sql::SET_REVERTED_BY)
                    .bind(document_id)
                    .bind(seq)
                    .bind(by)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn undo_target(
                c: &mut $Conn,
                document_id: &str,
                client_id: &str,
                change_id: &str,
            ) -> Result<Option<UndoTarget>> {
                // (No stored change has such an id: the journal refuses the zero character, which
                // PostgreSQL could not even be asked about.)
                if client_id.contains('\0') || change_id.contains('\0') {
                    return Ok(None);
                }
                Ok(sqlx::query_as(sql::UNDO_TARGET)
                    .bind(document_id)
                    .bind(client_id)
                    .bind(change_id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn change_sizes(
                c: &mut $Conn,
                document_id: &str,
                before: i64,
                after: i64,
                limit: i64,
            ) -> Result<Vec<ChangeSize>> {
                Ok(sqlx::query_as(sql::CHANGE_SIZES)
                    .bind(document_id)
                    .bind(before)
                    .bind(after)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn change_sizes_after(
                c: &mut $Conn,
                document_id: &str,
                after: i64,
                upto: i64,
                limit: i64,
            ) -> Result<Vec<ChangeSize>> {
                Ok(sqlx::query_as(sql::CHANGE_SIZES_AFTER)
                    .bind(document_id)
                    .bind(after)
                    .bind(upto)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn history(
                c: &mut $Conn,
                document_id: &str,
                before: i64,
                after: i64,
                limit: i64,
            ) -> Result<Vec<HistoryRow>> {
                Ok(sqlx::query_as(sql::HISTORY)
                    .bind(document_id)
                    .bind(before)
                    .bind(after)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn change_broke(
                c: &mut $Conn,
                document_id: &str,
                seq: i64,
            ) -> Result<Option<Option<String>>> {
                Ok(sqlx::query_scalar(sql::$dialect::CHANGE_BROKE)
                    .bind(document_id)
                    .bind(seq)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn set_change_broke(
                c: &mut $Conn,
                document_id: &str,
                seq: i64,
                broke: &str,
            ) -> Result<()> {
                sqlx::query(sql::SET_CHANGE_BROKE)
                    .bind(document_id)
                    .bind(seq)
                    .bind(broke)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn versions(
                c: &mut $Conn,
                document_id: &str,
            ) -> Result<Vec<VersionWithAuthor>> {
                Ok(sqlx::query_as(sql::VERSIONS)
                    .bind(document_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn version_by_name(
                c: &mut $Conn,
                document_id: &str,
                name: &str,
            ) -> Result<Option<Version>> {
                Ok(sqlx::query_as(sql::VERSION_BY_NAME)
                    .bind(document_id)
                    .bind(name)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn insert_version(c: &mut $Conn, v: &Version) -> Result<bool> {
                let done = sqlx::query(sql::INSERT_VERSION)
                    .bind(&v.id)
                    .bind(&v.document_id)
                    .bind(v.seq)
                    .bind(&v.name)
                    .bind(&v.note)
                    .bind(&v.author_id)
                    .bind(v.created_at)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn delete_version(
                c: &mut $Conn,
                document_id: &str,
                id: &str,
            ) -> Result<bool> {
                let done = sqlx::query(sql::DELETE_VERSION)
                    .bind(document_id)
                    .bind(id)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn replace_assembly_refs(
                c: &mut $Conn,
                assembly_id: &str,
                rows: &[AssemblyRefRow],
            ) -> Result<()> {
                sqlx::query(sql::DELETE_ASSEMBLY_REFS)
                    .bind(assembly_id)
                    .execute(&mut *c)
                    .await?;
                for r in rows {
                    sqlx::query(sql::INSERT_ASSEMBLY_REF)
                        .bind(&r.assembly_id)
                        .bind(&r.component_id)
                        .bind(&r.part_id)
                        .bind(r.seq)
                        .execute(&mut *c)
                        .await?;
                }
                Ok(())
            }

            pub async fn assembly_refs(
                c: &mut $Conn,
                assembly_id: &str,
            ) -> Result<Vec<AssemblyRefRow>> {
                Ok(sqlx::query_as(sql::ASSEMBLY_REFS)
                    .bind(assembly_id)
                    .fetch_all(c)
                    .await?)
            }

            /// Ids per statement of [`parts_by_ids`] (far below SQLite's and PostgreSQL's limits).
            const IDS_PER_QUERY: usize = 256;

            pub async fn parts_by_ids(c: &mut $Conn, ids: &[Id]) -> Result<Vec<PartRow>> {
                let mut out = Vec::new();
                for chunk in ids.chunks(IDS_PER_QUERY) {
                    let text = sql::in_list(sql::PARTS_BY_IDS, chunk.len());
                    let mut query = sqlx::query_as::<_, PartRow>(&text).persistent(false);
                    for id in chunk {
                        query = query.bind(id);
                    }
                    out.extend(query.fetch_all(&mut *c).await?);
                }
                Ok(out)
            }

            pub async fn versions_of(c: &mut $Conn, ids: &[Id]) -> Result<Vec<VersionWithAuthor>> {
                let mut out: Vec<VersionWithAuthor> = Vec::new();
                for chunk in ids.chunks(IDS_PER_QUERY) {
                    let text = sql::in_list(sql::VERSIONS_OF, chunk.len());
                    let mut query = sqlx::query_as::<_, VersionWithAuthor>(&text).persistent(false);
                    for id in chunk {
                        query = query.bind(id);
                    }
                    out.extend(query.fetch_all(&mut *c).await?);
                }
                // As `sql::VERSIONS` orders one document's (newest seq, then newest, then id).
                out.sort_by(|a, b| {
                    a.document_id
                        .cmp(&b.document_id)
                        .then(b.seq.cmp(&a.seq))
                        .then(b.created_at.total_cmp(&a.created_at))
                        .then(a.id.cmp(&b.id))
                });
                Ok(out)
            }

            pub async fn refs_to_part(c: &mut $Conn, part_id: &str) -> Result<Vec<AssemblyRefRow>> {
                Ok(sqlx::query_as(sql::REFS_TO_PART)
                    .bind(part_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn assemblies_using(
                c: &mut $Conn,
                part_id: &str,
            ) -> Result<Vec<AssemblyUse>> {
                Ok(sqlx::query_as(sql::ASSEMBLIES_USING)
                    .bind(part_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn assemblies_using_project(
                c: &mut $Conn,
                project_id: &str,
            ) -> Result<Vec<AssemblyUse>> {
                Ok(sqlx::query_as(sql::ASSEMBLIES_USING_PROJECT)
                    .bind(project_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn pinned_before(
                c: &mut $Conn,
                part_id: &str,
                seq: i64,
            ) -> Result<Vec<PinnedComponent>> {
                Ok(sqlx::query_as(sql::PINNED_BEFORE)
                    .bind(part_id)
                    .bind(seq)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn lock_live_document(c: &mut $Conn, id: &str) -> Result<bool> {
                let found: Option<Id> = sqlx::query_scalar(sql::$dialect::LOCK_LIVE_DOCUMENT)
                    .bind(id)
                    .fetch_optional(c)
                    .await?;
                Ok(found.is_some())
            }

            pub async fn lock_accounts(c: &mut $Conn) -> Result<()> {
                sqlx::query(sql::$dialect::LOCK_ACCOUNTS).execute(c).await?;
                Ok(())
            }

            pub async fn lock_user(c: &mut $Conn, id: &str) -> Result<bool> {
                let found: Option<Id> = sqlx::query_scalar(sql::$dialect::LOCK_USER)
                    .bind(id)
                    .fetch_optional(c)
                    .await?;
                Ok(found.is_some())
            }

            pub async fn lock_org(c: &mut $Conn, id: &str) -> Result<bool> {
                let found: Option<Id> = sqlx::query_scalar(sql::$dialect::LOCK_ORG)
                    .bind(id)
                    .fetch_optional(c)
                    .await?;
                Ok(found.is_some())
            }

            pub async fn lock_documents(c: &mut $Conn, ids: &[Id]) -> Result<()> {
                // Ascending, each once: chunk after chunk keeps the order over all of them.
                let mut ids: Vec<&Id> = ids.iter().collect();
                ids.sort_unstable();
                ids.dedup();
                for chunk in ids.chunks(IDS_PER_QUERY) {
                    let mut text = sql::in_list(sql::LOCK_DOCUMENTS, chunk.len());
                    text.push_str(sql::$dialect::IN_ID_ORDER_LOCKED);
                    let mut query = sqlx::query(&text).persistent(false);
                    for id in chunk {
                        query = query.bind(*id);
                    }
                    query.execute(&mut *c).await?;
                }
                Ok(())
            }

            pub async fn snapshot_at_or_before(
                c: &mut $Conn,
                document_id: &str,
                seq: i64,
            ) -> Result<Option<SnapshotRow>> {
                Ok(sqlx::query_as(sql::SNAPSHOT_AT_OR_BEFORE)
                    .bind(document_id)
                    .bind(seq)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn snapshot_row(
                c: &mut $Conn,
                document_id: &str,
                epoch: i64,
                seq: i64,
            ) -> Result<Option<SnapshotRow>> {
                Ok(sqlx::query_as(sql::SNAPSHOT_ROW)
                    .bind(document_id)
                    .bind(epoch)
                    .bind(seq)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn room_lease(c: &mut $Conn, document_id: &str) -> Result<Option<RoomLease>> {
                Ok(sqlx::query_as(sql::ROOM_LEASE)
                    .bind(document_id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn lock_room_lease(c: &mut $Conn, document_id: &str) -> Result<Option<i64>> {
                Ok(sqlx::query_scalar(sql::$dialect::LOCK_ROOM_LEASE)
                    .bind(document_id)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn insert_room_lease(c: &mut $Conn, l: &RoomLease) -> Result<bool> {
                let done = sqlx::query(sql::INSERT_ROOM_LEASE)
                    .bind(&l.document_id)
                    .bind(&l.node_id)
                    .bind(l.epoch)
                    .bind(l.expires_at)
                    .execute(c)
                    .await?;
                Ok(done.rows_affected() == 1)
            }

            pub async fn take_room_lease(
                c: &mut $Conn,
                document_id: &str,
                node_id: &str,
                expires_at: f64,
            ) -> Result<Option<i64>> {
                Ok(sqlx::query_scalar(sql::TAKE_ROOM_LEASE)
                    .bind(document_id)
                    .bind(node_id)
                    .bind(expires_at)
                    .fetch_optional(c)
                    .await?)
            }

            pub async fn orgs_owned_alone(c: &mut $Conn, user_id: &str) -> Result<Vec<Org>> {
                Ok(sqlx::query_as(sql::ORGS_OWNED_ALONE)
                    .bind(user_id)
                    .bind("owner")
                    .fetch_all(c)
                    .await?)
            }

            pub async fn active_admin_count(c: &mut $Conn) -> Result<u64> {
                let n: i64 = sqlx::query_scalar(sql::ACTIVE_ADMIN_COUNT)
                    .bind(true)
                    .bind(false)
                    .fetch_one(c)
                    .await?;
                Ok(u64::try_from(n)?)
            }

            pub async fn other_active_user_count(c: &mut $Conn, user_id: &str) -> Result<u64> {
                let n: i64 = sqlx::query_scalar(sql::OTHER_ACTIVE_USER_COUNT)
                    .bind(user_id)
                    .bind(false)
                    .fetch_one(c)
                    .await?;
                Ok(u64::try_from(n)?)
            }

            pub async fn user_org_ids(c: &mut $Conn, user_id: &str) -> Result<Vec<Id>> {
                Ok(sqlx::query_scalar(sql::USER_ORG_IDS)
                    .bind(user_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn erase_user(
                c: &mut $Conn,
                id: &str,
                email: &str,
                name: &str,
                now: f64,
            ) -> Result<()> {
                for statement in [
                    sql::DELETE_USER_SESSIONS,
                    sql::DELETE_ALL_USER_IDENTITIES,
                    sql::DELETE_USER_LOGIN_CODES,
                    sql::DELETE_USER_PASSWORD_RESETS,
                    sql::DELETE_USER_ORG_MEMBERS,
                    sql::DELETE_USER_PROJECT_MEMBERS,
                ] {
                    sqlx::query(statement).bind(id).execute(&mut *c).await?;
                }
                sqlx::query(sql::ERASE_USER)
                    .bind(id)
                    .bind(email)
                    .bind(name)
                    .bind(false)
                    .bind(true)
                    .bind(now)
                    .execute(c)
                    .await?;
                Ok(())
            }

            pub async fn org_document_ids(c: &mut $Conn, org_id: &str) -> Result<Vec<Id>> {
                Ok(sqlx::query_scalar(sql::ORG_DOCUMENT_IDS)
                    .bind(org_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn org_member_ids(c: &mut $Conn, org_id: &str) -> Result<Vec<Id>> {
                Ok(sqlx::query_scalar(sql::ORG_MEMBER_IDS)
                    .bind(org_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn user_memberships(c: &mut $Conn, user_id: &str) -> Result<Vec<Membership>> {
                Ok(sqlx::query_as(sql::USER_MEMBERSHIPS)
                    .bind(user_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn user_project_roles(
                c: &mut $Conn,
                user_id: &str,
            ) -> Result<Vec<ProjectWithRole>> {
                Ok(sqlx::query_as(sql::USER_PROJECT_ROLES)
                    .bind(user_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn user_sessions(c: &mut $Conn, user_id: &str) -> Result<Vec<Session>> {
                Ok(sqlx::query_as(sql::USER_SESSIONS)
                    .bind(user_id)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn changes_by_author(
                c: &mut $Conn,
                user_id: &str,
                after_document: &str,
                after_seq: i64,
                limit: i64,
            ) -> Result<Vec<AuthoredChange>> {
                Ok(sqlx::query_as(sql::CHANGES_BY_AUTHOR)
                    .bind(user_id)
                    .bind(after_document)
                    .bind(after_seq)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }

            pub async fn versions_by_author(
                c: &mut $Conn,
                user_id: &str,
                after: &str,
                limit: i64,
            ) -> Result<Vec<Version>> {
                Ok(sqlx::query_as(sql::VERSIONS_BY_AUTHOR)
                    .bind(user_id)
                    .bind(after)
                    .bind(limit)
                    .fetch_all(c)
                    .await?)
            }
        }
    };
}

pub(super) use store_impl;
