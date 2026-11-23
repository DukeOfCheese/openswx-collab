"""Baseline: the schema of the first release (and of databases made before migrations existed)

Revision ID: 0001
Revises: 
Create Date: 2026-11-23 20:30:07.810408
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = '0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('meta',
    sa.Column('key', sa.String(length=64), nullable=False),
    sa.Column('value', sa.Text(), nullable=False),
    sa.PrimaryKeyConstraint('key')
    )
    op.create_table('oauth_states',
    sa.Column('state_hash', sa.String(length=64), nullable=False),
    sa.Column('provider', sa.String(length=32), nullable=False),
    sa.Column('redirect_uri', sa.Text(), nullable=False),
    sa.Column('invite', sa.Text(), nullable=True),
    sa.Column('challenge', sa.String(length=64), nullable=False),
    sa.Column('link_user_id', sa.String(length=32), nullable=True),
    sa.Column('expires_at', sa.Float(), nullable=False),
    sa.PrimaryKeyConstraint('state_hash')
    )
    op.create_table('orgs',
    sa.Column('id', sa.String(length=32), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('users',
    sa.Column('id', sa.String(length=32), nullable=False),
    sa.Column('email', sa.String(length=320), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('password_hash', sa.Text(), nullable=True),
    sa.Column('is_server_admin', sa.Boolean(), nullable=False),
    sa.Column('disabled', sa.Boolean(), nullable=False),
    sa.Column('email_verified', sa.Boolean(), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_users_email'), ['email'], unique=True)

    op.create_table('invites',
    sa.Column('id', sa.String(length=32), nullable=False),
    sa.Column('token_hash', sa.String(length=64), nullable=False),
    sa.Column('org_id', sa.String(length=32), nullable=False),
    sa.Column('email', sa.String(length=320), nullable=True),
    sa.Column('role', sa.String(length=16), nullable=False),
    sa.Column('projects', sa.JSON(), nullable=False),
    sa.Column('created_by', sa.String(length=32), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.Column('expires_at', sa.Float(), nullable=False),
    sa.Column('accepted_by', sa.String(length=32), nullable=True),
    sa.ForeignKeyConstraint(['org_id'], ['orgs.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('token_hash')
    )
    with op.batch_alter_table('invites', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_invites_org_id'), ['org_id'], unique=False)

    op.create_table('login_codes',
    sa.Column('code_hash', sa.String(length=64), nullable=False),
    sa.Column('user_id', sa.String(length=32), nullable=False),
    sa.Column('challenge', sa.String(length=64), nullable=False),
    sa.Column('link_provider', sa.String(length=32), nullable=True),
    sa.Column('link_subject', sa.String(length=255), nullable=True),
    sa.Column('link_email', sa.String(length=320), nullable=True),
    sa.Column('expires_at', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('code_hash')
    )
    op.create_table('oauth_identities',
    sa.Column('provider', sa.String(length=32), nullable=False),
    sa.Column('subject', sa.String(length=255), nullable=False),
    sa.Column('user_id', sa.String(length=32), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('provider', 'subject')
    )
    with op.batch_alter_table('oauth_identities', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_oauth_identities_user_id'), ['user_id'], unique=False)

    op.create_table('org_members',
    sa.Column('org_id', sa.String(length=32), nullable=False),
    sa.Column('user_id', sa.String(length=32), nullable=False),
    sa.Column('role', sa.String(length=16), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['org_id'], ['orgs.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('org_id', 'user_id')
    )
    with op.batch_alter_table('org_members', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_org_members_user_id'), ['user_id'], unique=False)

    op.create_table('projects',
    sa.Column('id', sa.String(length=32), nullable=False),
    sa.Column('org_id', sa.String(length=32), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['org_id'], ['orgs.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('projects', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_projects_org_id'), ['org_id'], unique=False)

    op.create_table('sessions',
    sa.Column('token_hash', sa.String(length=64), nullable=False),
    sa.Column('user_id', sa.String(length=32), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.Column('expires_at', sa.Float(), nullable=False),
    sa.Column('label', sa.String(length=200), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('token_hash')
    )
    with op.batch_alter_table('sessions', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_sessions_user_id'), ['user_id'], unique=False)

    op.create_table('documents',
    sa.Column('id', sa.String(length=32), nullable=False),
    sa.Column('project_id', sa.String(length=32), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('kind', sa.String(length=16), nullable=False),
    sa.Column('format_version', sa.Integer(), nullable=True),
    sa.Column('head_seq', sa.Integer(), nullable=False),
    sa.Column('created_by', sa.String(length=32), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.Column('updated_at', sa.Float(), nullable=False),
    sa.Column('deleted_at', sa.Float(), nullable=True),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    with op.batch_alter_table('documents', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_documents_project_id'), ['project_id'], unique=False)

    op.create_table('project_members',
    sa.Column('project_id', sa.String(length=32), nullable=False),
    sa.Column('user_id', sa.String(length=32), nullable=False),
    sa.Column('role', sa.String(length=16), nullable=False),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('project_id', 'user_id')
    )
    with op.batch_alter_table('project_members', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_project_members_user_id'), ['user_id'], unique=False)

    op.create_table('assembly_refs',
    sa.Column('assembly_id', sa.String(length=32), nullable=False),
    sa.Column('component_id', sa.String(length=200), nullable=False),
    sa.Column('part_id', sa.String(length=32), nullable=False),
    sa.Column('seq', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['assembly_id'], ['documents.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('assembly_id', 'component_id')
    )
    with op.batch_alter_table('assembly_refs', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_assembly_refs_part_id'), ['part_id'], unique=False)

    op.create_table('ops',
    sa.Column('document_id', sa.String(length=32), nullable=False),
    sa.Column('seq', sa.Integer(), nullable=False),
    sa.Column('author_id', sa.String(length=32), nullable=False),
    sa.Column('client_id', sa.String(length=128), nullable=True),
    sa.Column('change_id', sa.String(length=64), nullable=True),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.Column('command', sa.String(length=100), nullable=False),
    sa.Column('args', sa.JSON(), nullable=True),
    sa.Column('ops', sa.JSON(), nullable=False),
    sa.Column('inverse_ops', sa.JSON(), nullable=False),
    sa.Column('targets', sa.JSON(), nullable=False),
    sa.Column('reverts', sa.Integer(), nullable=True),
    sa.Column('reverted_by', sa.Integer(), nullable=True),
    sa.Column('broke', sa.JSON(), nullable=True),
    sa.ForeignKeyConstraint(['document_id'], ['documents.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('document_id', 'seq'),
    sa.UniqueConstraint('document_id', 'client_id', 'change_id', name='uq_op_change')
    )
    with op.batch_alter_table('ops', schema=None) as batch_op:
        batch_op.create_index('ix_ops_doc_seq', ['document_id', 'seq'], unique=False)

    op.create_table('snapshots',
    sa.Column('document_id', sa.String(length=32), nullable=False),
    sa.Column('seq', sa.Integer(), nullable=False),
    sa.Column('key', sa.Text(), nullable=False),
    sa.Column('size', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['documents.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('document_id', 'seq')
    )
    op.create_table('versions',
    sa.Column('id', sa.String(length=32), nullable=False),
    sa.Column('document_id', sa.String(length=32), nullable=False),
    sa.Column('seq', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=200), nullable=False),
    sa.Column('note', sa.Text(), nullable=False),
    sa.Column('author_id', sa.String(length=32), nullable=False),
    sa.Column('created_at', sa.Float(), nullable=False),
    sa.ForeignKeyConstraint(['document_id'], ['documents.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('document_id', 'name', name='uq_version_name')
    )
    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_versions_document_id'), ['document_id'], unique=False)



def downgrade() -> None:
    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_versions_document_id'))

    op.drop_table('versions')
    op.drop_table('snapshots')
    with op.batch_alter_table('ops', schema=None) as batch_op:
        batch_op.drop_index('ix_ops_doc_seq')

    op.drop_table('ops')
    with op.batch_alter_table('assembly_refs', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_assembly_refs_part_id'))

    op.drop_table('assembly_refs')
    with op.batch_alter_table('project_members', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_project_members_user_id'))

    op.drop_table('project_members')
    with op.batch_alter_table('documents', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_documents_project_id'))

    op.drop_table('documents')
    with op.batch_alter_table('sessions', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_sessions_user_id'))

    op.drop_table('sessions')
    with op.batch_alter_table('projects', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_projects_org_id'))

    op.drop_table('projects')
    with op.batch_alter_table('org_members', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_org_members_user_id'))

    op.drop_table('org_members')
    with op.batch_alter_table('oauth_identities', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_oauth_identities_user_id'))

    op.drop_table('oauth_identities')
    op.drop_table('login_codes')
    with op.batch_alter_table('invites', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_invites_org_id'))

    op.drop_table('invites')
    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_users_email'))

    op.drop_table('users')
    op.drop_table('orgs')
    op.drop_table('oauth_states')
    op.drop_table('meta')
